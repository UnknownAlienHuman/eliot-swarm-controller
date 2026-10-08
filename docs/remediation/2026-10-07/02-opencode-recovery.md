# R02. OpenCode: восстановление без повторного эффекта и без потерянного владельца

**PR #28 · задание уточнено 7 октября 2026; production-код R02 ещё не изменён.**
Основание: AUD-011/027/028/033, source `40591a295af94b1541ec2ba30afe8e3247701a71`; прочитан head задания `26cfc03da5986bbfde7ed892b15136bc1d8bed67`. Реализацию добавлять в эту же ветку целиком: journal → hello → RPC/outbox → result → stop.

## Начать здесь

Прочитать `run_owned` в `crates/swarm-adapter-opencode/src/lib.rs`, затем вызываемые функции из таблицы. Цель — восстановить управляемый adapter lifecycle, не переносить встроенный `runtime/opencode_v2` и не расширять native capabilities.

```text
Journal::open → recover_outbox → native_root_for_hello
  → HostSession::hello_retry/open_link
  → module.next → handle_command → flush_outbox
  → остановка: run_owned → NativeOwnerController::shutdown → NativeOwner::shutdown
```

## Документация по участкам

- [Module contract](../../agent_swarm.module-contract-v2.md), §2–4: native topology, delivery/replay policy, неизвестный исход.
- [OpenCode UPDATE](../../../modules/opencode/UPDATE.md): различие native service, JS owner и Rust adapter; artifact/activation/recovery правила.
- [Modularity](../../agent-operations/modularity.md), §2.1 и §3: один Store, exact handshake; адаптер не зависит от другого адаптера.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4 и §2.2: manager/worktree, минимальный gate, отсутствие heuristic kill и automatic evidence eviction.

## Карта существующих функций

Все пути ниже — `crates/swarm-adapter-opencode/src/`, кроме явно названных доноров.

| Функции | Что исправлять / сохранить |
|---|---|
| `journal.rs::Journal::{open,load,load_by_path}` | Одна bounded трактовка существующего journal; отсутствие файла не равно пустому/повреждённому файлу. |
| `recover_outbox`, `native_root_for_hello`, `checkpoint_root` | Использовать согласованный результат recovery. Починить только первый обход недостаточно: hello снова читает те же journals. |
| `queue_outcome`, `write_pending`, `acknowledge_outcome`, `acknowledge_result`, `remove_pending` | Exact immutable payload/digest, локальный durable ACK и восстановимая доставка; несовпадение не стирать. |
| `lib.rs::HostSession::{open_link,hello_retry,call,call_recorded_retry}` | Владение здоровым ModuleLink вместо открытия и hello на каждый вызов. |
| `flush_outbox`, `remember_root_from_outcome`, `save_store_root`, `set_root_hint` | Разделить обязательное сохранение root/ACK и производный hint. Root inconsistency — не необязательная диагностика. |
| `native.rs::NativeClient::read_assistant_result` | Отдельно EOF, scan limit и cursor cycle; сохранить exact input/assistant parent проверки. |
| `native_owner.rs::{NativeOwnerController::shutdown,NativeOwner::shutdown,ensure_started}` и `lib.rs::run_owned` | Удержать Child и owner **во всей цепочке вызова**, пока departure не доказан. |

## 1. Один decoder, разные решения о восстановлении

Объединить `load` и `load_by_path` в private decoder с входным optional expected operation ID и проверкой `operation_key(id)` против имени файла. Framing оставлять bounded; v2 operation record и v1 root checkpoint не смешивать в один произвольный schema-less JSON parser.

Предлагаемое внутреннее представление, **ещё не существующий API**: проверенная `OperationHistory` + граница полностью разобранных записей + состояние целостности. Парсер сам не удаляет/обрезает исходный файл. Объединить проверку record kinds, digest, ID и последовательности ACK в одном месте.

| Состояние | Решение |
|---|---|
| Файла нет; нет иных retained intent/effect facts | Обычный путь новой операции по действующему admission. |
| Существующий файл пуст | Не трактовать как разрешение повторить POST; явно отсутствует доказательство истории. |
| Полный валидный intent, outcome ещё нет | Unknown/readback, как уже делает `handle_command`; не повтор native input. |
| Валидный префикс + незавершённый последний record | Сохранить исходные bytes и префикс; удержать неопределённость. Новый report/ACK нельзя append поверх torn bytes. |
| Повреждение внутри истории, ID/digest conflict | Изолировать идентифицированную операцию и сохранить причину; не пропускать произвольную строку как EOF. |
| Повреждён state identity или неразрешима общая root identity | Удержать scope/reconciliation; не объявить новую пустую установку/готовый root. |

Неидентифицируемый файл с хэш-именем нельзя уверенно приписать произвольному operation ID. Recovery summary должен различать operation-local проблему и отсутствие общей identity. Нельзя «продолжить здоровые операции», если нездоровая запись могла определять тот же единственный native root. Согласовать это с `native_root_for_hello`, а не просто добавить `.ok()` в оба обхода.

Для восстановления appendable файла выбрать явный owner-serialized repair с сохранённым оригиналом и доказанным verified prefix; до его безопасной публикации оставить операцию held. Не выдавать обрезанный intent за no-effect. Общая state identity остаётся fail-closed. Новая БД не требуется.

## 2. ACK и локальная публикация

Сначала проверить pending payload и его связь с journal/сессией. После `recorded:true` сохранить точный ACK. Обязательный root checkpoint должен быть либо записан, либо однозначно восстановим из уже сохранённой authoritative истории до удаления единственной копии. Производный in-memory hint не должен бесконечно блокировать ACK несвязанных записей.

`write_pending` сейчас отвергает разные payload под одним ключом. Не заменить это unconditional overwrite. Переход Unknown → Applied и доставка старого ACK требуют сравнения точных digests и доказательства разрешённого readback-перехода; аудит не доказал, что всякий такой переход сломан.

Отдельные identity/outbox файлы публиковать через собственный private temporary file в той же директории → write → file sync → публикация с нужной no-clobber семантикой → поддержанный directory sync. Сначала сверить имеющиеся primitives; не строить adapter SDK ради этого PR. Temp-файлы не должны попадать в `pending_items` как готовые сообщения.

## 3. Одна здоровая IPC-сессия, не retry-everything

`open_link` уже возвращает `ModuleLink`; `hello_retry` сегодня его теряет, а `call` открывает заново. Сохранить успешный link у одного владельца и вызывать `module_link::call` на нём. Предпочтителен явный mutable transport owner в существующем последовательном цикле; не удерживать синхронный Mutex guard через await. Ошибка транспорта инвалидирует link, не identity операции.

Только сохранённые `module.outcome/result/observe` передоставлять по прежнему ID и неизменным bytes/digest в пределах действующего ACK-контракта. `module.next` — admission, **не чтение**: потеря ответа могла уже перевести работу в sending. Не делать повторный next в новом link как будто ничего не произошло; сохранить существующий reconciliation-only путь до следующего admission.

Нельзя повышать readiness из candidate root hint. `native_root_checkpoint_for_hello` отдельно обозначает подтверждённую Store identity. Не создавать новый writer lease или native prompt при reconnect.

## 4. EOF результата не выводится из старого cursor

В `read_assistant_result` конечная страница сейчас делает `None => break`, оставляя прежний cursor, после чего `cursor.is_some()` ошибочно означает scan limit. Ввести явный флаг достигнутого EOF либо согласованно обновлять cursor перед выходом. Лимит страниц/байтов, повтор cursor и отсутствующий exact input/assistant — разные ошибки. Уже имеющиеся `cursors`/`ids` sets и `validate_assistant_parent` сохранить; «взять последнее сообщение» не является исправлением.

## 5. StopPending должен пережить возврат helper-функции

`NativeOwnerController::shutdown` сейчас делает `active.take()`, а `NativeOwner::shutdown(mut self)` потребляет Child. Изменить ожидание на заимствование; EOF собственного stdin отправляется один раз. Timeout/ошибка wait сохраняют owner, identity, Child и неизвестный исход, но не прежнюю readiness.

**Обязательный второй участок:** `run_owned` сейчас выполняет `native_owner.shutdown().await?; return Ok(())`. Даже исправленный `&mut Child` будет потерян, если этот `?` завершит функцию. На timeout перейти в явное состояние ожидания выхода с сохранённым controller либо передать владение реально существующему подтверждённому owner. Не достаточно сохранить Option внутри функции, из которой сейчас выходят.

В состоянии остановки новые native admissions запрещены; `ensure_started` не возвращает cached ready для StopPending. Не опрашивать завершённый `ctrl_c` future в горячем цикле. Использовать существующий технический backoff/readback, различая живого child, exited child и неподтверждённую process family. External-attach route не останавливает чужой/shared service. Никакой эскалации kill по одному timeout.

## Доноры: точные API и пределы заимствования

- [Tokio 1.53.1, `Child::wait/try_wait`](https://github.com/tokio-rs/tokio/blob/75fef53d0a8590c2d1dbb63672aa7b7d1ef51155/tokio/src/process/mod.rs#L1334-L1410): `wait(&mut self)` cancel-safe и повторно возвращает известный exit. Это позволяет отменить ожидание, сохранив Child; не сохраняет ваш controller, если caller сам его уничтожил. `wait` закрывает оставшийся child stdin; отдельно извлечённым stdin управляет владелец. Kill-пример из документации сюда не переносить.
- [Собственный Command `write_replace_bytes`](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-command/src/journal.rs#L991-L1061): useful temporary-file/file-sync/Unix-directory-sync sequence. Несмотря на имя, существующие другие bytes он отвергает; это **не** general replace и **не** JSONL salvage. Проверка exists перед rename сама не гарантирует атомарный no-clobber против другого writer. Не импортировать соседний adapter crate; common primitive переносить только при реальном общем consumer.
- [tempfile 3.27.0, `NamedTempFile::persist/persist_noclobber`](https://docs.rs/tempfile/3.27.0/tempfile/struct.NamedTempFile.html): изучено как сравнение, не новая зависимость. `persist` не синхронизирует файл/директорию; `persist_noclobber` не обещает атомарность на всех платформах. Название API не заменяет требуемую гарантию.

## Критерии итоговой квалификации — не исполнены этой документацией

| Сценарий | Ожидаемый результат |
|---|---|
| Одна повреждённая op-history и независимая проверенная история | Явная изоляция без ложного fresh state; hello не падает от повторного несогласованного decoder. |
| Сбой после native effect, до ACK; после ACK, до cleanup | Ни одного нового POST; точный исход/страница восстанавливаются из сохранённых фактов. |
| Несколько обычных RPC на healthy link | Один handshake на link; исходы/rights не берутся из чужого boot. |
| Потерян ответ module.next | Нет слепого повторного admission на том же boot. |
| 2+ страницы, EOF на последней; повтор cursor; реальный limit | Успех, cursor-error, limit-error соответственно; exact parent проверен. |
| Stop timeout, затем поздний exit | Child остаётся у живого владельца через caller; новые старты не разрешены; завершение подтверждается readback. |
| External-attach shutdown | Detach наблюдения, без остановки чужого сервиса. |

Один manager/worktree, writers без Cargo. После законченного кода — scoped formatting и минимальный gate:

```sh
cargo clippy --locked -p swarm-adapter-opencode --lib --bins -- -D warnings
```

Tests/native/load — итоговая фаза. Сдать exact SHA, изменения всех названных callers, сохранённые identity/unknown гарантии, реальный gate и остаток. R01/#27 владеет внешним supervisor/helper, R05/#31 — общим dispatch receipt. Их типы не дублировать; не добавлять forms/goal/steer, очистку authoritative истории или второй Store в этот блок.
