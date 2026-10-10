# R08. Доставка: один порядок записи, точный старт наблюдения и восстанавливаемые gaps

**PR #34 · уточнено 7 октября 2026 · код R08 ещё не изменён.**
Основа: AUD-006/007/009/010/014; source `40591a295af94b1541ec2ba30afe8e3247701a71`, исходный head задания `c1e8c3beb178d6ceb7952c4981f17106452b1765`. Реализовать в этой ветке writer → index → cursor reader → subscription; не создавать отдельные PR на неподключённые primitives.

## Цель и документы

Позднее сохранённое сообщение не теряется за cursor. Подписка без after получает точный стартовый cutoff без обхода всей истории. Переполнение и частичный сбой дают согласованный диапазон пропуска, а не ложную непрерывность. Deadline-watch не теряет свой срок из-за порядка if.

Читать: [Program](../../agent-communication-program.md) §2–4; [Peer Implementation](../../agent-communication-peer-autonomy-implementation.md) §2.2 и §6.3; [Peer Autonomy](../../agent-communication-peer-autonomy.md) — watches; [Modularity](../../agent-operations/modularity.md) §2.1; [Owner Decisions](../../owner-decisions.md) §1.2–1.4/2.2. Tool-contracts §6.5/7 — дополнительная детализация, не основание заменить нынешнюю публичную форму inbox старым примером.

## Карта существующего кода

| Путь и функция | Ответственность |
|---|---|
| `swarm-kernel-host/src/store/coordination.rs::{send,inbox,delivery_matches_scope,consult_delivery_matches}` | Scoped delivery index, проверка recipient/context/исходного body, cursor чтения. |
| `swarm-kernel-host/src/coordination/mod.rs::{mailbox_key,mailbox_prefix}` | Текущий timestamp/UUID key; заменить ordering, не opaque delivery identity. |
| `swarm-kernel-host/src/store/mod.rs::mutate_in_transaction_with_authority` | Общий success-путь вставляет raw Observation после apply и до safe events; здесь доступен точный sequence новой записи. |
| `store/message_batch.rs::{process,process_one}` | Batch использует тот же mutate_in_transaction; ACK только после commit общей транзакции. Не добавлять вторую реализацию индекса для batch. |
| `store/mod.rs` — read `report.delta`/`message.read`, `timeline_visibility_sql`, `operation_visible_to` | Authoritative scoped timeline; current права до выдачи и повторный exact operation check. |
| `store/projection.rs::{limit_items,frame,timeline_gap_reference}` | Уже имеющиеся byte/item limits и явные detached references; не переписывать. |
| `swarm-mcp/src/mcp/subscriptions.rs::{PumpSource,SubscriptionHub,poll_loop,deliver_tick,scan_to_head,resync_reads}` | Один отдельный pump IPC, bounded mpsc queues, lifecycle и gaps. Это не broadcast receiver. |
| `swarm-mcp/src/mcp/mod.rs::on_custom_request` | Admission протокольной подписки, связь ACK/start/capability. Найти существующую ветку SUBSCRIBE_METHOD. |
| `swarm-kernel-host/src/coordination/watch.rs::{parse_create,validate_address}`; `store/coordination_watch.rs::{create,reconcile,event_cursor,notifications}` | Точный deadline, expiry, retained subject и current creator authority. |

Пути в таблице относительно `crates/`; сокращённые store-пути относятся к swarm-kernel-host. Новые helpers ниже — предложения реализации, не уже доступные API.

## 1. Не смешивать три cursor spaces

| API | Нынешняя граница | Требование |
|---|---|---|
| Participant `coordination.inbox` | `after_operation_id`, timestamp/UUID index | Versioned scoped scan position, независимая от последней валидной Operation. |
| Thread reads | Message/Observation cursor конкретного thread | Не менять произвольно при исправлении обычного inbox. |
| `report.delta` и subscriptions | Числовой `observation_id` | Сохранить пространство и exact typed resync; не подставлять inbox operation ID. |

`observations.observation_id` уже `INTEGER PRIMARY KEY AUTOINCREMENT` в реально включённой миграции `crates/swarm-kernel-host/migrations/001_core.sql`. Использовать его порядок; не добавлять Snowflake, второй broker или внешний генератор часов. Пропуски номеров сами по себе не означают потерю сообщений.

## 2. Связать inbox index с конкретной вставкой Observation

Сейчас send записывает timestamp-index внутри apply, а общий mutation success затем вставляет raw Observation с source `controller`, source_event_key/operation_id текущей операции и kind=method. После него могут появиться другие normalized events. Поэтому `MAX(observation_id)` или поздний `last_insert_rowid()` не доказывают ID именно доставки.

Предпочтительный путь: получить `observation_id` самой single-row вставки через `INSERT ... RETURNING observation_id`/`query_row`, затем передать его узкому helper индексирования внутри той же транзакции и до ACK. Сверить поддержку RETURNING закреплённой SQLite; не менять dependency ради этого без необходимости. Полученная строка RETURNING ещё не означает commit.

Индексировать только реально созданную доставку. `consult`, ответивший из card, и coalesced consult не получают новый delivery ID/позицию старого сообщения. Сохранить source event key, method, operation/recipient/context и связь с исходной доставкой. Убрать прежнюю независимую timestamp-запись после подключения нового helper, а не поддерживать два конкурирующих актуальных индекса.

На чтении использовать indexed recipient/scope + sequence, потом проверять authoritative receipt/body. Миграция derived index должна сохранять прежние Operations/Observations и быть повторяемой; incomplete rebuild возвращает явный partial/resync, не пустой «готовый inbox». Историю не сканировать заново на каждом пользовательском чтении. Старый after_operation_id либо переводится по проверенной исходной доставке, либо получает явный version/resync error — не трактуется как sequence.

**Снятое подозрение:** в прочитанных direct и batch путях created_at_ms Operation и apply-time now передаются из одного вызова mutate_in_transaction. Отдельный баг «send всегда пишет другую миллисекунду, чем cursor reader» здесь не доказан. Реальная ошибка — немонотонный timestamp/UUID ordering; её и исправлять.

## 3. Cursor продвигается по просмотренному, но не перескакивает невозвращённое

Разделить last_scanned и last_emitted. Отсутствующая/устаревшая ссылка после bounded scan получает gap и позволяет следующую страницу; курсор не обязан ссылаться на валидную Operation этой страницы. Проверять его namespace/version/scope/форму/размер, не принимать произвольный meta key за полномочие.

Первую валидную запись, не помещённую из-за item/byte budget, не считать consumed. Все одновременно возникшие причины неполноты показать независимо. Malformed derived index и отозванный scope — разные исходы; SQLite/commit errors не превращать в пустой список. Существующие payload/body/recipient проверки сохранить.

`hold` не разрешает продвижение пользовательского read cursor как будто скрытая почта прочитана. Stale Participant fallback сохраняет только свои compact watch headers, не историческую почту или context. `refuse` остаётся отказом admission. Никакого model wake из чтения.

## 4. Concrete cutoff до запуска live poller

Сейчас `PumpSource::delta_page` вызывает только `report.delta {after,limit}`. Его `next_cursor` — конец возвращённой страницы, не high-water. `SubscriptionHub::open/subscribe` синхронны, ACK при after=None содержит null, а poll_loop догоняет head от нуля.

Добавить один узкий read-путь получения **разрешённой границы timeline** с тем же текущим Principal/visibility и exact operation checks. Согласовать Store parser/registry, IPC caller и capability в этом PR. Новый helper наподобие `PumpSource::start_position` — ещё не существующий API. Не подменять его глобальным `monitor::journal_cut`, полным report scan, operator credential или fallback=0 при ошибке.

Предпочтительно сначала асинхронно получить/проверить start position, затем передать конкретный cursor в синхронную регистрацию hub. Для head-only пути выбирать bounded metadata, без чтения всех payload; проверить план/scan budget. Если точную разрешённую границу установить нельзя, возвращать явный head-unavailable, а не успешную live-подписку. Не обещать O(1) для фильтрованного SQL без измерения.

Регистрировать Entry и проверять MAX_SUBSCRIPTIONS в одной критической секции; не держать StdMutex через await. Poller не должен вызвать forget до появления владеющей записи. После регистрации запустить его от конкретного cutoff, сохранить отдельный признак requested-live и вернуть cutoff в ACK.cursor и ACK.resync.after. Explicit after сохраняет replay, не заменяется head.

Каждая последующая страница проверяет текущие права. Смена области/авторизации требует явного нового resync, а не использования прежнего cut как новой authority. Read transaction snapshot живёт в пределах чтения cut/страницы, не всю жизнь подписки. Durable replay покрывает интервал между cut и началом polling; потеря volatile hint не теряет запись. Не называть помещение уведомления в mpsc доказательством получения моделью.

## 5. Один результат scan вместо счётчика с побочным эффектом

`scan_to_head` сейчас изменяет внешний dropped counter, но при ошибке следующей страницы возвращает None и теряет уже пройденный cursor. Заменить на один внутренний результат, например **новый** `ScanProgress { examined_through, matched_dropped, reached_cut, failure }`. Cursor/count должны описывать одни и те же полностью обработанные записи.

Закрепить конечный cut для эпизода fast-forward, не догонять постоянно растущий head без границы. Если источник читает страницы дальше cut, не учитывать записи за ним. При частичном сбое сохранять проверенный progress/count вместе и продолжать после него; malformed page/cursor не считать EOF через unwrap_or_default. Пустая страница с has_newer=true не доказывает достигнутый head.

После успешной постановки lagged marker перед новыми элементами продвинуть локальную **покрытую** границу до through_cursor. Сейчас delivered_through на этом пути не меняется, и следующий gap может снова начинаться до уже объявленного пропуска. Не увеличивать число доставленных сообщений на dropped_items: coverage, queue admission, transport send и model consumption — разные факты.

Если marker ещё не поставлен в очередь, не обгонять его новыми уведомлениями и не закрывать эпизод. Один item, совпавший с reports+operations+coordination, порождает одно уведомление с несколькими category tags, а не несколько доставок.

## 6. Способы resync соответствуют фактически отправляемым ID-only событиям

`notification_item` сворачивает координационные события до IDs и для широких Reports/Operations. Но `resync_reads` добавляет Thread/contract reads только явной Category::Coordination; для Concilium широкие категории уже обработаны.

Исправить замыкание resync по всем семействам, которые выбранная категория действительно может выдать: Reports/Operations получают и Concilium, и Thread/contract reads. Согласовать admission/role policy и advertised reads; не рекламировать вызов, запрещённый данной поверхности, и не расширять права ради подсказки. Сохранить запрет Participant-профилю без report.delta обходить свой narrow inbox через subscriptions.

Не переносить сюда новый каталог инструментов: только список recovery reads и соответствующий существующий subscription gate. R14/#40 позже перенесёт общие schemas, не перепишет delivery lifecycle.

## 7. Deadline и expiry: узкое изменение семантики, не перестановка всех if

Parser требует положительный expected_deadline_ms для retained consult reply_deadline_ms и положительный expiry; create проверяет expiry относительно now/TTL. Сравнения expiry с deadline нет. Reconcile сначала делает expired, затем event_cursor: при равных сроках match недостижим.

В owning watch-документе и коде согласовать точную границу. Предлагаемая узкая норма для exact_deadline_reached: новый expiry раньше D отклоняется; D==expiry допускается; событие D не теряется только потому, что тик пришёл позже D/expiry. Сначала валидировать current creator и exact retained subject/deadline, затем применить эту норму. Это **изменение контракта**, а не уже работающая возможность; version старого поведения/записей должен быть явным.

Для остальных kinds не выводить время события из now чтения: учитывать только доказанную occurrence и определённую допустимую границу; без неё не считать expired запись matched. Сохранить one-shot identity, cancel и coalescing. Повтор create сам по себе не обновляет срок старого watch. Старые settled watches не переоткрывать и notifications не replay-ить как новый model input.

## Доноры и точные ограничения

- [CCCC inbox, `1e67dc8`](https://github.com/ChesterRa/cccc/blob/1e67dc8700515acbb2cc6a56c2e546b850f3c559/crates/cccc-core/src/inbox.rs): `mail_pending_summary` идёт newest-first до cursor/actor generation. Полезны locality и различие unread/notice/reply. `list_unread_many` всё ещё использует ledger::inspect; весь донор не является O(1). `consume_unread` меняет cursor и пишет mail.read через PendingRead — не переносить в read-only ELIOT inbox или поверх SQLite как второй файл-протокол.
- [Paseo SessionDelivery, `7fd469a`](https://github.com/getpaseo/paseo/blob/7fd469ae1908a1853cc3ec48eb84b81ff752c4a8/packages/server/src/server/session/owned-subscriptions/index.ts): hasDemand/detach/releaseOwner — образец явного lifecycle-owner. Сам файл отдаёт buffering домену, а detach отменяет также owned operations. Брать только освобождение observation subscriptions; disconnect ELIOT не отменяет принятую Task. AsyncLocalStorage/TS manager и native ownership не переносить.
- [SQLite AUTOINCREMENT](https://www.sqlite.org/autoinc.html), [RETURNING](https://www.sqlite.org/lang_returning.html), [WAL isolation](https://www.sqlite.org/isolation.html), прочитаны 07.10.2026: использовать существующий sequence и transaction. RETURNING сообщает строку вставки, не commit; номера не обязаны быть подряд; одна долгая read transaction не видит новые commits. Это механизмы уже выбранной БД, не новая зависимость.

## Итоговые сценарии — пока не выполнены

| Сценарий | Требуемый исход |
|---|---|
| Поздняя запись той же ms, rollback wall clock, batch из двух sends | Последовательное чтение не пропускает доставку; ACK после commit. |
| Retry/coalesced consult и answered_from_card | Нет нового delivery identity или второй позиции старого сообщения. |
| Полное stale-окно, затем valid item; конец по byte budget | Scan продвигается, valid не перескочен, DB error не скрыт. |
| Большая история + after=None + commit между cut и polling | Concrete ACK.cut; запись после него доставлена либо покрыта явным gap. |
| Успешная первая scan page, ошибка второй; два lag episodes подряд | Count/cursor согласованы; уже покрытый диапазон не объявляется заново как новый пропуск. |
| Reports/Operations с coordination ID-only fact | Recovery reads достаточны и доступны по реальной policy; одно уведомление на item. |
| Expiry <, =, > D; поздний tick; отзыв прав | Явный выбранный контракт; нет незаконного раскрытия и auto wake. |
| Unsubscribe/disconnect/reconnect | Освобожден только observer; старый subscription ID не продолжает жить, native work не затронута. |

## Владение и сдача

R06/#32 владеет context, registration и будущей семантикой двухстороннего envelope/authority predicate. R08 — ordering/index traversal/cursors/watch/subscription. Не делать второй context или круговую зависимость; базовый one-scope delivery ремонт не ждёт проектирования всего межзадачного графа. С R07 согласовать только names реальных новых event families, без своей ratification реализации.

Один manager/worktree, writers без Cargo. После законченного кода — scoped formatting и:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-mcp --lib --bins -- -D warnings
```

Tests/native/load — итоговая фаза. Сдать exact SHA, реальные callers нового index/head/scan helper, version/resync решения, вывод gate и оставшийся scope. В этой поставке обновлён план, не продукт, DB schema или dependencies; документационный CI не доказывает работу курсоров.
