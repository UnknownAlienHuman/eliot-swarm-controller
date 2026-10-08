# ELIOT — рабочая карта серии PR #27–#40

**Срез 7 октября 2026, после уточнения R01/R02/R05.** В серии 14 открытых draft PR: **3 содержат изменения product-кода, 11 пока содержат задания. Ни один не слит и ни один полный блок не объявлен квалифицированным.** Это снимок GitHub, не обещание фонового исполнения агентов.

Main повторно прочитан: `40591a295af94b1541ec2ba30afe8e3247701a71`. Перед началом перечитать head выбранного PR и его diff: таблица ниже может устареть после чужого commit. Код добавлять в существующую ветку, не создавать новые PR на отдельные DTO/handler/reader. Все ветки серии направлены в main, а не друг в друга.

## 1. Где мы и какое следующее действие

| PR / блок | Состояние | Начать со следующего действия |
|---|---|---|
| [#27 / R01](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/27) | Задание уточнено; кода нет | По `01-module-lifecycle.md`: birth constructor → перенос verified prior owner → raw/validated receipt fan-in → same-boot status → installed/source separation. |
| [#28 / R02](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/28) | Задание `c41a7661bc7d56c4e8a6ad0eede76d4fa05dff39`; кода нет | Один decoder для recovery и hello; затем exact ACK/IPC. Stop исправлять и в NativeOwner, и в вызывающем run_owned. |
| [#29 / R03](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/29) | Задание; кода нет | Exact current-turn steer вместо запрета из-за page_limited всей истории; оставить native expectedTurnId guard. |
| [#30 / R04](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/30) | Код `f269b3d4dea2c5e15754feba9971b1f1c1b20c2b`; пакет не готов | Остался module.example.json с bridge.7 при коде bridge.8: прежняя запись заблокирована инструментом. Не обходить блокировку и не активировать несовпадающий пакет. Затем квалифицировать root/child pending races. |
| [#31 / R05](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/31) | Задание `30b57ced92d4b8088c3a841d27b7b98de6c26722`; кода нет | Передать expected Attempt в Claude helper, использовать существующий sealed origin и добавить 3 outer/inner сравнения receipt. |
| [#32 / R06](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/32) | Задание; кода нет | Согласовать current_scope/registration/fingerprint и code-scope consumers; затем явную разрешённую relation двух заданий. |
| [#33 / R07](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/33) | Задание; кода нет | Proposal writer/reader digest и реальный ratify/reject путь; затем согласовать terminal Concilium, не скрывая history/dissent. |
| [#34 / R08](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/34) | Задание; кода нет | Durable sequence writer/index/reader и migration/resync; subscription cutoff в ACK; deadline отдельно от expiry. |
| [#35 / R09](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/35) | Задание; кода нет | Exact assignment replacement без выдуманного результата и сброса Attempt state; SQL/scan budget до review_view. |
| [#36 / R10](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/36) | Код `4c2f3a007acfcc0b418936253ccc6926b918661c`; не квалифицирован | Проверить весь disable → receipt путь на exact IDs и degraded links; завершить минимальный Rust gate, не расширять catch до DB/commit errors. |
| [#37 / R11](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/37) | Частичный код `8d27b0c2fea5a61ac544b15c29b0d77291049053` | Producer secondary_codes исправлен. Остались общий closed terminal codec, legacy aliases AUD-036 и квалификация; старые cursors не перематывать. |
| [#38 / R12](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/38) | Задание; кода нет | Source-local outcomes, no-progress backoff и Store-owned issuance cursor. DB failure не выдавать за локальную пропущенную запись. |
| [#39 / R13](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/39) | Задание; кода нет | Malformed ledger ≠ empty; exact execution terminal и exact lease owner вместо любой queued операции Task. |
| [#40 / R14](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/40) | Задание `fc34714586713e2b769897afb70b1f4f224e8199`; кода нет | Реальные host→MCP callers, data-only registry/schema extraction. Кэшировать конечные wire-варианты, не права; сохранить digest v1 bytes. |

**Приоритет реализации:** R01, R05, R06 и завершение R10. Независимые R02–R04 можно готовить рядом. Это не требование одновременно запустить определённое число менеджеров.

## 2. Проверка кода — отдельный статус

- [#26](https://github.com/UnknownAlienHuman/eliot-swarm-controller/pull/26), head `e3d1f6f0f38b81639bc72c3dd84d823c0287135f`, остаётся открытым compiler-baseline PR. Его body содержит отчёт автора, не новое доказательство успешной сборки нашей серии. Не копировать одни compiler fixes во все ветки и не менять owner policy ради красного CI.
- [#37, CI 37701138311](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37701138311) повторно прочитан: formatting failed на Windows/Linux; Clippy и integration steps skipped. Документация прошла. Пять незатронутых source-файлов из ранее прочитанного Linux log остаются отдельным baseline-formatting долгом, не повод глобально форматировать их из R11.
- [#30, CI 37697108986](https://github.com/UnknownAlienHuman/eliot-swarm-controller/actions/runs/37697108986): сохранённый результат предыдущего прохода — docs/JS syntax success; SDK import/selftest/native не квалифицированы. Head PR сейчас повторно прочитан; новый запуск CI не выполнялся.
- Для #36 сохранена проверка diff/плана SQL и неудачная локальная попытка Clippy (`cargo` отсутствовал). Это не passing Store test и не Rust qualification.

Практический следующий шаг владельца compiler baseline: закончить #26 и необходимое форматирование его проверяемой базы в одном согласованном участке, затем повторить минимальный gate. **Не сливать автоматически.** Остальные менеджеры продолжают код/документацию; blocker итоговой сборки указывают конкретным package/diagnostic и SHA.

## 3. Что читать новому агенту

Нужен файл выбранного задания в его PR, а не все аудиты и комментарии. Каждое задание содержит цель, существующие symbols, порядок изменения producer → consumer, constraints, сценарии результата и scoped gate. Для R01/R02/R05 добавлены таблицы пограничных состояний; предлагаемый новый API явно помечен как новый.

Быстрый вход в уже имеющейся рабочей копии (чтение, не создание нового worktree):

```sh
git status --short
git rev-parse HEAD
git diff --stat origin/main...HEAD
```

Сверить фактические refs; не доверять локальному origin/main как свежему без обычной синхронизации. Не делать reset/stash/clean поверх чужой работы. Нормы: [owner-decisions](../../owner-decisions.md) §1.2–1.4, [modularity](../../agent-operations/modularity.md) §2–3. Ветка и brief не заменяют Task/Attempt/candidate identity.

После чтения строить одну завершённую функцию продукта, не ещё один общий framework. У каждого нового helper должен быть реальный production caller в том же PR. «Написан тип» или «прошёл docs CI» не равно выполненному заданию.

## 4. Зависимости: реальные и только интеграционные

```text
R06 (#32) → R07 (#33), R13 (#39): общий work context
R07/R08/R09 → R14 (#40): порядок переносов frontend, чтобы избежать конфликтов
```

R08/R09 могут готовиться отдельно и принять общий context при интеграции. R10/R11/R12 независимы друг от друга. R01/R02/R05 не ждут нового context из R06: их проверяемые identity уже доступны. Номер PR не задаёт последовательный конвейер из четырнадцати шагов. Красный чужой package не превращает документальную работу или независимый adapter в BLOCKED.

## 5. Владельцы общих функций

| Участок | Разделение изменений |
|---|---|
| `store/coordination.rs` | R06: context/registration/fingerprint/participant listing/normalize_send. R07: proposal/decision. R08: inbox/delivery index. R09: review-specific bind/pending guards. |
| `store/code_scopes.rs` | R06: propose/accept identity и expiry-before-override. R13: active/conflict readers и collision domain. |
| `store/submissions.rs` | R05: candidate provenance и связанный artifact read. R09: review replacement/disposition seam. |
| `swarm-contracts/src/runtime.rs` | R05: outer/inner dispatch receipt; остальные adapters используют этот validator. |
| `store/mod.rs` | Только необходимые named dispatch/codec hooks; R11 — terminal producer. Не глобальный рефакторинг dispatcher из соседней задачи. |
| Supervisor / OpenCode | R01: общий helper/receipt/status. R02: NativeOwner/Child и его outer run loop внутри adapter. |
| MCP/CLI/registry | R07/R08/R09 добавляют необходимые формы без перестановки файлов; R14 выполняет последующий data-only перенос. |

Одну функцию правит один владелец. Второй использует согласованный результат; совпавший filename не требует остановить весь PR. Writer не получает собственный worktree и не запускает Cargo. Manager интегрирует и проверяет весь кандидат.

## 6. Доноры: функция, гарантия, ограничение

| Источник | Применить | Не переносить / ограничение |
|---|---|---|
| [Tokio watch 1.53.1](https://docs.rs/tokio/1.53.1/tokio/sync/watch/struct.Sender.html) | In-place `send_modify`/`send_if_modified` для R01 | `false` у send_if_modified не откатывает mutation; status не durable receipt, guard нужен отдельно. |
| [Tokio Child 1.53.1](https://github.com/tokio-rs/tokio/blob/75fef53d0a8590c2d1dbb63672aa7b7d1ef51155/tokio/src/process/mod.rs#L1334-L1410) | Cancel-safe `wait(&mut self)` и последующий `try_wait` для R02 | Не сохраняет внешний owner при выходе caller; не разрешает kill чужой process family. |
| [Command journal](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-adapter-command/src/journal.rs#L991-L1061) | Private temp/file sync/directory sync как образец последовательности | private `write_replace_bytes` отвергает different existing bytes; не готовый salvage и не импорт соседнего adapter crate. |
| [ELIOT sealed Claude origin](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/crates/swarm-kernel-host/src/store/results.rs#L206-L410) | Вернуть проверенный origin и сравнить его с expected Attempt в R05 | Ещё один self-consistency hash не доказывает принадлежность ожидаемой работе. |
| [RMCP schema utilities](https://github.com/modelcontextprotocol/rust-sdk/blob/08e021153ef0530aeb0bb406ebb360a38cfb8ee4/crates/rmcp/src/handler/server/common.rs) | Immutable schema reuse для R14 | Thread-local cache; schema_for_input меняет title/description; не готовый exact-byte cache ELIOT. |
| CCCC / Paseo | R08: locality чтения и ownership subscription; ссылки в его задании и аудите | Не второй ledger, TS session manager или владение Task со стороны observer. |

SHA/API-версия — источник проверки, не инструкция обновить установленный runtime. В этой поставке donor-код не скопирован и зависимости не изменены.

## 7. Готовность поставки

Сначала законченный код; затем manager выполняет scoped formatting и минимальный warnings-denied Clippy из задания. Для JS-only — соответствующий syntax gate. Полные tests/native/load остаются итоговой фазой; не запускать их от writer и не отмечать будущие сценарии выполненными.

Сдача в том же PR: `candidate SHA → изменённый путь и callers → удалённые дубли → выполненная команда/exit → оставшееся`. Не нужны новый журнал прогресса на каждый агент и дополнительные файлы-сдачи. Обновлять существующую карточку/PR body, а не заставлять исполнителя искать актуальный план среди комментариев.

В текущем проходе меняются только R01/R02/R05, эта карта и описания PR. Нового product-кода, merge, private config, native processes, credentials, БД и policy-изменений нет. Проверка Markdown не является Clippy или проверкой поведения будущего Rust-кандидата.
