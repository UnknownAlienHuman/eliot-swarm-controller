# R11. События: host failure и точные runtime aliases

**Статус:** в PR #37 добавлено узкое исправление producer для AUD-035. AUD-036, общая типизация terminal payload и поведенческая квалификация остаются открыты. PR сохраняет Draft; целиком R11 не выполнен.

Исследован `main` `40591a295af94b1541ec2ba30afe8e3247701a71`; работа продолжена поверх задания `5d503c4d969c5d9e2c18d3ea6a683e67fc22c16b`. Перед следующей правкой перечитать текущую ветку. Аудиты — доказательный материал, не новая owner policy.

## Требуемый результат

Отказ host с дополнительной диагностикой остаётся routable. Runtime outcomes проходят соответствующие проверенные codecs; два представления одного события не допускают два одинаковых действия. Права, происхождение события и exact scope проверяются при admission.

## Читать адресно

- [Observability](../../agent-operations/observability.md), §1 и «Host terminal events and retained diagnostics»: durable fact отдельно от подробной диагностики; граница исправления старых записей.
- [Architecture](../../agent-operations/architecture.md), разделы event intake и typed action consumers.
- [Modularity](../../agent-operations/modularity.md), §2.1 и §3: одна транзакция event/cursor/action и descriptor-admitted module metadata.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4 и §2.2: manager/worktree, фаза проверки, live ownership и retention.

## Участок и внесённый код

`crates/swarm-kernel-host/src/store/host_lifecycle.rs::retain_exit` больше не добавляет `secondary_codes` в нормализованный terminal `host.exit`. Полный validated `Exit` по-прежнему записывается в `host:last-exit:v1` / `host:latest-failure:v1`; `finish`, `exit_receipt` и status readback сохраняют коды.

Это восстанавливает уже существующую форму `store/mod.rs::insert_safe_host_terminal_failure_event` и требования `automation_intake.rs::host_terminal_exit_projection`: закрытый набор полей, точные source/key/epoch, совпадение paired payload и времени. Allowlist не расширен, проверка парности не удалена. Изменение source — удаление трёх исполняемых строк и пояснение из двух строк; нет новых dependencies, таблиц, методов или другого event store.

**Не исправлено этой правкой:** старые наблюдения с лишним полем и уже продвинутые consumer cursors. Нельзя переписать immutable history либо перемотать cursor и автоматически повторить script/native effect под видом ремонта. Для исторического восстановления сначала нужен точный admission/readback-контракт.

## Оставшаяся реализация в этом же PR

1. Свести два terminal builders к одному closed DTO/codec, сохранив нынешние serialized bytes и существующие guards. Не переносить весь `store/mod.rs` ради небольшого типа. Producer-коррекция выше не выдаётся за завершённое устранение дублирования.
2. В `automation_dispatch.rs::script_event_projections_with_alias` разделить ветви по source-family + event kind. Уже работающий `accepted runtime.outcome` оставить на direct `native_input_accepted` пути.
3. Пропускать legacy applied/rejected/unknown в их provenance-validated alias path, а не в общий module-metadata return. Произвольный module event допускается только через заявленный descriptor envelope. `None` не разрешает общий fallback; `runtime.state` требует собственного доказанного codec.
4. Сверить dedup/occurrence в source admission и consumer cursor. Не делать новые model calls, retry policy или изменения бизнес-правил автоматизации.

## Критерии итоговой квалификации

- [ ] Failure с 0/1/2 secondary codes: оба safe terminal payload совпадают, diagnostic receipt сохраняет коды, одна семантическая occurrence на один consumer/action.
- [ ] Graceful exit, startup/runtime/supervisor failure сохраняют свои исходы; graceful exit не стирает latest failure.
- [ ] Неверный source/epoch, отсутствующий или несовпавший sibling и лишние поля не проходят проекцию.
- [ ] Accepted direct path не регрессирует; legacy applied/rejected/unknown проходят только с правильным provenance. Незаявленное module-событие не получает fallback.
- [ ] Старые malformed pairs не объявляются восстановленными; прошедший cursor не перематывается и эффект не повторяется автоматически.

## Проверка этой поставки

Исходный `host_lifecycle.rs` восстановлен побайтово и сверен с blob `1b1ef44b1aecb70cd0876a83bd3a75b639d7b5cd`; изменённый source имеет blob `5cbaf1869c0ad3a9ebe7dd45459d325a5e6fabeb`. Это проверка точности исходника и диффа, не выполнения Rust.

Минимальный gate был вызван:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Результат локально: `cargo: command not found`, exit 127. Компиляция, Clippy, тесты и native-квалификация не подтверждены. Чекбоксы не отмечены по чтению кода. Исходные compiler fixes PR #26 здесь не копируются.

## Донор и границы интеграции

[CloudEvents 1.0.2, required id/source](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/spec.md#id) использован как сравнительный контракт: distinct event получает уникальную пару source/id, повторная доставка может сохранить её. Это не гарантия exactly-once исполнения и не authority. ELIOT не переводится на CloudEvents wire format; SDK/брокер не добавлены.

R11 независим от R10/R12. В `store/mod.rs` затрагивать только named terminal producer; R07 владеет своим contract-event family. Один manager/worktree, writers без Cargo; код интегрировать в этот же PR, затем scoped gate. Широкие tests/live — итоговая фаза.
