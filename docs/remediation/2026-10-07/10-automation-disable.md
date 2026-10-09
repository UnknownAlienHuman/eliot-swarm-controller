# R10. Автоматика: точный ID и надёжное выключение

**Статус: реализация квалифицирована rustfmt и changed-line Clippy на Ubuntu и Windows; тесты остаются в итоговой продуктовой фазе.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-039, AUD-040.
Исторический SHA фиксирует источник доказательства и не ограничивает текущую версию продукта.

## Результат

Валидное `enabled=false` фиксируется независимо от полноты `affected_work` диагностики; допустимые automation IDs никогда не смешиваются SQL wildcard matching.

## Читать адресно

- `docs/agent-operations/configuration.md` — `automation.config.preview/apply`, revision guards, disable/narrowing и affected work.
- `docs/agent-operations/modularity.md` — §4: effective policy и уменьшение церемонии.
- `docs/owner-decisions.md` — §1.4: observation не меняет уже принятую работу.

## Участок кода

`crates/swarm-kernel-host/src/store/automation.rs`: `operation_impacts`, `apply`, `explain`; `src/automation/config.rs`: `entry_operation_prefix`, ID validator. Mutation savepoint в `store/mod.rs` остаётся границей и не переписывается.

## Реализованная семантика

1. `operation_impacts` использует точный бинарный диапазон `[prefix, successor)`, а не `LIKE`. `_`, `%` и ASCII case больше не получают семантику pattern matching.
2. Каждая найденная запись всё равно проходит повторную проверку exact on-behalf link по owner/project/automation.
3. Missing, damaged или unsupported sealed link закрывает всю производную коллекцию через bounded `closed.status=degraded`; уже прочитанный префикс не выдаётся за полный ответ.
4. Валидный disable/narrowing не откатывается только потому, что derived `affected_work` нельзя доказать полностью.
5. SQLite, transaction и commit failures не классифицируются как degraded diagnostics и продолжают завершать mutation ошибкой.
6. Disable не отменяет уже принятую native работу и не разрешает replay неизвестного эффекта.

## Критерии поведения итоговой тестовой фазы

- `audit_one`, `auditXone` и `Audit_one` возвращают только собственные операции.
- Повреждение derived link не откатывает разрешённый `enabled=false`; ответ содержит incomplete/degraded диагностику.
- Неверный owner/revision и настоящий отказ commit не дают успешного receipt.
- После disable новый admission невозможен; уже начатая native работа не убивается и uncertain effects не повторяются.

## Границы

Нет массовой замены всех `LIKE`: hashed owner/project prefixes не являются тем же дефектом. Нет нового ACL, Store, writer, event engine или ослабления authorization. Диагностический readback не становится authority.

R11 владеет event payloads, R12 — scheduler pacing. Они не блокируют этот независимый срез.

## Квалификация

Exact head `f7b739dd68900361169e5e7d3c25e146b1e7c470`, workflow run `37881436670`:

- changed-path/package classification — passed;
- documentation validation — passed;
- changed Rust files rustfmt-clean — passed;
- selected package and real local reverse-dependency closure compiled and completed changed-line Clippy on Ubuntu — passed;
- same scoped compiler/Clippy gate on Windows — passed;
- no compiler error or warning owned by a changed Rust line remained;
- changed integration tests correctly skipped because this slice changes no integration test target.

Full tests, native/live and load qualification remain the final product phase under the project rule.