# Reference-комплект v18

**Уточнено 30.09.2026. Проектные артефакты для C01–C06. Приложения и рабочей БД ещё нет.**

[Initial DDL](migrations/001_core.sql) сохраняет девять таблиц. Ранее добавлены
`tasks.origin_key`, `attempts.start_operation_id`, `check_runs.resource_claimed_at_ms/resource_released_at_ms`.
Уточнение 30.09 добавляет `attempts.start_owner`, `tasks.accepted_operation_id` и `check_runs.cached_from_check_id`.
Это редакция будущей первой схемы (`user_version=1`), не исполняемая миграция рабочей БД.
Старую reference DB нельзя открыть как новую только из-за совпадающего user_version; при реализации C01 фиксируется одна initial schema.

## Предметные примеры

| Файл | Граница |
|---|---|
| [Origin](examples/task-origin-deduplication.json) | Одна Issue не порождает два root Task через aliases |
| [Initial dispatch](examples/single-initial-dispatch.json) | Один начальный prompt; native-manager claim не принимает controller dispatch до регистрации ребёнка |
| [Guard](examples/dispatch-guard-races.json) | Revise/drain до и после native-send admission |
| [Ресурс](examples/check-resource-release.json) | Incomplete результата не освобождает неизвестный процесс |
| [Check dedupe/cache](examples/active-check-deduplication.json) | Активная проверка своей Attempt; reuse готового process result без выдуманного нового процесса |
| [Настройки](examples/settings-interleaving.json) | Старое applied не означает текущий Max |
| [Dependencies](examples/dependency-revalidation.json) | Отзыв адресован одному решению; повторная приёмка того же SHA имеет другой ID |
| [Family](examples/runtime.family.coverage.json) | Полнота списка и конкретная активация повторно используемой child session |
| [Reconnect](examples/caller-reconnect.json) | Stable caller отдельно от link/GM epoch |
| [Retention](examples/retention-boundary.json) | Evidence/idempotency не стираются вместе с telemetry |

[SQL фрагмент begin_send](transactions/begin-initial-send.reference.sql) проверяет только часть initial-dispatch guards,
включая `start_owner=controller`. Store также проверяет authority/settings/dependencies в той же транзакции;
RETURNING не разрешает native-send до COMMIT.

`cached_from_check_id` ссылается на исходный process CheckRun. Cached row не получает собственный exit code,
PID или resource claim. Проверка source passed/complete/not-invalidated, inputs и profile остаётся в Store:
SQL FK подтверждает существование строки, не пригодность её результата. Ссылки cache→cache разворачиваются
в исходный process result; собственный semantic verdict чужой Task из кэша не переносится.

`accepted_operation_id` различает решения при неизменных revision/phase/candidate.
Отзыв old decision не очищает new decision; request_changes сравнивает ещё и submission_ref.
`start_owner` задаётся явно в Store и не меняется после резервирования Attempt.

Остальные примеры иллюстрируют собственный протокол, не команды vendor API.
TOML остаются disabled-шаблонами, runtime capabilities не объявлены квалифицированными.

## Граница прежней проверки

[Validation 29.09.2026 в истории](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/b5a437f57488f8ddcdcc3f4aaea24746a3ea1f62/docs/agent_swarm.spec-v18/validation-results.json) относится к исходной редакции, не к последующим изменениям.
При чистке документации 30.09 SQL/JSON/TOML сохранялись; последующий аудит уже изменяет описанные выше контракты.

Повторная проверка 30.09: 15 направленных DDL/SQL-проверок в in-memory SQLite 3.46.1, включая старый
cache-контрпример, новые CHECK/FK, оба пути старта и CAS адресного отзыва acceptance. Модельный native spawn
в последовательности — предположение сценария, а не выполненный вызов CLI. Полнота family, Windows Jobs,
реальный Store/IPC, многопоточность и SDK не проверены. C01 не выполнен существованием этой схемы.

[Архитектура](../agent_swarm.md) · [план v6](../agent_swarm.implementation-v6.md) ·
[контракт v2](../agent_swarm.module-contract-v2.md) · [выводы](../lessons-learned.md#31-проверка-согласованности-контрактов-30092026) ·
[статус](../../README.md).
