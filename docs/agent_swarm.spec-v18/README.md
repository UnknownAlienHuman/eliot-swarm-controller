# Reference-комплект v18

**29.09.2026. Проектные артефакты для C01–C06. Приложения и рабочей БД ещё нет.**

[Initial DDL](migrations/001_core.sql) сохраняет девять таблиц. Относительно v17:
`tasks.origin_key`, `attempts.start_operation_id`, `check_runs.resource_claimed_at_ms/resource_released_at_ms`;
active CheckRun uniqueness теперь по Attempt+cache key, resource unique до explicit release.
Это редакция будущей первой схемы (`user_version=1`), не исполняемая миграция рабочей БД.
Старую reference DB нельзя открыть как новую только из-за совпадающего user_version; при реализации C01 фиксируется одна initial schema.

## Предметные примеры v18

| Файл | Граница |
|---|---|
| [Origin](examples/task-origin-deduplication.json) | Одна Issue не порождает два root Task через разные aliases |
| [Initial dispatch](examples/single-initial-dispatch.json) | Один начальный prompt Attempt, даже при новом request ID |
| [Guard](examples/dispatch-guard-races.json) | Revise/drain до и после native-send admission |
| [Ресурс](examples/check-resource-release.json) | Incomplete результата не освобождает неизвестный процесс |
| [Check dedupe](examples/active-check-deduplication.json) | Нет неоднозначного active reuse между Attempt |
| [Настройки](examples/settings-interleaving.json) | Старое applied не означает текущий Max |
| [Dependencies](examples/dependency-revalidation.json) | Отзыв evidence отличается от появления новой revision |
| [Reconnect](examples/caller-reconnect.json) | Stable caller отдельно от link/GM epoch |
| [Retention](examples/retention-boundary.json) | Evidence/idempotency не стираются вместе с telemetry |

[SQL фрагмент begin_send](transactions/begin-initial-send.reference.sql) проверяет только часть initial-dispatch guards.
Он запускается внутри Store-транзакции с дополнительными authority/settings/dependency проверками;
RETURNING не разрешает native-send до COMMIT.

Остальные сохранённые примеры v17 иллюстрируют собственный протокол, не команды vendor API.
TOML остаются disabled-шаблонами, runtime capabilities не объявлены квалифицированными.

[Validation](validation-results.json) разделяет SQL fixtures, последовательные модели и синтаксическую проверку документов.
Полнота native family, реальные Windows Jobs, многопоточность Rust/SDK и модельное исполнение здесь не проверялись.

[Архитектура](../agent_swarm.md) · [план v6](../agent_swarm.implementation-v6.md) ·
[контракт v2](../agent_swarm.module-contract-v2.md) · [review](../agent_swarm.design-review-v18-20260929.md) ·
[checkpoint](../agent_swarm.checkpoint.md).
