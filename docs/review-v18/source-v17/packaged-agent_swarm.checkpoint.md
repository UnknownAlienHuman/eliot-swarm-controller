# ELIOT Swarm — checkpoint v17

**29.09.2026. Архитектурное уточнение и reference artifacts; Rust-приложение ещё не реализовано.**

## Действующий комплект

- [Архитектура v17](agent_swarm.md).
- [План реализации v5](agent_swarm.implementation-v5.md).
- [Контракт модулей v1](agent_swarm.module-contract-v1.md).
- [Reference schema/examples v17](agent_swarm.spec-v17/README.md).
- [Разбор изменений](agent_swarm.design-review-v17-20260929.md).
- [Донорский реестр](agent_swarm.donors-20260929.toml): прежние pins, не install lock.

## Что сделано

Согласованы typed operation outcome, readiness после configure, native identity, role requirements,
backpressure и updates. Основной документ сокращён, native mappings v16 сохранены отдельно.
SQL initial v17: девять таблиц, одно новое поле native_scope_key, NULL-pair и partial unique native owner.
Контрпример alias-коллизии прежней схемы сохранён. Это проверка DDL, не исправление работающего сервиса.

При начале прохода отдельно смонтированные main/checkpoint/donors были старее содержимого
последнего v16-harness-intake ZIP. Оба варианта сохранены в review-v17/source; основой обновления
выбран полный последний пакет. Исходный brief и входное исследование не изменялись.

## Фактическая готовность

C01–C11 не выполнены. Нет нового Rust executable, сборки SDK, Windows IPC/Job-квалификации,
model/Max-пробы и нагрузочных измерений. Structural results —
[validation-results.json](agent_swarm.spec-v17/validation-results.json).

Следующая реализация — C01 model/config/Store с текущей initial schema, затем C02 host/CLI/IPC.
Первый путь: Task → claim/dispatch → Operation → статус → повтор без дубля.
Native identity uniqueness резервируется при attach/получении identity; полноценный SDK не нужен для C01.
Затем Muse Max и OpenCode V2. Не возобновлять проектирование framework до работающего среза.

## Сохранение

Текущие документы, схема, примеры, validator, diffs и исходные версии включаются в docs-v17 ZIP.
progress.json отмечает промежуточные стадии; завершённый stage подтверждает только сохранение
и структурную проверку документов, не успешное исполнение native-модулей.
