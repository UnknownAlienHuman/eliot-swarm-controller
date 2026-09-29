# Проверка универсальности и качества — v17

**29.09.2026. Проверка собственного проекта, не нового набора upstream-продуктов.**

## Основа

Прочитаны архитектура v16, implementation-v4, reference SQL, donor inventory, актуальный brief и harness intake.
Материалы последнего `docs-v16-harness-intake` ZIP использованы как полная предыдущая редакция: отдельно
смонтированные `agent_swarm.md`, checkpoint и donor inventory оказались старее содержимого пакета.
Оба варианта сохранены в `review-v17/source`; новых donor pins при согласовании не добавлено.

Внешняя проверка ограничена первичными техническими источниками Tokio/Cargo/SQLite из контракта модулей.
Повторное исследование возможностей всех семи vendor CLI в этой итерации не заявляется.

## Существенные изменения

| ID | Было недостаточно определено / наблюдение | Решение и место |
|---|---|---|
| U01 | DDL v16 разрешает две строки ready с одним native_root_id в разных lanes | native_scope_key + partial UNIQUE; Store.record_native_identity; main §4–5 |
| U02 | Слово «успешно завершён prerequisite» не фиксирует требуемый native apply | completion_condition в существующем effective_request_json; main §7, contract §6 |
| U03 | Bounded bus может задержать native reply за reporter/телеметрией | Раздельные пути, быстрый classify, справедливый dispatch; main §6 |
| U04 | Generic supports_* недостаточно для разных входов CLI | Малый RuntimePort + operation semantics + role requirements; contract §4–5 |
| U05 | Общая модель версий легко делает любой новый vendor field глобальным отказом | Additive fields не ломают core; mandatory mapping локально unavailable; contract §11 |
| U06 | Фраза «многопоточно» скрывает невозможность abort запущенного blocking closure | Short bounded CPU jobs отдельно, long SDK readers своими threads/processes; contract §8 |
| U07 | Сохранённый due slot не задаёт поведение Tokio timer после паузы | Explicit Skip/Delay + прежняя slot/Operation transaction; contract §9 |
| U08 | «Быстро подключать модели» могло требовать менять общий API каждый раз | Data-only model alias, один adapter на новый protocol; native extensions без изменения core |
| U09 | В основном документе накапливались детали семи vendor и старых проходов | Основной текст сокращён; native audit v16 сохранён отдельно; формальный seam вынесен в contract |

## Что проверено непосредственно

Контрпример U01 воспроизведён SQL в памяти: две строки с разными lanes и одинаковой native identity
принимались v16. Это недостаток reference DDL, не сообщение об аварии существовавшего приложения.

В v17 индекс отклоняет вторую unreleased identity того же namespace; одинаковое имя в другом namespace
разрешается. Устаревшая освобождённая строка не мешает новому control owner. Store всё ещё обязан проверить
правильность scope, реальные ownership facts и условия передачи; индекс не доказывает это вместо него.

Проверки JSON/TOML/Markdown/SQL выполняются отдельной утилитой этого комплекта. Полный список исходов —
`agent_swarm.spec-v17/validation-results.json`. Это не test framework продукта и не benchmark Windows/Tokio.

## Что не поменялось

Один Rust crate, один host, одна DB-thread, девять таблиц. Нет UI, inference proxy, нового broker,
workflow-language, дополнительных подписей/проверок каждого tool call, forced compaction или timebox Issue.
Native permissions и подписочные маршруты сохраняются. Первые C01→C02→Muse→OpenCode V2 остаются прежними.

В отличие от прежней жёсткой формулировки «один bridge на lane», контракт допускает native multiplex;
первая реализация не обязана его строить и не выдаёт его за измеренную экономию.

Донорский код берётся целой нужной единицей. Согласование metadata inventory не изменяет source pins.
Не переносим без проверки способ отмены/повтора чужого SDK только потому, что это готовый код.

## Граница готовности

C01–C11 ещё не реализованы. Нет Rust executable, donor build/install, actual Max probe, Windows IPC/Job
квалификации, live work или нагрузки. Проектные 200 entities / 1000 events/s остаются целями, не результатом.
Главный документ и контракт содержат нормы реализации; никакой уровень «без ошибок» ими не доказан.

## Технические источники

- Tokio channels/backpressure: https://tokio.rs/tokio/tutorial/channels
- Tokio 1.53.1 spawn_blocking: https://docs.rs/tokio/1.53.1/tokio/task/fn.spawn_blocking.html
- Tokio missed ticks: https://docs.rs/tokio/1.53.1/tokio/time/enum.MissedTickBehavior.html
- Cargo dependency overrides: https://doc.rust-lang.org/cargo/reference/overriding-dependencies.html
- SQLite partial indexes: https://www.sqlite.org/partialindex.html

Прочитано 29.09.2026. Ни один из этих документов не подтверждает, что прототип уже собран с указанной библиотекой.
