# ELIOT Swarm — checkpoint v18

**29.09.2026. Девять новых находок в собственном проекте сохранены и внесены в документацию. Rust-сервис ещё не реализован.**

## Действующий комплект

[Архитектура v18](agent_swarm.md), [план v6](agent_swarm.implementation-v6.md),
[контракт модулей v2](agent_swarm.module-contract-v2.md), [reference v18](agent_swarm.spec-v18/README.md),
[предметный аудит](agent_swarm.design-review-v18-20260929.md).
[Полный пакет](agent_swarm.docs-v18-20260929.zip) и [manifest](agent_swarm.docs-v18-manifest.json) определяют согласованную поставку.

## Сохранённые исправления

Origin identity импортированной Task; один initial dispatch на Attempt; revalidation на begin_send;
COMMIT до native вызова; результат проверки отдельно от resource release; active-check reuse внутри
одной Attempt; prepare/settings interleaving; dependency receipt revalidation; stable caller/GM роль;
retention idempotency/evidence. Девять таблиц, один host, без нового broker/framework.

Исходные root файлы и packaged v17 metadata сохранены в review-v18/source-v17. Отдельно смонтированные
checkpoint/donor header были v14/v12; основой служит полный v17 ZIP. Исходный brief не менялся.

## Что выполнено и чего нет

Reference SQL и условные state traces выполнены; результаты в [validation](agent_swarm.spec-v18/validation-results.json).
SQL engine — Python sqlite3 3.46.1, in-memory, single connection. Это не запуск WAL-профиля продукта.
Counterexamples подтверждают свойства DDL, не поведение несуществующего Rust Store.

Не выполнены: C01–C11, native SDK install/build, Clippy нового приложения, Windows IPC/Job, live модели,
оплачиваемые пробы, нагрузка и изменения GitHub. Donor pins сохранены.

## Точка продолжения

**C01: model/config/Store и закреплённая initial schema v18; C02: host/CLI/IPC.**
Далее Muse Max и OpenCode V2. Реализовывать рабочий путь, не новый раунд выбора платформ.
Минимальный fmt/Clippy после кода; broad tests не ставятся перед feature-complete срезом.

Если отдельные вложения расходятся по версиям, сначала сверить manifest текущего ZIP. Не откатывать
готовую документацию на старый checkpoint лишь потому, что файл снова смонтирован с прежним именем.
