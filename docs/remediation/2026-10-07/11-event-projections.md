# R11a. Host terminal safe payload: diagnostics remain in the retained receipt

**Статус: узкий producer-fix AUD-035 реализован в PR #37; rustfmt/Clippy qualification выполняется на текущем main. Общий terminal DTO и legacy runtime aliases вынесены в Issue #78.**

Основа исследования: `main` `40591a295af94b1541ec2ba30afe8e3247701a71`. Исторический SHA фиксирует доказательство и не является требованием версии.

## Результат этого PR

`crates/swarm-kernel-host/src/store/host_lifecycle.rs::retain_exit` больше не добавляет `secondary_codes` только в нормализованный `host.exit`.

Полный validated `Exit` по-прежнему сохраняется в:

```text
host:last-exit:v1
host:latest-failure:v1
```

и остаётся доступен через существующий lifecycle status readback.

Парные `host.exit` / `host.failed` теперь используют одну уже существующую закрытую safe-форму:

```text
schema_version
phase
status
occurrence_id
host_epoch
failure_category?
failed_supervisor?
```

Detailed diagnostic codes не являются ScriptRun input и не должны менять safe event bytes.

## Почему это отдельный законченный срез

Current consumer `automation_intake::host_terminal_exit_projection` требует:

- закрытый набор полей;
- exact source/key/epoch;
- одинаковый paired payload;
- одинаковое observed time;
- одну occurrence identity.

Раньше дополнительная диагностика расширяла только `host.exit`, поэтому обе safe-проекции одного реального failure отвергались. Удаление трёх producer-строк восстанавливает существующий consumer contract без нового event store, schema, method или dependency.

## Не входит в этот PR

- два terminal builders всё ещё должны быть сведены к одному private `HostTerminalFact` constructor/codec;
- legacy applied/rejected/unknown runtime aliases всё ещё затеняются generic `module:*` interception;
- historical malformed pairs не переписываются;
- consumer cursors не перематываются;
- никакой внешний/native effect не повторяется автоматически.

Этим остатком владеет [Issue #78](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/78). Он не должен расширять этот узкий producer PR.

## Границы

Allowlist и paired-payload checks не ослаблены. Secondary diagnostics не удалены из retained Exit. `host.failed` не становится вторым независимым failure. Event identity не предоставляет action authority сама по себе.

CloudEvents 1.0.2 использован только как сравнительный принцип `source + id` для distinct event versus redelivery. ELIOT не получает новый wire format, SDK или broker.

## Критерии итоговой тестовой фазы

- Failure с 0/1/2 secondary codes: safe payloads совпадают, retained receipt сохраняет codes.
- Startup/runtime/supervisor failures сохраняют свои category/name fields.
- Graceful exit не стирает latest retained failure.
- Неверный source/epoch, missing/mismatched sibling и лишние safe fields отвергаются.
- Старые malformed pairs остаются historical gaps; replay отсутствует.

## Квалификация поставки

Минимальный gate текущего PR:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

GitHub workflow выполняет changed-line rustfmt и Clippy на Ubuntu/Windows по текущему main. Full tests/native/live остаются итоговой продуктовой фазой по правилам проекта.