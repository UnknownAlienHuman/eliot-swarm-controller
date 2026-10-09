# R12 remainder. Scheduler progress, source isolation and issuance fairness

**Статус: не входит в PR #38. Точный оставшийся implementation owner — Issue #79.**

Основа исследования: `main` `40591a295af94b1541ec2ba30afe8e3247701a71`, AUD-030/AUD-032. Исторические SHA и версии — координаты доказательства, не требования закрепить runtime.

## Разделение ответственности

PR #38 исправляет только доказанную calendar input boundary:

```text
absolute UTC milliseconds
→ floor to whole UTC second
→ timezone conversion preserving the exact instant/fold offset
→ Croner
```

Остальная scheduler-работа не должна удерживать этот независимый fix в Draft.

[Issue #79](https://github.com/UnknownAlienHuman/eliot-swarm-controller/issues/79) владеет:

- независимыми bounded outcomes для due sources;
- no-progress backoff для read/admit/stale paths;
- authoritative readback перед повтором uncertain admission;
- launcher issuance cursor/fairness;
- Store/worker-local transient pacing вместо process-global state;
- сохранением primary error при отказе secondary diagnostics.

R34/#60 отдельно владеет poison-fact isolation внутри automation domains и per-domain transactions. Adapter IPC/reconnect остаётся R02.

## Нормативные ограничения

- SQLite/connection/commit uncertainty остаётся hard failure.
- Unknown external effect не повторяется по timeout или reconnect.
- Не добавляются второй scheduler, broker, workflow engine, model-task deadline или произвольный лимит попыток.
- Конечная успешная очередь старого issuance algorithm не объявляется доказанной вечной потерей; исправляется fairness/state ownership, а не выдуманный failure mode.

## Итоговая проверка Issue #79

```text
independent malformed source does not starve unrelated due work
repeated no-progress receives bounded pacing
real durable progress resets pacing
tail remains reachable under arrivals and repeated failures
two Stores do not share cursor/backoff state
unknown issuance is readback-only
```

Минимальный gate после реализации Issue #79:

```sh
cargo clippy --locked -p swarm-kernel-host -p swarm-automation --lib --bins -- -D warnings
```

Tests, native/live and load qualification remain the final product phase under the project rule.