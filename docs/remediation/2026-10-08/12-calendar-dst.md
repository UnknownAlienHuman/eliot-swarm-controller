# R12 companion. Calendar DST: сохранять абсолютный instant до Croner

**Статус:** implementation handoff. Production-код этим документом не изменён.

**База доказательства:** ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`; current selected `croner` 4.0.1 с `chrono` backend. Номера версий и SHA ниже — координаты source review, не launch allowlist и не требование удерживать старый release.

## 1. Подтверждённая причина

`crates/swarm-kernel-host/src/scheduler/calendar.rs::local_second_at` сегодня делает:

```rust
let utc = Utc.timestamp_millis_opt(epoch_ms).single()?;
utc.with_timezone(&parsed.timezone)
    .with_nanosecond(0)
    .ok_or_else(...)
```

UTC timestamp однозначен. После `with_timezone` instant всё ещё однозначен и несёт конкретный offset первой либо второй стороны DST fold. Но `DateTime<Tz>::with_nanosecond` в Chrono реализован через локальное remapping:

```rust
f(dt.overflowing_naive_local())
    .and_then(|datetime| dt.timezone().from_local_datetime(&datetime).single())
```

В повторном осеннем часу тот же wall-clock существует с двумя offsets. `.single()` возвращает `None`, хотя исходный UTC instant был точным. Поэтому вызовы `latest_due`, `next_due_at_ms` и `preview_next_occurrences`, начавшиеся **внутри** fold hour, могут завершиться `calendar timestamp could not be rounded to a second` до вызова Croner.

Текущий тест `fixed_local_time_in_fall_overlap_occurs_only_once` начинает поиск до overlap. Он проверяет выбранную fixed-time semantics, но не вызывает `local_second_at` для instant, уже находящегося в неоднозначном часу. Поэтому defect остаётся непокрытым.

## 2. Нормативная семантика уже есть

`docs/schedules.md` определяет:

- fixed local time в spring gap выполняется в первый valid instant после gap;
- fixed local time в fall overlap выполняется только в первый matching instant;
- wildcard/interval expressions следуют Croner для каждого real instant;
- occurrence identity строится по intended UTC instant.

Исправление не меняет эти правила. Оно удаляет собственную ELIOT-предобработку, которая уничтожает offset/instant identity до maintained evaluator.

## 3. Минимальное изменение

Округлять миллисекунды **на UTC timeline до timezone conversion**.

Предпочтительная форма без повторного local mapping:

```rust
fn second_at(parsed: &ParsedCalendar, epoch_ms: i64) -> Result<DateTime<Tz>> {
    let second_ms = epoch_ms
        .div_euclid(1_000)
        .checked_mul(1_000)
        .ok_or_else(|| Error::invalid(
            "calendar timestamp is outside the supported date range"
        ))?;
    let utc = Utc
        .timestamp_millis_opt(second_ms)
        .single()
        .ok_or_else(|| Error::invalid(
            "calendar timestamp is outside the supported date range"
        ))?;
    Ok(utc.with_timezone(&parsed.timezone))
}
```

Точное имя helper может остаться `local_second_at`, но комментарий обязан сказать: floor выполняется на absolute UTC timeline, затем сохраняется конкретный fold offset.

Почему `div_euclid`, а не `/`: public parser сейчас требует nonnegative anchor, но preview/utility boundaries не должны получить неверное округление для отрицательного Unix timestamp при future reuse. Checked multiplication сохраняет typed out-of-range failure.

Не использовать:

- local naive datetime + `from_local_datetime(...).earliest()/latest()`;
- ручной выбор DST offset;
- второй cron parser;
- timezone-specific if/else;
- upgrade/downgrade зависимости как замену исправлению call boundary.

Croner уже владеет fixed/wildcard DST iteration. ELIOT должен передать ему точный instant.

## 4. Затронутые callers

Один helper используется:

```text
latest_due
first_occurrence_at_or_after
next_due_at_ms
preview_next_occurrences
```

Не создавать отдельные fixes для preview и scheduler. После изменения все четыре пути используют одну UTC-floor boundary.

`generation_digest`, `occurrence_id`, Store cursor/receipt и latest-only coalescing не меняются. Occurrence ID остаётся UTC `due_at_ms`; никакой миграции persisted occurrences не требуется.

## 5. Точные проверки

Добавить tests в `scheduler/calendar.rs` через public functions модуля, а не test-only clone helper.

### 5.1 America/New_York fold

Для 2024-11-03:

1. `latest_due("* * * * * *")` во время первой 01:30 и второй 01:30 возвращает occurrence либо `None` по cursor, но не `CRON_EVALUATION`/INVALID.
2. `next_due_at_ms("* * * * * *")` из первой и второй fold-side продвигается к следующему real second.
3. `preview_next_occurrences` внутри fold возвращает строго возрастающие UTC instants без duplicate/non-advancing item.
4. fixed `0 30 1 * * *` остаётся one-shot на первом matching instant согласно текущему contract.

### 5.2 Europe/Berlin fold

Повторить boundary для 2025-10-26 02:xx. Это защищает алгоритм, а не один американский offset.

### 5.3 Subsecond boundary

В обеих сторонах fold подать `...123ms` и подтвердить floor к exact UTC second, не к ambiguous local reconstruction.

### 5.4 Spring gap regression

Существующий `fixed_local_time_in_spring_gap_uses_first_valid_time_after_gap` остаётся зелёным.

### 5.5 Cursor/coalescing

Для wildcard schedule получить occurrence первой fold-side, передать её как `last_considered_ms`, затем подтвердить поведение второй real instant по documented Croner semantics. Тест должен закрепить фактический selected evaluator contract, а не предполагать его по названию.

## 6. Проверка до Store integration

Минимальный package gate после кода:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Поведенческий набор на итоговой фазе:

```sh
cargo test --locked -p swarm-kernel-host scheduler::calendar
```

Команды здесь — будущие gates, не отчёт об уже выполненном прогоне.

## 7. Ownership и упрощение

Этот companion принадлежит R12/#38, потому что календарь является одним из due sources shared scheduler. Он не меняет R34 poison-fact classifier: после исправления valid fold instant больше не превращается в source error.

Удаляется только ошибочная post-timezone `with_nanosecond(0)` boundary и устаревшая строка R12 «DST не чинить без доказательства». Новая abstraction, dependency, table, service или compatibility path не добавляется.

## 8. Источники

- ELIOT calendar source: `crates/swarm-kernel-host/src/scheduler/calendar.rs` на `40591a2`.
- ELIOT calendar contract: `docs/schedules.md` на `40591a2`.
- Chrono `DateTime<Tz>::with_nanosecond` / `map_local`: `chronotope/chrono@6adaa5240c26fecb7bd9077334a91f8f67f4f3fe`.
- Croner 4.0.1 selected in current lockfile; upstream 4.0 release notes include DST-overlap iteration fixes. Это подтверждает границу ответственности evaluator, а не предписание версии.
