# R02. OpenCode: целостное восстановление, transport и чтение результата

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-011, AUD-027, AUD-028, AUD-033.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

OpenCode adapter сохраняет неизвестные эффекты после сбоя, восстанавливает проверяемые записи, завершает многостраничное чтение и удерживает владельца при stop timeout; здоровый IPC link используется без hello на каждом RPC.

## Читать адресно

- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §2–4: HTTP topology, базовые операции, replay policy.
- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.4 и §2.2: live work и retention.
- [modules/opencode/UPDATE.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/modules/opencode/UPDATE.md) — действующий native/adapter contract и recovery ограничения.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-adapter-opencode/src/{journal.rs,lib.rs,native.rs,native_owner.rs}`: `load/load_by_path`, `recover_outbox`, `run_owned`, `HostSession::call/open_link`, `flush_outbox`, `read_assistant_result`, обе shutdown-функции. Внутренний донор: `crates/swarm-adapter-command/src/journal.rs::write_replace_bytes` — только примитив публикации.

## Что и как сделать

1. Объединить journal decoding: полная история, подтверждённый префикс с незавершённым хвостом, повреждение внутри истории. Сохранить спорные bytes и unresolved intent. Повреждённый operation journal изолировать по identity; повреждение общей state identity не трактовать как новую пустую установку.
2. Публиковать отдельные identity/receipt-файлы через private same-directory temporary file, flush и установку final name с нужной no-clobber/directory-sync семантикой. Не копировать весь Command RunStore и не объявлять rename готовым JSONL salvage.
3. Согласовать journal/outbox/ACK: журнал остаётся authoritative, outbox — восстановимый индекс доставки. ACK не должен блокироваться повторными необязательными действиями. Нормальный Unknown→Applied по readback проверить отдельно: прошлый аудит не доказал безусловный конфликт этого сценария.
4. Сделать один владеющий IPC link объект; hello только на новый authenticated link. Точные сохранённые outcome/result разрешено передоставлять, но потерянный module.next требует reconciliation: это admission, не безопасное чтение. Не добавлять retry-everything.
5. Различать EOF, исчерпание scan budget и цикл cursor в read_assistant_result. При shutdown ждать через &mut Child; timeout оставляет StopPending/Unknown, owner identity и handle. Снимать owner только после доказанного выхода/передачи владения.

## Критерии готовности

- [ ] Две и более страницы с final cursor=None дают результат; повторяющийся cursor и реальный лимит дают точную ошибку.
- [ ] Оборванный последний record не разрешает повтор native POST; неизменяемые ACK/outcome можно восстановить без смены identity.
- [ ] Один healthy link обслуживает несколько RPC; reconnect не повторяет неясный module.next.
- [ ] При stop timeout Child/owner остаются отслеживаемыми; чужой/shared service не уничтожается.

## Границы и интеграция

Не мигрировать встроенный runtime/opencode_v2 целиком и не добавлять forms/goal/steer capabilities. Никакого удаления authoritative истории по LRU. Новые библиотеки не обязательны. Native version из UPDATE — доказательная граница, не повод навязать downgrade.

Самостоятельно; R01 отвечает за внешний helper, R05 — за общий dispatch receipt. В swarm-process не создавать второй общий writer: сначала использовать существующую публикацию или оставить узкий adapter-local helper.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-adapter-opencode --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
