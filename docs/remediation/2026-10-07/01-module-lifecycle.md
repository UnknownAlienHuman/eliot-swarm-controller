# R01. Модули: identity, restart, hello и независимая установка

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-005, AUD-037, AUD-038, AUD-041.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Установленный модуль переживает очистку build-cache; супервизор различает прежнего и нового владельца, подтверждает тот же worker boot и не теряет готовность после очередного опроса.

## Читать адресно

- [docs/agent-operations/modularity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-operations/modularity.md) — §2–3: process/package boundaries, handshake; разделы recovery и optional workers.
- [docs/agent_swarm.module-contract-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent_swarm.module-contract-v2.md) — §2–4: владение runtime и неизвестный исход.
- [tools/modules/README.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/tools/modules/README.md) — descriptor-last publication, install receipt и Handoff and limits.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-supervisor/src/{supervisor.rs,descriptor.rs,standalone.rs}`: `identity_from_process_image`, `process_identity_is_live`, `wait_for_prior_owner_to_depart`, `clear_prior_helper_result`, `monitor_owner_helper`, `confirm_module_hello`, `update_status`, `load_installed_descriptor`. Контрагенты: `crates/swarm-process/src/{process_group.rs,module_owner.rs}`. Не переписывать весь supervisor.

## Что и как сделать

1. Согласовать birth identity с producer swarm-process: сохранять platform/PID/birth fields из единственного constructor, а не вручную собирать несовместимый JSON. Сохранить отдельные проверки image и принадлежности process family.
2. Провести restart как один переход: доказанный departed prior owner → запуск helper → публикация нового owner → worker receipt → hello. Пока helper ещё не опубликовал новый receipt, принимать только точное совпадение с уже проверенным прежним receipt как состояние ожидания. Неизвестное несовпадение остаётся ошибкой; ожидание ограничено технической фазой startup, с явным unknown и сохранённым handle/receipt.
3. Не удалять owner.json вслепую: helper требует его для checkpoint recovery. Согласовать обе стороны публикации/чтения в одном PR, сохранив старое доказательство до безопасного перехода.
4. Менять status атомарно на текущем значении; использовать имеющийся Tokio watch::Sender::send_if_modified. Live-poll того же boot не понижает ProcessRunning до Starting; поздний hello другого boot не восстанавливает завершённого worker. Независимые поля не затираются snapshot-clone overwrite.
5. Оставить source_file/source_sha256 историей установки. При загрузке проверять installed EXE, descriptor/receipt и install-root; повторное существование старого build-output не требовать. Не ослаблять хэш установленного executable. Удалить только ставшие ненужными реконструкции и source-runtime check.

## Критерии готовности

- [ ] Windows identity проходит round-trip producer → supervisor; чужие PID/birth/image/family отвергаются.
- [ ] Проверенный старый owner до публикации нового не вызывает ложную подмену; неизвестный owner не принимается.
- [ ] После hello несколько live-polls сохраняют ProcessRunning; параллельная правка другого поля status не теряется.
- [ ] После удаления/пересборки source EXE установленная версия загружается; изменение installed EXE отклоняется.

## Границы и интеграция

Никаких restart/kill по молчанию модели, удаления чужого сервиса или автоматического обновления модулей. Изменения install semantics описать в README инструмента, не переписывая frozen owner policy. Tokio уже используется; новый actor framework не нужен.

Самостоятельный блок. R02 владеет Child внутри OpenCode adapter; здесь только общий supervisor/helper. С R14 не смешивать переносы пакетов. #26 — существующий compiler baseline, не часть этого PR.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-supervisor -p swarm-process --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
