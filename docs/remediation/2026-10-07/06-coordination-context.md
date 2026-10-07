# R06. Координация: единый work context и законченный code-scope путь

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-001, AUD-002, AUD-008, AUD-013, AUD-015, AUD-021.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Актуальный Participant проходит propose/accept/read; identity и fingerprint одинаковы у producers/consumers. Разрешённое сотрудничество двух заданий выражается отдельно от прав исполнения, без Root-ретранслятора.

## Читать адресно

- [docs/agent-communication-program.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-program.md) — цели самостоятельной адресной коммуникации и границы authority.
- [docs/agent-communication-tool-contracts.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/agent-communication-tool-contracts.md) — §6.3, §8, §9: admission, proposal и code-scope.
- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.2–1.4: manager/worktree, identity, read-only.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/coordination.rs`: current scope loaders/projectors, registration/fingerprint, `list_participant_page`, `normalize_send` и targeted discovery; `store/code_scopes.rs`: `propose`, `accept`; `src/coordination/mod.rs` и существующие integration relation readers. Concilium меняется здесь только на consumption общего identity, не на lifecycle.

## Что и как сделать

1. Ввести либо использовать один внутренний AuthenticatedWorkContext из проверенного Principal/registration key + Task/Attempt. Убрать чтение отсутствующего authority.scope и расхождение client_id=""/null. Публичные DTO строить явно; не дописывать пустые identity defaults.
2. Определить один fingerprint preimage для данного вида grant и обновить его действующих consumers. Исторический другой формат читать только с явной версией, не угадывая alias. Изменение fingerprint само по себе не разрешает новые права.
3. До override любых scope intents проверять допустимость expiry и актуальность exact proposal. На отказе не менять прежний active scope. Manager amendment сохранить как явную recorded revision, как требует §9.2, а не запретить расширение без основания.
4. В participant paging продвигать scan cursor по реально просмотренной записи даже при фильтрации; возвращать gaps. Только NOT_FOUND/ожидаемая stale-ситуация становятся STALE_PARTICIPANT; ошибки БД сохраняются.
5. Для межзадачного общения проверить, выражает ли существующая project/integration relation оба задания; использовать её, если да. Иначе добавить минимальную явную relation в той же БД и её версионированный контракт в этом PR. Source и target execution contexts проверять отдельно, учитывать inbound policy; одно совпадение project не даёт общего доступа. Это продуктовое расширение AUD-021. Ограничить одним законченным видом разрешённой связи; не строить новый ACL сервис.

## Критерии готовности

- [ ] Participant code.scope.propose → manager accept → read использует непротиворечивую identity; expired replacement не уничтожает прежний scope.
- [ ] Concilium identity projector и retained-reader согласованы для того же registration; lifecycle закрывает R07.
- [ ] Повреждённая индексная запись не делает следующую страницу недостижимой; Store error не маскируется как stale.
- [ ] Producer Task A и consumer Task B по явной разрешённой relation обмениваются сообщением; другой project/неактуальная Attempt отвергаются, никаких agent.send/чужой записи не разрешается.

## Границы и интеграция

Здесь владелец общего work context и registration fingerprint. R08 владеет mailbox ordering/watch/subscription, R09 — review-specific pending gate, R13 — collision/resource semantics. Не переписывать весь coordination.rs и не форматировать чужие участки.

R07 и R13 используют итоговый work context этого блока: их интеграция после R06. R08/R09 могут готовить свои изменения независимо, но общий файл сливать по символам, без конкурирующих DTO.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
