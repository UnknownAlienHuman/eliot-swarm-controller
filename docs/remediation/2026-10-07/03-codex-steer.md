# R03. Codex: exact steer через нативный atomic target guard

**PR #29 · уточнено 7 октября 2026 · код R03 ещё не изменён.** ELIOT main `40591a295af94b1541ec2ba30afe8e3247701a71`, исходный head задания `f2685994514cb31f45a6c15374374facba20fe5b`. Основа AUD-012. Работать в этой ветке, не разделять sender и его readback на разные PR.

## Результат

Коррекция текущего хода доставляется при любой длине завершённой истории. Строго сохраняются binding/native root/configuration guards, caller-owned input identity и native expectedTurnId. Несовпавшая цель не превращается в следующий ход. Transport failure после отправки не превращается в разрешение повторить prompt.

## Документация до кода

- [Module contract](../../agent_swarm.module-contract-v2.md), §1 классы доставки и §4 replay policy.
- [Codex UPDATE](../../../modules/codex/UPDATE.md): Rust descriptor v4 и Python bridge.3 — разные артефакты с разным parity.
- [Owner decisions](../../owner-decisions.md), §1.2–1.4/2.2; [Modularity](../../agent-operations/modularity.md), §3.
- [Официальный app-server](https://learn.chatgpt.com/docs/app-server), разделы turn/steer, initialization и paginated history, прочитан 07.10.2026.
- [TurnSteerParams на `82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b`](https://github.com/openai/codex/blob/82e70121f86bc1f6fea7f2bb7bbc169d259b3c6b/codex-rs/app-server-protocol/schema/typescript/v2/TurnSteerParams.ts): threadId, input, required expectedTurnId, optional clientUserMessageId. Это current source evidence, не установленная версия .159. Перед реализацией выбрать реально поддерживаемую schema из соответствующего artifact, не обновлять server автоматически.

## Реальная цепочка и точка упрощения

`crates/swarm-adapter-codex/src/lib.rs`:

```text
send_operation
  → validate_route / проверка сохранённого root и scope
  → NativeClient::read_thread
  → NativeClient::active_turns [проблемная дополнительная предпосылка]
  → native_send_payload / OperationRecord::intent / Journal::save
  → NativeClient::request("turn/steer", ...)
  → outcome либо reconcile_send по сохранённой identity
```

`active_turns` читает первые 20 thread/turns/list, возвращает active и page_limited. `send_operation` отвергает steer при любом limited. Наличие старых завершённых ходов поэтому ошибочно объявляет текущую цель неактивной.

Native `expectedTurnId` уже проверяется при самом steer. **Не лечить это сканированием всей истории.** Для поддержанного exact-target контракта убрать зависимость write admission от полного active_turns history scan. Сохранить current root/model/provider/workspace preflight; внешний native target guard отвечает за гонку текущего turn. Он не гарантирует атомарную неизменность всех остальных settings.

Если конкретный поддерживаемый native протокол требует дополнительного read, использовать только реально существующую адресную/metadata форму, а не выдуманный `currentTurn` API. History reader можно оставить для другого реального consumer; после удаления единственного caller удалить мёртвый helper, не добавлять dead_code.

## Что реализовать по шагам

1. Проверить непустой expected_turn_id от authenticated RuntimeCommand и exact binding/root; не выбирать «последний turn» вместо caller target. Убрать page_limited как основание EXPECTED_TURN_NOT_ACTIVE в steer-пути.
2. Сформировать payload по выбранному TurnSteerParams. Не передавать turn/start overrides (`model`, `cwd`, `sandboxPolicy`, `outputSchema`) через steer. Если clientUserMessageId поддержан, использовать прежнюю operation-derived identity, записанную до отправки; не менять её при reconnect.
3. Сохранить `OperationRecord::intent`/`Journal::save` до native I/O, dispatch/continuation validators по их реальным условиям. Не переписывать общий receipt validator R05/#31.
4. Сравнить полученный turnId с expected target. Native отказ из-за другой/завершённой цели — точный отказ; потеря ответа либо противоречивый success после возможного эффекта — unknown с сохранённым context. Не фабриковать no-effect из локального mismatch после отправки.
5. ACK steer означает принятую коррекцию в прежний turn. **Новый turn/started для неё не ожидается.** Lifecycle/readback привязывается к текущему turn и input identity; отсутствие нового start не повод посылать ещё раз. Completion/Task acceptance не выводятся из ACK.
6. При replay unresolved Operation использовать существующий reconcile_send с exact clientUserMessageId/target; не вызывать turn/start, новый steer или thread/resume как fallback. Явный новый запрос — другое действие, не скрытое восстановление старого.

## Capability: важный независимый остаток

`NativeClient::attach` сейчас отправляет experimentalApi:false, хотя история читается через методы, которые live documentation помечает experimental. Сверить schema выбранного server: не объявлять доказанным дефектом всякой .159 установки и не просто включить true для всех методов. Сам native turn/steer guard не требует полного experimental history scan.

Непрерывный notification pump, account quota API, answers на approvals, children и native goals — отдельный R15/#41 и будущие control parity блоки. Нынешний receive_response пропускает no-id notifications, но исправление всех возможностей не должно блокировать компактную починку steer. R03 не меняет sandbox/approval политику или модель под видом устранения pagination bug.

## Итоговые сценарии — пока не выполнены

| Сценарий | Результат |
|---|---|
| 21+ завершённых ходов, current target правильный | Один native steer, нет full-history scan на admission. |
| Target сменился после preflight | Native guard отвергает; нет нового turn/start. |
| Нет active turn, caller передал старый ID | Точный отказ; не автоматическое пробуждение новой работы. |
| ACK с тем же turnId, нет нового turn/started | Принятая коррекция не записывается как потерянная из-за отсутствия нового start. |
| Timeout после отправки, затем повтор ELIOT request | Readback прежней identity, ни одного дополнительного input. |
| Success с противоречивым turnId | Unknown/integrity error с evidence; не Rejected-as-no-effect и не replay. |
| Required native method/schema unavailable | Честная capability gap; no silent downgrade к next-turn или иной модели. |

Один manager/worktree, writers без Cargo. После целого кода scoped formatting и:

```sh
cargo clippy --locked -p swarm-adapter-codex --lib --bins -- -D warnings
```

Tests/native — итоговая фаза; таблица не утверждает выполненные проверки. Сдать exact SHA, producer/payload/ACK/reconcile path, удалённый лишний scan, реальный gate и ограничения выбранной native schema. Compiler baseline #26 не копировать, Python executor не удалять. Эта редакция меняет только задание, не SDK/сервер/код продукта.
