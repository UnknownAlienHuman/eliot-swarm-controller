# R04. Muse: вопросы и approvals не теряются при refresh

**Статус: bridge.8 реализован и прошёл syntax/документационный gate. Поведенческие, native и live-сценарии остаются итоговой продуктовой фазой.**

Основа исследования: `main` `40591a295af94b1541ec2ba30afe8e3247701a71`, AUD-025. Исторические SDK/SHA ниже — координаты доказательства, не требования закрепить или понизить внешний runtime.

## Результат

Root/child attention сохраняет новый вопрос и не воскрешает уже разрешённый при гонке server request/notification с `approval/listPending`.

Одна pending identity теперь имеет форму:

```text
(sessionId, kind, nativeRequestId)
```

Native request ID без session scope больше не может смешать root и child. Частичный `approval/updated` без сохранённого original request не становится actionable question с выдуманными tool/request fields; bridge сохраняет bounded gap.

## Участок кода

`modules/muse/bridge.mjs`:

- `sessionChanged`;
- `pendingRequestKey`;
- `recordPendingRequestGap`;
- `replacePendingInventory`;
- `onNotification`;
- `onServerRequest`;
- `refreshRoot` / `refreshChild`.

`modules/muse/module.example.json`, route examples, README/UPDATE и selftest используют artifact generation `muse-sdk-1.3.0-bridge.8`. Это versioned ELIOT bridge identity; она не требует менять установленный пользователем Muse runtime.

## Реализованная семантика

1. Server requests и notifications увеличивают одну per-session generation, которую реально проверяют refresh-функции.
2. Полный inventory сначала целиком проверяется по session, ID и duplicate identity; только затем синхронно заменяет записи одной session при неизменной generation.
3. Malformed последняя запись не оставляет частично удалённый inventory.
4. Creation, update и terminal removal используют один exact composite key для root и child.
5. `approval/updated` объединяет stage fields только с exact retained original request той же session.
6. Отсутствующий optional `subagentOrigin` в новом stage удаляется, а не наследуется от предыдущего stage.
7. Orphan update публикует bounded `PENDING_APPROVAL_UPDATE_WITHOUT_REQUEST` gap; model call, auto-answer и reply не выполняются.
8. `RequestReceipt {}` остаётся подтверждением представления server request, а не решением approval.
9. `pending_inventory_applied` отделён от `metadata_applied`.

## Критерии итоговой тестовой фазы

- Вопрос, пришедший между началом `listPending` и старым ответом, остаётся в attention.
- Разрешённый в том же окне вопрос не возвращается из старого inventory.
- Root и child с одинаковым native request ID остаются двумя независимыми вопросами.
- Resolution одной session не удаляет вопрос другой session.
- Malformed/duplicate/foreign-session inventory ничего не стирает частично.
- Orphan `approval/updated` остаётся gap, не actionable request.
- Multi-stage approval сохраняет original identity и не наследует удалённый optional origin.
- Наблюдение не вызывает approval decision, reply или новый model turn.

## Границы

Нет нового poller, persistence layer, SDK router, model loop или автоматической policy. `pendingRequests` остаётся локальной bounded observation projection; Store authority, exact reply admission и native session ownership не меняются.

Поведение внешнего Muse runtime не выводится из старого SDK номера. Live Muse/Max, resume и Windows process ownership квалифицируются отдельно через текущую установленную и авторизованную подписочную поверхность.

## Квалификация поставки

Exact head `9dd2c72a1b98229eb46b1c6c273dca4368ff5a8b`, workflow run `37882410590`:

- changed-path classification — passed;
- JavaScript syntax for changed module files — passed;
- Markdown/diff validation — passed;
- Rust/rustfmt/Clippy jobs correctly skipped because Rust files were not changed;
- full product/native tests intentionally deferred to the final phase.

До этого exact patch workflow также выполнил:

```text
node --check modules/muse/bridge.mjs
node --check modules/muse/selftest.mjs
JSON parse modules/muse/module.example.json
git diff --check
```

Selftest execution, SDK import, real MSP traffic and model calls не выполнялись и этим gate не заявляются.