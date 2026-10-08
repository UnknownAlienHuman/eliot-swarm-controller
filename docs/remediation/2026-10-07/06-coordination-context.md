# R06. Координация: один проверенный контекст, рабочий code-scope и честная область общения

**PR #32 · уточнено 7 октября 2026 · production-код R06 ещё не изменён.**
Основа: AUD-001/002/008/013/015/021, main `40591a295af94b1541ec2ba30afe8e3247701a71`; исходный head задания `ee20361b9bebd7eac569c697afcddd954d3085b1`. Перед реализацией перечитать текущий diff. Реализацию добавлять в этот PR, не поставлять отдельно неподключённый DTO.

## Цель и первый вход в код

Исправить `code.scope.propose → accept → read` и Participant identity в Concilium. Довести bounded discovery до продвигаемого курсора без маскировки Store errors. Межзадачная связь остаётся отдельным явно обозначенным продуктовым пунктом внутри R06; её неизвестный контракт не блокирует эти доказанные исправления, но не позволяет объявить весь R06 готовым.

Начать в `crates/swarm-kernel-host/src/store/coordination.rs`:

```text
participant_registration / load_current_scope_for_client
  → существующий ScopeData
  → scope_projection / concilium_participant_scope_projection
  → code_scopes::propose / Concilium participant writers и retained readers
```

`ScopeData` уже существует. Новое название `AuthenticatedWorkContext` само ничего не исправляет. Расширить существующий внутренний путь минимальными проверенными полями и подключить потребителей; не создавать второй контекст поверх прежнего.

## Что читать

- [Communication Program](../../agent-communication-program.md), §1–4: текущие владельцы контрактов; общение не назначение и не запуск модели.
- [Peer Autonomy Implementation](../../agent-communication-peer-autonomy-implementation.md), §2–3 и §5–6: stored registration, exact current scope, адресность, hold/refuse и узкие исторические исключения.
- [Fleet-Scale Freedom](../../agent-communication-fleet-scale-freedom.md), §4–5 и §7: sparse current relations, task_revision_set, границы peer-local authority.
- [Tool Contracts](../../agent-communication-tool-contracts.md), §9: proposal/manager amendment/acceptance. Это дополнительная детализация, не замена текущим владельцам из Program.
- [Owner Decisions](../../owner-decisions.md), §1.2–1.4 и §2.2: manager/worktree, этап проверки и сохранение evidence.

## Карта существующих функций

Пути относительно `crates/swarm-kernel-host/src/`.

| Участок | Изменение и сохранённая гарантия |
|---|---|
| `store/coordination.rs::ScopeData`, `load_current_scope_for_client` | Переносить ID клиента из проверенного ключа/Principal; уже загруженные Task/Attempt не читать заново ради соседней проекции. |
| `scope_projection`, `public_registration`, `current_scope` | Публичная форма остаётся явной redacted-проекцией; token_hash и private native fields не выходят наружу. |
| `concilium_participant_scope_projection`, `concilium_current_participant_scope`, `concilium_participant_scope_for_client` | Строить непустой actor и точные scope/basis из того же проверенного контекста. |
| `store/concilium.rs::verify_retained_registration`, `registration_fingerprint` | Тот же проверенный client ID и тот же Concilium fingerprint preimage, что у writer; сохранить отдельную retained-read policy. |
| `store/coordination_threads.rs::registration_fingerprint`, `retained_identity_matches` | Рабочий контрагент для сравнения, не цель глобальной замены preimage. |
| `store/code_scopes.rs::propose`, `accept`, `require_exact_scope_owner` | Все поля proposal identity, проверка текущего автора/срока до override, одинаковый смысл identity при последующем release. |
| `store/coordination.rs::list_participant_page`, `load_current_scope_for_client` | Cursor по просмотренному ключу; предметный stale отдельно от ошибки хранения. |
| `normalize_send`, `send`, `delivery_matches_scope`; `store/integration.rs::sync_integration` | Полная граница межзадачной связи: admission, envelope, адресный индекс и readback должны согласоваться. См. отдельный пункт ниже. |

## 1. Client ID принадлежит ключу записи, не отсутствующему JSON-полю

Нынешний writer сохраняет регистрацию под `client:{client_id}` без поля client_id в value. **Такая форма прямо приведена в §2.1 документа регистрации:** отсутствие поля само по себе не повреждение и не повод мигрировать все credentials.

Передать проверенный ID в внутренний context и fingerprint constructor. Если новая форма value содержит ID, сверять его с ключом, а не предпочитать payload. Убрать `unwrap_or_default()`/null как источник actor identity. Не подставлять ID текущего читателя в произвольную чужую retained-запись.

`manager_scope` намеренно имеет `registration: Null`: менеджер не превращается в Participant. При введении typed grant отразить эту разницу явно; не фабриковать participation_basis для всех ролей. Не заменять historical review/Concilium read на обязательную live Attempt — это другая граница полномочий.

## 2. Исправить весь контракт code-scope, не только путь task_id

`current_scope` возвращает `{scope_id, participant, task, attempt}`. `propose` сейчас ожидает другой набор: `scope`, `actor`, верхнеуровневые `participation_basis` и `registration_fingerprint`. Замены одной строки чтения task_id недостаточно.

Сделать узкий внутренний accessor/projection из уже проверенного context для code-scope/Concilium. Он возвращает exact Task/revision/Attempt, actor, basis, binding pair и fingerprint; публичный `swarm.context.get` не должен ради этого менять wire-форму. Ordinary attempt_owner/producer_ref допускаются, sponsored reviewer не получает implementation scope. Assignment ID сравнивается с действующей basis.

В `accept` проверить proposal revision/digest, текущие Task/Attempt, действующую identity автора и положенный fingerprint. Использовать один `now` всей транзакции. Если передан expiry, требовать допустимое будущее значение **до** изменения override targets; отказ оставляет старый active scope неизменным. Сохранить outer transaction/receipt и возможность явного manager amendment, не вводить несуществующее правило «только подмножество предложения». Overlap/collision алгоритм принадлежит R13/#39.

## 3. Общий extractor не означает один fingerprint для разных контрактов

На исследованном source:

| Семейство | Проверяемые особенности |
|---|---|
| Concilium writer | Берёт отсутствующий client_id как пустую строку; включает disabled, не grant_revision. |
| Concilium retained reader | Берёт то же отсутствие как null; остальная форма должна совпасть с writer. |
| Thread | Получает client_id отдельным аргументом; включает grant_revision; disabled проверяет отдельно от digest. |

Устранить первое расхождение общей Concilium-функцией с явным verified ID. Общие извлечение identity и canonical serialization переиспользовать; разные семантические профили назвать/версионировать, а не молча объединить под одним хэшем. Добавление grant_revision или нового поля в preimage — изменение контракта, не косметический refactor.

Новые writers и их readers подключаются вместе. Старые Thread bytes/digests сохранить. Старые malformed Concilium snapshots с пустым actor не «лечить» догадкой о владельце. Отзыв регистрации продолжает блокировать доступ даже при совпавшем историческом digest. Новый generic криптографический слой не нужен.

## 4. Bounded participant scan с настоящим продвижением

Сейчас `list_participant_page` игнорирует SQL key, использует client_id из value и пропускает запись без него до обновления last_scanned. Полностью stale-окно может стать непроходимым; ошибка БД при загрузке scope маскируется как stale.

Хранить границу **фактически проверенного индексного ключа** отдельно от последнего возвращённого участника. Проверять соответствие suffix ключа и client_id; index value не задаёт произвольный следующий cursor. Для позиции, которую старый after_client_id выразить не может, нужен узкий versioned scan cursor с проверкой scope/формы/размера. Не выдавать сырой meta key как client_id и не принимать cursor за право читать записи.

Не перескакивать первый валидный, но не возвращённый элемент при limit/byte budget. Stale/corrupt derived index допускает явный gap + продвижение; Store/transaction error возвращается ошибкой. `NOT_FOUND` от Task/Attempt можно переводить в предметный stale, но не любой Error. Отдельно отразить scan-bound и stale-count, если оба присутствуют. Обычному Participant не открывать полный roster вместо exact peer discovery.

## 5. Межзадачная связь: не удалить guard и не выдумать готовый grant

Fleet document предполагает отношения между актуальными назначениями и task_revision_set. Но нынешний `sync_integration` требует один scope_id/Task/revision/Attempt у всех участников. **Текущая cell не является уже готовым разрешением A → B.** Совпадение project или contract_key само по себе не доказывает все необходимые права.

Полная цепочка сейчас однообластная: normalize_send запрещает другой scope; send индексирует по sender.scope_id; envelope/result содержат только sender tuple; delivery_matches_scope требует ту же tuple у recipient. Простое снятие первого запрета лишь создаст сообщение, которое recipient не сможет прочесть.

Для расширения сначала закрепить в owning fleet/peer contract один конкретный способ определения допустимой связи, затем реализовать его reader и доставку вместе. Минимальная спецификация связи должна определить: две exact current Task/revision/Attempt и actor/basis; источник relation и её revision/digest; кто вправе её создать/изменить; разрешённые направления и видимые поля; результат смены назначения/отзыва. Это **новый подконтракт**, не существующий API с угаданным именем. Не вводить отдельное согласование менеджером каждого обычного сообщения и не строить общий ACL-сервис.

Новая доставка должна сохранять source_context и target_context отдельно, адресовать индекс получателя и проверять обе стороны при admission. Чтение не даёт artifact/agent.send/assignment authority. Старый v1 envelope остаётся явно однообластным; новый формат и его readers согласовать, не угадывая недостающую tuple из текущей Task. Hold/refuse и отсутствие idle model wake сохраняются.

Сейчас конкретный существующий межзадачный grant данным проходом не подтверждён. Не маскировать этот остаток общим `.is_ok()` или считать R06 полностью завершённым после исправления context. Доказанные пункты 1–4 и их consumers R07/R13 не должны ждать проектирования всего relevance graph.

## Сценарии итогового кандидата — ещё не исполнены

| Вход | Результат |
|---|---|
| Документированная регистрация без client_id в value | Непустой проверенный actor из ключа; source/retained fingerprints согласованы. |
| Payload ID не совпал с ключом, disabled либо чужая basis | Отказ, без присвоения identity читателя. |
| Ordinary Participant propose → manager accept → read/release | Один и тот же exact context; reviewer implementation scope не получает. |
| Expired replacement / stale proposer / revision conflict | Нет уничтожения прежнего active scope; явный отказ. |
| Полное окно stale indexes, затем валидная запись | Продвигаемый scope-bound scan cursor; валидная запись достижима. |
| Ошибка Store вместо отсутствующей Task | Ошибка не превращается в пустой roster/ложный stale. |
| Межзадачный сценарий после реализации нового подконтракта | Разрешённая пара получает сообщение в target inbox; чужая/отозванная связь не получает доступа, старый v1 не расширяется. |

## Доноры, границы и сдача

Первый внутренний образец — `integration::sync_integration`: он уже правильно читает вложенные task/attempt и не переписывает authority. Использовать при согласовании форм, но не переносить его exact-Attempt ограничение как универсальную модель сотрудничества. Thread fingerprint — образец явного ID от caller, не байтово совместимая замена Concilium. [CCCC inbox](https://github.com/ChesterRa/cccc/blob/1e67dc8700515acbb2cc6a56c2e546b850f3c559/crates/cccc-core/src/inbox.rs) полезен разделением позиции события и actor generation; файловый ledger и consume_unread в read-only discovery не нужны.

R06 владеет context, identity, scope accept и семантикой нового envelope/его authority predicate. R08/#34 — sequence/index traversal, pagination и subscriptions; общую функцию не правят два владельца. Новые source/target поля согласовать до интеграции, без двух DTO и круговой зависимости. R09 — review-specific guards, R13 — collision/resource predicates. Не форматировать соседние большие файлы целиком.

Один manager/worktree, writers без Cargo. После законченного кода менеджер выполняет scoped formatting и:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Широкие tests/native/load — итоговая фаза. Сдача в этом же PR: exact SHA, подключённые consumers, удалённые дубли, фактический gate и явный остаток межзадачного контракта. Эта редакция меняет только задание; будущие сценарии и green docs CI не квалифицируют Rust-код.
