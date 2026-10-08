# R08 companion. Mailbox identity: один digest producer и fail-closed delivery lookup

**Статус:** implementation handoff. Production-код этим документом не изменён.

**База:** ELIOT `40591a295af94b1541ec2ba30afe8e3247701a71`.

## 1. Подтверждённые расхождения

### 1.1 `find_delivery` выбирает произвольную строку

`store/mailbox.rs::find_delivery` выполняет:

```sql
SELECT operation_id
FROM operations
WHERE method IN (...)
  AND state='settled'
  AND json_extract(result_json,'$.delivery_id')=?1
```

через `query_row` без `ORDER BY`, `LIMIT 2` или уникального Store invariant. Если retained Store содержит две settled mailbox Operations с одним `delivery_id`, cancel/reply проверяются против произвольно выбранной строки.

Current producer генерирует случайный ID и только проверяет, что он не равен operation/message ID. `_tx` в `admit_delivery_core` не используется; collision/duplicate не проверяется. Вероятность случайной UUID collision мала, но security identity не должна зависеть от предположения «этого почти не будет». Legacy/repair/imported damage также обязан fail closed.

### 1.2 Typed mailbox доверяет caller-supplied digest

`DeliveryRequest` принимает одновременно:

```text
payload
payload_digest
```

`admit_delivery` проверяет только hex shape. Current Thread producer действительно вычисляет `sha256(canonical(payload))`, но mailbox boundary это не доказывает. Новый internal caller способен передать произвольный well-formed digest, который затем используется reply/cancel/readback как binding fact.

Это класс «producer честен, consumer не проверяет», уже выделенный общим аудитом.

## 2. Минимальная реализация без новой таблицы

### 2.1 Mailbox сам вычисляет typed digest

Удалить `payload_digest` из `DeliveryRequest`.

В `admit_delivery` после payload shape/actor checks:

```rust
let payload_digest = format!(
    "sha256:{}",
    model::digest(model::canonical(&request.payload)?.as_bytes())
);
```

Именно это значение передаётся в `DeliveryBody::Typed` и retained result.

Удалить:

- caller-side digest construction в `coordination_threads::apply_send`;
- `validate_sha256_digest` как typed-admission boundary;
- возможность bare/uppercase digest у нового typed producer.

Legacy `message.send` сохраняет свой исторический `message_payload_digest` contract. Не объединять два preimage без explicit version migration.

### 2.2 Выделить bounded ID allocator

Заменить `new_delivery_id(operation_id, message_id)` на:

```rust
fn allocate_delivery_id(
    tx: &Transaction<'_>,
    operation_id: &str,
    message_id: &str,
) -> Result<String>;
```

Алгоритм:

1. сгенерировать ID текущим `model::new_id()`;
2. reject equality с operation/message ID;
3. exact existence query по четырём mailbox methods и settled delivery results;
4. bounded number попыток, например 8;
5. невозможность выбрать свободный ID → `MAILBOX_DELIVERY_ID_EXHAUSTED` до settlement.

Store имеет одного writer, поэтому lookup и текущая Operation settlement находятся в одной serialized transaction. Новый global allocator/sequence/service не нужен.

### 2.3 Lookup читает максимум две строки

Один private helper:

```rust
fn delivery_operation_ids(
    db: &Connection,
    delivery_id: &str,
) -> Result<Vec<String>>;
```

SQL использует deterministic `ORDER BY operation_id LIMIT 2`.

Результат:

```text
0 → None
1 → exact Operation
2 → MAILBOX_DELIVERY_AMBIGUOUS
```

Не выбирать newest/oldest и не лечить damage удалением одной строки. Cancellation/reply не выполняются при ambiguity.

На первом slice не добавлять новую delivery table или custom index protocol. Если explain/benchmark на реальном объёме покажет дорогой scan, отдельный partial expression index допустим после duplicate preflight; correctness helper остаётся тем же.

## 3. Direction и method contract

`find_delivery` используется:

- legacy reply;
- typed Thread reply;
- cancellation;
- Thread reply validation.

Все callers получают одну fail-closed identity.

Typed reply дополнительно должен проверять direction в mailbox boundary либо один доказанный Thread caller должен сделать это до вызова. Предпочтительно закрыть внутри `validate_typed_reply`:

```text
prior sender == current recipient
prior recipient == current sender
```

Thread membership не заменяет direction swap. Legacy path уже имеет аналогичную проверку.

Не расширять methods beyond actual delivery-bearing methods. Если новый method начнёт создавать mailbox delivery, его добавление должно быть одним change в closed helper/test, а не случайным SQL literal в нескольких functions.

## 4. Error ordering

В cancel path сначала проверить current sender registration/authority, затем digest claim. Dergistered caller не должен получать oracle «digest верный/неверный» до authorization.

Порядок:

```text
find exact unambiguous delivery
→ authenticate current sender/ownership
→ verify digest claim
→ check existing cancellation
→ create immutable cancellation receipt
```

`reason` должен проходить existing bounded text/closed request contract; arbitrary object/array не сохранять в immutable receipt. Если public parser уже гарантирует type/size, mailbox test фиксирует эту гарантию; иначе добавить local bounded optional text parser.

## 5. Tests через public Store paths

Не ограничиваться direct helper tests.

### 5.1 Typed digest

- `coordination.thread.send` сохраняет digest exact canonical payload;
- mutation test: caller-side fake digest field невозможен/игнорируется, потому что поля больше нет;
- non-ASCII payload даёт одинаковый digest в receipt/readback/reply claim.

### 5.2 Duplicate delivery

В isolated fixture без future uniqueness index создать две legitimate-shaped settled delivery Operations с одним ID:

- `message.cancel` → `MAILBOX_DELIVERY_AMBIGUOUS`;
- legacy reply → same error;
- typed reply → same error;
- никакая новая Operation не settled как effect.

### 5.3 Allocation collision

Inject deterministic ID generator/test hook либо private allocator fixture:

- first generated ID already retained;
- allocator chooses second ID;
- bounded repeated collision returns typed error before result settlement.

Не менять production RNG global state ради теста; допустим private generator parameter under `cfg(test)` или pure helper receiving candidate iterator.

### 5.4 Direction

A→B delivery:

- B→A reply accepted;
- A→B claiming same delivery rejected;
- C→A/B rejected;
- same thread membership alone does not bypass direction.

### 5.5 Authorization/error precedence

Disabled/deregistered sender with correct и wrong digest получает один authorization error; digest validity не раскрывается первым.

## 6. Integration с основным R08

Sequence/cursor index R08 адресует **положение доставки**, а этот companion — immutable delivery identity. Не использовать sequence как delivery ID и не использовать random delivery ID как cursor.

```text
delivery_id = object identity
observation/mailbox sequence = ordering position
operation_id = mutation receipt identity
message_id = Thread/legacy message identity
```

Эти четыре значения не взаимозаменяемы.

R23/#49 определяет object visibility Operation; он не исправляет ambiguous delivery lookup. R24/#50 method gate не заменяет sender/object authorization.

## 7. Что удаляется

После кода:

- `DeliveryRequest.payload_digest`;
- caller-side canonical digest copy;
- `validate_sha256_digest` в typed path;
- random ID function без Store check;
- arbitrary `query_row` lookup;
- duplicated direction assumption в caller comments.

Ожидается небольшой отрицательный либо почти нейтральный production diff.

## 8. Gate

После связанного кода:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Mailbox/coordination public-path tests — итоговая фаза вместе с остальным R08. Этот документ не утверждает, что они уже выполнены.
