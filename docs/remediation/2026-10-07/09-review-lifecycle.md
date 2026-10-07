# R09. Review: управляемая замена аудитора и ограниченное чтение

**Статус: задание на реализацию; текущая поставка содержит только эту спецификацию.**
Основа: аудит редакции 3, 07.10.2026, код `40591a295af94b1541ec2ba30afe8e3247701a71`. Карточки: AUD-026, AUD-029.
Перед работой сравнить актуальный main с этим SHA; уже исправленное не переписывать. Аудит — доказательный материал, не новая owner policy.

## Результат

Manager может адресно заменить ревьюера exact candidate без противоречивого state gate; review.list строит только ограниченную страницу, не всю историю.

## Читать адресно

- [docs/owner-decisions.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-decisions.md) — §1.3: независимое ревью и exact candidate identity.
- [docs/owner-policy-v2.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/owner-policy-v2.md) — полный короткий раздел scoped manager disposition; frozen текст не менять.
- [docs/gm-session-continuity.md](https://github.com/UnknownAlienHuman/eliot-swarm-controller/blob/40591a295af94b1541ec2ba30afe8e3247701a71/docs/gm-session-continuity.md) — историческая identity, текущая authority и поздние результаты.

Нормы работы: `docs/owner-decisions.md` §1.2–1.4, §2.2; текущие project instructions имеют приоритет. Исторический SHA здесь фиксирует источник, не ограничивает используемые версии.

## Участок кода

`crates/swarm-kernel-host/src/store/reviews.rs`: reserve_assign/create_assignment/list_assignments/list/review_view; review-specific helpers `store/coordination.rs::bind_review_assignment/validate_pending_review_tuple`; `src/review.rs` и `store/submissions.rs` — только exact replacement/disposition seam. R05 отдельно владеет candidate origin.

## Что и как сделать

1. Разделить первичную pending registration и явно разрешённую replacement authority. Замена после ReturnForCorrection принимает актуальный needs_correction и те же submission/candidate refs; не сбрасывать Attempt в submitted ради guard.
2. Для не ответившего reviewer добавить узкое явное manager supersede/revoke решение с expected assignment CAS, причиной и current candidate scope. Это расширение recovery из AUD-026, не автоматически существующее право; описать его отдельным контрактом, не изменяя frozen owner-policy-v1/v2.
3. Сохранить прежние assignment/result как историю. Новый reviewer получает только новый exact slot; поздний результат старого не меняет текущий указатель и не принимает Task. Не убивать молчащего native агента.
4. Свести обычный/coalesced assignment result к одной явной форме; не возвращать весь internal record вместо flat response. Проверку role/sponsor/pending slot не удалять: она уже есть в bind_review_assignment.
5. Переписать review.list на стабильный sequence и индексированную bounded выборку; дорогое review_view только для selected page. Если auth фильтруется после SQL — ограничить scan window и вернуть next_scan_cursor+partial. Ожидаемый отказ доступа отдельно от Store/corruption error; не вычислять полный count ценой всей истории.

## Критерии готовности

- [ ] Replacement после ReturnForCorrection проходит при неизменном candidate; сменившийся candidate/assignment вызывает conflict.
- [ ] Явная замена не ответившего reviewer не требует выдуманного review.result; старые evidence сохраняются.
- [ ] Обычное назначение не допускает произвольную роль и сохраняет sponsor/exact-scope проверки.
- [ ] limit=1 не детализирует всю историю; за окном чужих/повреждённых записей следующая страница достижима.

## Границы и интеграция

Не превращать review pass в acceptance, не снимать независимость аудитора. Новая recovery-норма не применяется задним числом к смыслу сохранённых snapshots. Полноценный IAM/новая очередь ревью не нужны.

Самостоятельно после compiler baseline. В coordination.rs менять только review-specific helpers: R06 — общий context/fingerprint, R08 — inbox. R14 переносит frontend shapes после стабилизации этого DTO.

## Проверка и сдача

Один manager и один его worktree; writers получают непересекающиеся участки и не запускают Cargo. Реализацию добавлять в этот же PR, не плодить отдельные PR для DTO/handler/reader. Форматирование только затронутого кода. Минимальный gate менеджера на итоговом кандидате:

```sh
cargo clippy --locked -p swarm-kernel-host --lib --bins -- -D warnings
```

Полные тесты, native/live и нагрузочные прогоны — отдельная итоговая фаза, не выполнять сейчас автоматически. Сценарии выше — критерии поведения, не утверждение о выполненных тестах. В сдаче указать exact SHA, изменённые producer/consumer, результат gate и оставшуюся неопределённость. Draft не переводить в Ready и не сливать как исправление, пока здесь только задание.
