# Аудит v2.3.3-stage1 — дефект «повторное открытие позиции при активной частичной SELL»

Дата: 2026-11-21. База: tsclp_v2.3.2_stage1 (проверено: файлы идентичны
опубликованному репозиторию https://github.com/cristal-hub/tsclp_v2.3.2_stage1).
Режим: ТОЛЬКО SANDBOX. Стратегия/входы/индикаторы/SL-TP/риск-модель: UNCHANGED.

---

## 1. Дефект (воспроизведён и подтверждён)

Последовательность на коде v2.3.2:

1. Локальная позиция BUY: 10 лотов (state=OPEN).
2. SL/TP срабатывает, робот отправляет SELL на 10 лотов
   (`close_position` -> pos.state=CLOSING, close_order_id активен).
3. Брокер сообщает PARTIALLYFILL, исполнено 3 лота.
4. `apply_close_execution()` уменьшает локальное количество до 7
   (per-order cursor: 0 -> 3) и переводит позицию в UNCERTAIN
   (статус PARTIALLYFILL).
5. `process_closing()` получает достоверный снимок брокера с остатком
   7 лотов.
6. `resolve_uncertain()` видит совпадение `broker.lots == pos.quantity`
   (7 == 7) и переводит позицию в **OPEN**.
7. При этом старая SELL-заявка ещё не завершена (PARTIALLYFILL), а
   `close_order_id` остаётся активным.

Результат: робот снова включает SL/TP (`check_all` действует на OPEN) и
может отправить ПОВТОРНУЮ SELL по позиции с незавершённой заявкой —
перепродажа (каждый дополнительный частичный филл живой заявки
применится cursor'ом, а «лишняя» SELL уйдёт в минус/отказ).

Дефект подтверждён прогоном новых тестов на коде v2.3.2:
`tests/test_v233_partial_sell_reopen_defect.py` — 5 failed, 2 passed
(см. РЕЗУЛЬТАТЫ_ТЕСТОВ_v2.3.3.txt, раздел 2).

## 2. Главный принцип исправления

**Совпадение локального остатка с брокерским количеством НЕ является
основанием переводить позицию в OPEN, пока собственная SELL-заявка
закрытия имеет неподтверждённую терминальность.**

Ключевой инвариант (введён v2.3.3):

> `pos.close_order_id` сбрасывается ТОЛЬКО при обработке терминального
> отчёта заявки (`apply_close_execution` / `resolve_closing`).
> Следовательно: `close_order_id is not None`  ⟺  «терминальность
> собственной SELL не подтверждена» ⟹ OPEN запрещён, повторная SELL
> запрещена, SL/TP не действуют, новые BUY по FIGI заблокированы
> (UNCERTAIN входит в ACTIVE_STATES, `has_active_position` = True).

Наличие `close_order_id` само по себе не доказывает, что заявка активна
на бирже, но пока её терминальность не подтверждена отчётом —
безопасное состояние (UNCERTAIN) не покидается.

## 3. Проверены ВСЕ переходы состояний в OPEN (не только resolve_uncertain)

Все точки присвоения `PositionState.OPEN` в проекте:

| # | Место | Статус | Действие v2.3.3 |
|---|-------|--------|-----------------|
| 1 | `execution.py::promote_recovery` | RECOVERY -> OPEN | Без изменений: RECOVERY-позиция не имеет close_order_id (register_recovery не устанавливает), перезапись активной записи невозможна (guard has_active_position). |
| 2 | `execution.py::apply_close_execution` (ветка terminal) | терминальный отчёт с остатком (quantity>0) | **ИСПРАВЛЕНО**: немедленный OPEN заменён на UNCERTAIN + очистку завершённой заявки; остаток разрешается только через брокерский снимок (п. 4.2). Полное исполнение (quantity==0) по-прежнему CLOSED немедленно. |
| 3 | `execution.py::resolve_closing` (REJECTED/CANCELLED, lots==0) | терминальный отказ без исполнения | **ИСПРАВЛЕНО**: немедленный OPEN заменён на UNCERTAIN + очистку заявки; ветвь не выполняется при противоречивом отчёте (`applied_ok=False`). |
| 4 | `execution.py::resolve_uncertain` (совпадение количества) | главный путь дефекта | **ИСПРАВЛЕНО** (ядро): при активном close_order_id позиция остаётся UNCERTAIN (п. 4.1). |

Дополнительно в `main.py::process_closing` добавлена защита в глубину:
после `resolve_uncertain` позиция с `close_order_id` в состоянии OPEN
немедленно возвращается в UNCERTAIN (страхование от регрессий будущих
путей разрешения; SL/TP действуют только на OPEN — повторная SELL
исключена на любом пути).

Другие пути, проверенные и признанные безопасными:

- `sync_recovery_quantity`, `record_recovery_entry_lots`,
  `apply_late_entry_lots` — только RECOVERY/OPEN BUY-учёт, close_order_id
  не трогают; исполнение без order_id отклоняется (FIX 3 не ослаблен).
- `register_executed_buy` (main.py) — только BUY-сторона, P0-5/cursor
  не затронуты.
- `poll_open_orders` — опрашивает только `side="BUY"`; SELL-закрытия
  не переопрашивает.
- В2 (`_try_resolve_absent_position`) — защита от нетерминальной SELL
  при отсутствующей позиции сохранена без изменений (v2.3.2, Дефект-1);
  `close_all_positions.py`, `order_ledger.py`, `strategy.py`,
  `risk_manager.py`, `config.py`, `tinkoff_client.py` — НЕ изменены
  (проверено diff-ом).

## 4. Точные изменения (каждый новый переход состояния)

### 4.1 `execution.py::resolve_uncertain` — ЯДРО ИСПРАВЛЕНИЯ

Было (v2.3.2): направление совпало + `abs(broker.lots) == pos.quantity`
→ `pos.quantity = abs(broker.lots)`; `pos.state = OPEN`.

Стало (v2.3.3): те же проверки совпадения, НО перед OPEN:

```
если pos.close_order_id установлен:
    logging.warning(...повторное открытие ЗАПРЕЩЕНО...)
    pos.state = UNCERTAIN          # НЕТ синхронизации количества
    return
pos.quantity = abs(int(broker.lots))
pos.state = OPEN                   # штатное разрешение — только без активной заявки
```

Новый переход: «UNCERTAIN + совпадение количества + активная собственная
SELL → UNCERTAIN (без изменений учёта)». Количество НЕ синхронизируется
с брокером, пока заявка не разрешена: учётом управляет per-order cursor.

### 4.2 `execution.py::apply_close_execution` — терминальный отчёт с остатком

Было (v2.3.2): `terminal и quantity > 0 → OPEN` немедленно (+ очистка
close_order_id).

Стало (v2.3.3):

- `quantity == 0` → CLOSED (без изменений — полное исполнение).
- `terminal и quantity > 0` → **UNCERTAIN** + очистка завершённой заявки
  (`close_order_id=None`, `close_requested_lots=0`, `close_applied_lots=0`
  — только для текущего oid; журнал `close_orders` сохраняется целиком).
  Новый переход: «терминальный отчёт учтён cursor'ом → заявка очищена,
  остаток ожидает брокерского подтверждения». Штатное разрешение:
  следующий достоверный снимок с совпадающим остатком → OPEN (через 4.1
  без блокировки, т.к. close_order_id уже очищен).

### 4.3 `execution.py::apply_close_execution` — заморозка терминального отчёта

Новая защита (после существующих проверок противоречий, до применения
дельты): если последний записанный статус этой заявки
(`rec["status"]`) терминальный:

- `cumulative > previous` → противоречивый поздний/дублирующий отчёт:
  **fail-closed** — применение запрещено, количество не меняется,
  `pos.state = UNCERTAIN`, `return False`.
- `cumulative == previous` → идемпотентный повтор терминального отчёта:
  полностью игнорируется (`return False`) — ни количества, ни состояния,
  ни дубля CSV. Раньше повтор CANCELLED переводил разрешённую позицию
  обратно в OPEN (обход брокерской сверки), а повтор PARTIALLYFILL
  сбрасывал в UNCERTAIN.

Немедленная (полная) обработка первого терминального отчёта не менялась:
FILL закрывает учёт по фактическим исполнениям и per-order cursor;
прогрессия NEW → PARTIALLYFILL → FILL/CANCELLED работает как прежде
(статус в реестре записи не терминальный — заморозка не применяется).

### 4.4 `execution.py::resolve_closing` — отказ/отмена без исполнения

Было (v2.3.2): `status in (REJECTED, CANCELLED) и lots == 0 → OPEN`
немедленно (+ очистка заявки).

Стало (v2.3.3):

- Возвращаемое значение `apply_close_execution` сохраняется
  (`applied_ok`).
- `applied_ok и status in (2,3) и lots == 0` → **UNCERTAIN** + очистка
  заявки. Новый переход: «REJECTED/CANCELLED без исполнения → заявка
  очищена, остаток разрешается после подтверждения количества брокером»
  (REJECTED без исполнения при подтверждённом совпадении количества
  штатно возвращается в OPEN через resolve_uncertain в том же цикле).
- При `applied_ok=False` (противоречивый отчёт, включая замороженный
  терминальный повтор) ветвь НЕ выполняется — ссылка на заявку
  сохраняется для повторного опроса (устранён латентный дефект v2.3.2:
  ветвь очищала close_order_id даже после противоречия).
- Ветви PARTIALLYFILL→UNCERTAIN и FILL с lots<requested→UNCERTAIN
  сохранены (согласованы с 4.2).

### 4.5 `main.py::process_closing` — защита в глубину

После `monitor.resolve_uncertain(pos, broker)`:

```
если pos.state == OPEN и pos.close_order_id:
    logging.error(...)
    pos.state = UNCERTAIN
```

Инвариант «OPEN невозможен при неподтверждённой терминальности
close-заявки» закреплён на уровне вызывающего кода.

## 5. Сквозные сценарии (как теперь работает)

1. **SELL 10 → PARTIALLYFILL 3 → брокер 7**: resolve_closing применяет
   3 лота (cursor), UNCERTAIN → resolve_uncertain: совпадение 7==7, но
   заявка жива → UNCERTAIN. SL/TP не действуют, повторной SELL нет,
   BUY по FIGI заблокированы. Сверка: расхождений нет (7==7),
   `reconcile_done=True` — блокировка локальная, по FIGI.
2. **Повторные циклы с тем же PARTIALLYFILL**: resolve_closing
   идемпотентен (delta=0), UNCERTAIN сохраняется, `orders_sent` не
   растёт, `close_applications` не задваивается.
3. **CANCELLED 3 → брокер 7**: терминальный отчёт учтён, заявка
   очищена → UNCERTAIN → снимок 7==7 без активной заявки → OPEN 7.
   Далее SL/TP штатно отправляет НОВУЮ SELL ровно на 7 лотов.
4. **PARTIALLYFILL 3 → брокер 10** (снимок не согласован): mismatch
   в resolve_uncertain → UNCERTAIN; reconcile фиксирует расхождение
   (`uncertain_state=True`, `reconcile_done=False`) — глобальная
   блокировка входов до согласования.
5. **NEW 0 → брокер 10**: заявка жива → позиция CLOSING (не OPEN);
   check_all не действует; до терминального статуса повторного
   открытия нет.
6. **get_positions()=None**: снимок недостоверный — resolve_uncertain
   не вызывается, состояние не ослабляется (fail-closed, v2.3.2
   поведение сохранено).
7. **Повторные отчёты после разрешения**: идемпотентный повтор
   терминала игнорируется целиком; противоречивый рост — fail-closed
   (количество не меняется); CSV-дублей и повторных применений нет.

## 6. Изменённые/новые файлы

| Файл | Тип | Что изменено |
|------|-----|--------------|
| `execution.py` | изменён | resolve_uncertain (4.1), apply_close_execution (4.2, 4.3), resolve_closing (4.4), константа `_TERMINAL_SELL_STATUSES`, шапка |
| `main.py` | изменён | process_closing — защита в глубину (4.5), шапка |
| `tests/test_v233_partial_sell_reopen_defect.py` | НОВЫЙ | 7 обязательных интеграционных тестов ТЗ |
| `tests/test_execution.py` | изменён | 2 юнит-теста, кодировавшие старое дефектное поведение «терминальный CANCELLED с остатком → немедленный OPEN»: обновлены под новый контракт (UNCERTAIN → сверка → OPEN); сценарии per-order cursor сохранены и расширены проверками очистки заявки |
| `tests/test_v232_close_defects.py` | изменён | test_defect2_4 (REJECTED + позиция отсутствует): ожидание цикла 1 OPEN → UNCERTAIN (немедленный OPEN без сверки — устраняемый дефект семейства); сценарий В3 сохранён |
| `tests/test_manual_close_integration.py` | изменён | test_restart_sell_rejected_position_absent_no_false_close — аналогично; fixture env: `RECONCILE_CONFIRM_MIN_AGE_SECONDS` 0.0 → 30.0 (производительный минимум, как в test_v232) + явная имитация разноса снимков (age_candidate) в трёх В3-тестах. Причина: исправление оставляет позицию в UNCERTAIN, первый reconcile цикла выполняется в process_closing — при нулевом интервале два reconcile ОДНОГО цикла засчитывались за двойное подтверждение В3 в обход ТЗ-3 (артефакт тестовой среды, не прод-конфигурации) |
| `izmenenija_v2.3.2_to_v2.3.3.diff` | НОВЫЙ | полный diff v2.3.2 → v2.3.3 |
| `РЕЗУЛЬТАТЫ_ТЕСТОВ_v2.3.3.txt` | НОВЫЙ | доказательная база прогонов |
| `SHA256SUMS_v2.3.3.txt` | НОВЫЙ | контрольные суммы |

НЕ изменены (проверено diff-ом против v2.3.2): `strategy.py`,
`order_ledger.py`, `close_all_positions.py`, `risk_manager.py`,
`config.py`, `tinkoff_client.py`, `logger.py`, `screener.py`,
`telegram_bot.py` и все остальные файлы проекта.

## 7. Тесты

### 7.1 Новые интеграционные тесты (полный цикл trading_cycle)

`tests/test_v233_partial_sell_reopen_defect.py` — 7 обязательных сценариев ТЗ:

1. `test_1_partialfill_broker_match_does_not_reopen` — PARTIALLYFILL 3 +
   брокер 7: позиция НЕ OPEN; проверены state/quantity/close_order_id/
   close_applied_lots/close_applications/orders_sent/uncertain_state/
   reconcile_done/CSV; прямой контроль check_all: SL пробит (97 < 98),
   но закрытия нет.
2. `test_2_repeated_cycles_partialfill_no_resell` — 3 цикла с тем же
   PARTIALLYFILL (цена всё время пробивает SL): повторная SELL не
   отправляется ни разу, применения не задваиваются.
3. `test_3_cancelled_partial_resolves_open_after_broker_match` —
   CANCELLED 3 + брокер 7 → OPEN 7, заявка очищена; далее SL отправляет
   НОВУЮ SELL ровно на 7 лотов (штатное возобновление).
4. `test_4_snapshot_not_reconciled_stays_uncertain` — PARTIALLYFILL 3 +
   брокер 10: UNCERTAIN, `uncertain_state=True`, `reconcile_done=False`,
   повторная SELL запрещена.
5. `test_5_new_order_does_not_reopen_for_sltp` — NEW 0 + брокер 10:
   CLOSING (не OPEN) в двух циклах, SL-пробой не вызывает закрытия.
6. `test_6_none_snapshot_does_not_weaken_state` — None-снимок при живой
   частичной SELL: UNCERTAIN сохраняется, кандидат/сверка не тронуты,
   восстановленный достоверный снимок не открывает позицию.
7. `test_7_duplicate_reports_after_terminal_resolution` — после
   разрешения (CANCELLED 3 → OPEN 7): идемпотентный повтор CANCELLED
   игнорируется; запоздалый PARTIALLYFILL не сбрасывает состояние;
   противоречивый FILL 10 — fail-closed (количество 7 не меняется,
   UNCERTAIN); полный цикл штатно возвращает OPEN 7; CSV-дублей нет,
   `close_applications` == 1.

### 7.2 Верификация на дефектном коде

Новые тесты на коде v2.3.2: **5 failed, 2 passed** — падают именно
дефект-специфичные сценарии (1, 2, 3, 6, 7). Сценарии 4 и 5 в v2.3.2
не открывали позицию и раньше — сохранены как регрессионные.

### 7.3 Полный набор и сравнение с базой

- Полный pytest: **321 passed, 25 failed**.
- Список 25 падений ПОБАЙТОВО идентичен базе v2.3.2 (снята на
  немодифицированном коде перед правками): среда/внешние зависимости —
  test_screener (6), test_strategy ADX (3), test_tinkoff_client (3),
  test_persistence (5), test_trading_flag (3), test_main (1),
  test_p0_batch2 (2), test_p0_defect_fixes (2).
- **Новых падений нет** (diff списка пуст).

## 8. Соблюдение ограничений ТЗ

- STRATEGY LOGIC: UNCHANGED (strategy.py не тронут; входы, индикаторы,
  SL/TP, риск-модель без изменений).
- `order_ledger.py` — НЕ изменён (не потребовался).
- `close_all_positions.py` — НЕ изменён.
- P0-5, fail-closed, per-order cursor, защита от поздних исполнений,
  двойное подтверждение В3 — не ослаблены (усилены: заморозка
  терминальных отчётов 4.3, applied_ok-гейт 4.4, защита 4.5).
- Журналы не удалялись и не обрезались (close_orders/close_applications
  сохраняются полностью; очистка только legacy-зеркала текущей заявки).
- Торговые параметры не менялись (config.py не тронут).
- Только SANDBOX: тесты используют FakeBroker без сетевых вызовов.

## 9. Готовность

Все обязательные сценарии ТЗ проходят (7/7). Заявлять готовность к
песочнице можно только после прогонов на боевой песочнице оператором;
настоящая сборка устраняет блокирующий дефект повторного открытия и
не добавляет новых падений относительно v2.3.2.
