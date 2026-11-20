# tests/test_v233_partial_sell_reopen_defect.py
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ v2.3.3-stage1 (дефект «повторное открытие позиции
# при активной частичной SELL»).
#
# Сценарий дефекта (v2.3.2):
#   1. Локальная позиция BUY: 10 лотов.
#   2. Робот отправил SELL на 10 лотов (close_order_id активен).
#   3. Брокер сообщает PARTIALLYFILL, исполнено 3 лота.
#   4. apply_close_execution() уменьшает локальное количество до 7 и
#      переводит позицию в UNCERTAIN.
#   5. process_closing() получает достоверный снимок брокера с остатком
#      7 лотов.
#   6. resolve_uncertain() видит совпадение broker.lots == pos.quantity и
#      переводит позицию в OPEN.
#   7. Старая SELL-заявка ещё не завершена -> SL/TP снова включаются и
#      могут отправить ПОВТОРНУЮ SELL (перепродажа).
#
# Главный принцип исправления (v2.3.3-stage1): совпадение локального
# остатка с брокерским количеством НЕ является основанием переводить
# позицию в OPEN, пока собственная SELL-заявка имеет неподтверждённую
# терминальность. Инвариант: close_order_id сбрасывается ТОЛЬКО при
# обработке терминального отчёта, поэтому его наличие == «заявка ещё
# жива или её исход не подтверждён».
#
# Обязательные сценарии (полный цикл trading_cycle):
#   1. PARTIALLYFILL 3 + брокер 7 -> позиция НЕ становится OPEN.
#   2. Повторные циклы с тем же PARTIALLYFILL -> повторная SELL не
#      отправляется, исполнения не применяются повторно.
#   3. CANCELLED (3 исполнено) + брокер 7 -> позиция разрешается как
#      OPEN, количество 7, старая заявка больше не активна; далее SL/TP
#      штатно отправляет НОВУЮ SELL ровно на 7 лотов.
#   4. PARTIALLYFILL 3 + брокер 10 (снимок не согласован) -> позиция
#      неопределённая, повторная SELL запрещена.
#   5. NEW (0 исполнено) + брокер 10 -> позиция не открывается для SL/TP
#      до терминального статуса заявки.
#   6. get_positions()=None при активной частичной SELL -> состояние не
#      ослабляется.
#   7. Повторные/противоречивые отчёты по старой заявке после
#      терминального разрешения -> количество не меняется, дублей CSV и
#      повторных применений нет.
#
# В тестах проверяются: pos.state, pos.quantity, pos.close_order_id,
# pos.close_applied_lots, pos.close_applications, broker.orders_sent,
# broker.uncertain_state, reconcile_done, отсутствие дублей CSV и
# повторного применения исполнений (per-order cursor).
#
# Ограничения ТЗ соблюдены: стратегия, OrderLedger, P0-5, fail-closed и
# журналы не тронуты. Только SANDBOX: FakeBroker не выполняет сетевых
# вызовов и реальных заявок.
#
# Запуск из корня сборки: pytest tests/test_v233_partial_sell_reopen_defect.py -v

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import logger as research_logger
from execution import ExecutionMonitor, PositionState, TrackedPosition
from order_ledger import OrderLedger

UTC = timezone.utc

_IMPORT_ERROR = None
try:
    import main
except Exception as exc:  # noqa: BLE001
    _IMPORT_ERROR = exc

pytestmark = pytest.mark.skipif(
    _IMPORT_ERROR is not None,
    reason=f"main.py недоступен: {_IMPORT_ERROR}",
)

# Оригиналы глобальных функций main (восстанавливаются после тестов).
_ORIG_IS_TRADING_ENABLED = main.is_trading_enabled
_ORIG_SAVE_STATE = main._save_state


# ---------------------------------------------------------------- fake broker

class PosData:
    """Минимальный аналог PositionData брокера."""
    def __init__(self, lots, direction):
        self.lots = lots
        self.direction = direction


class _MV:
    def __init__(self, units, nano=0):
        self.units = units
        self.nano = nano


class FakeOrderState:
    """Отчёт заявки брокера: FILL=1, REJECTED=2, CANCELLED=3, NEW=4, PARTIALLYFILL=5."""
    def __init__(self, status_code=1, lots_executed=10, total_price_units=1000):
        self.execution_report_status = status_code
        self.lots_executed = lots_executed
        self.executed_order_price = _MV(total_price_units)
        self.executed_commission = None


class FakeBroker:
    """Фейковый брокер: никаких сетевых вызовов и реальных заявок."""
    def __init__(self, positions=None, prices=None, order_states=None):
        self.positions = positions if positions is not None else {}
        self.prices = prices if prices is not None else {}
        self.order_states = order_states or {}
        self.orders_sent = []    # (figi, lots, direction, order_id)
        self.cancelled = []
        self.uncertain_state = False

    def is_uncertain(self):
        return self.uncertain_state

    async def get_positions(self):
        return self.positions  # None трактуется системой как недостоверный снимок

    async def get_last_prices(self, figis):
        return {f: self.prices[f] for f in figis if f in self.prices}

    async def get_instrument_info(self, figi):
        return {"lot": 1, "ticker": "TEST"}

    async def get_sandbox_order_state(self, oid):
        return self.order_states.get(oid)

    async def post_market_order(self, figi, lots, direction, order_id=None):
        # Каждая отправка фиксируется для ассерта «повторная SELL запрещена».
        self.orders_sent.append((figi, lots, str(direction), order_id))
        new_state = FakeOrderState(4, 0, 0)
        if order_id:
            self.order_states[order_id] = new_state
        return SimpleNamespace(state=new_state, order_id=order_id, error=None)

    async def cancel_order(self, oid):
        self.cancelled.append(oid)
        return SimpleNamespace(status="OK")


# ---------------------------------------------------------------- helpers

def make_buy_pos(**over):
    base = dict(
        figi="F1", ticker="TEST", direction="BUY", entry_price=100.0,
        quantity=10, sl_price=98.0, tp_price=104.0, lot_size=1,
        state=PositionState.OPEN, trade_id="t1",
        order_send_time=(datetime.now(UTC) - timedelta(hours=1)).isoformat(),
        entry_order_id="buy-1",
    )
    base.update(over)
    return TrackedPosition(**base)


def proven_buy(ledger, figi="F1", lots=10):
    """Подтверждённая брокером BUY-заявка входа в реестре (терминальный FILL)."""
    assert ledger.record_intent("buy-1", trade_id="t1", figi=figi, ticker="TEST",
                                side="BUY", requested_lots=lots, reason="")
    assert ledger.record_sent("buy-1", figi=figi, ticker="TEST", side="BUY",
                              requested_lots=lots, executed_lots=lots,
                              status_hint="FILL", reason="", source="post_response")


def record_close_sell(ledger, oid="sell-1", figi="F1", requested=10,
                      status=None, executed=None, reason="close:SL"):
    """Запись SELL-заявки закрытия в реестре (INTENT + опциональный статус)."""
    assert ledger.record_intent(oid, trade_id="t1", figi=figi, ticker="TEST",
                                side="SELL", requested_lots=requested, reason=reason)
    if status is not None:
        assert ledger.record_status(oid, status, figi=figi, ticker="TEST",
                                    side="SELL", requested_lots=requested,
                                    executed_lots=executed,
                                    reason=reason, source="poll")


def run(coro):
    return asyncio.run(coro)


def read_events(tmp_path):
    path = tmp_path / "research_events.jsonl"
    if not path.is_file():
        return []
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def read_trades(tmp_path):
    path = tmp_path / "research_trades.csv"
    with open(path, "r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f, delimiter=";"))


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Изолированная среда: все записи в tmp_path; торговля выключена."""
    monkeypatch.setattr(main, "is_trading_enabled", _ORIG_IS_TRADING_ENABLED)
    monkeypatch.setattr(main, "_save_state", _ORIG_SAVE_STATE)
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv"))
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))
    monkeypatch.setattr(main, "TRADING_FLAG", str(tmp_path / "trading.flag"))
    monkeypatch.setattr(config, "RECONCILE_CONFIRM_MIN_AGE_SECONDS", 30.0)
    main._external_close_candidates.clear()
    main.reconcile_done = False
    main.state_save_failed = False
    main.telegram_bot = None
    main.top_instruments = []
    ledger = OrderLedger(str(tmp_path / "order_ledger.jsonl"))
    ledger.load()
    main.ledger_ref = ledger
    yield SimpleNamespace(tmp_path=tmp_path, ledger=ledger)
    main._external_close_candidates.clear()


def fresh_monitor(pos, client):
    monitor = ExecutionMonitor(client)
    monitor.positions[pos.figi] = pos
    return monitor


def closing_pos_with_sell(requested=10):
    """Позиция BUY 10 лотов сразу после отправки SELL (CLOSING, заявка активна)."""
    return make_buy_pos(
        quantity=10, state=PositionState.CLOSING,
        close_reason="SL", close_order_id="sell-1",
        close_requested_lots=requested,
        close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
    )


# ============================================================================
# Сценарий 1: PARTIALLYFILL + совпадающий остаток брокера — НЕ OPEN
# ============================================================================

def test_1_partialfill_broker_match_does_not_reopen(env):
    """Локально 10 лотов, SELL 10, PARTIALLYFILL 3, брокер показывает 7.
    Позиция НЕ должна стать OPEN: заявка ещё жива (close_order_id активен),
    SL/TP не включаются, повторная SELL не отправляется, количество
    уменьшено только на подтверждённые 3 лота."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(7, "LONG")}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})  # PARTIALLYFILL, 3 лот.
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    # Ядро дефекта: совпадение 7 == 7 НЕ переводит позицию в OPEN.
    assert pos.state == PositionState.UNCERTAIN, (
        "позиция переоткрыта в OPEN при живой PARTIALLYFILL SELL — дефект"
    )
    assert pos.state != PositionState.OPEN
    assert pos.quantity == 7                  # применены только подтверждённые 3 лота
    assert pos.close_order_id == "sell-1"     # заявка всё ещё считается активной
    assert pos.close_applied_lots == 3
    assert len(pos.close_applications) == 1   # одно применение: 0 -> 3, без дублей
    assert pos.close_applications[0]["delta_lots"] == 3
    assert pos.close_orders["sell-1"]["applied_lots"] == 3
    assert pos.close_orders["sell-1"]["status"] == "PARTIALLYFILL"
    assert broker.orders_sent == []           # повторная SELL не отправлялась
    assert main.reconcile_done is True        # снимок достоверный, расхождений нет
    assert broker.uncertain_state is False    # блокировка локальная (по FIGI), не глобальная
    assert len(read_trades(tmp_path)) == 0    # CSV не пишется — закрытия не было

    # SL/TP НЕ включаются для такой позиции: цена пробивает SL (98), но
    # позиция UNCERTAIN — check_all действует только на OPEN.
    to_close = run(monitor.check_all({"F1": 97.0}))
    assert to_close == [], "SL сработал по позиции с живой SELL-заявкой"
    assert broker.orders_sent == []


# ============================================================================
# Сценарий 2: повторные циклы с тем же PARTIALLYFILL — без повторной SELL
# ============================================================================

def test_2_repeated_cycles_partialfill_no_resell(env):
    """Несколько последовательных циклов с тем же PARTIALLYFILL (3 из 10)
    и брокерским остатком 7 (цена всё время пробивает SL): повторная SELL
    не отправляется ни в одном цикле, исполнения не применяются повторно,
    количество стабильно."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(7, "LONG")}, prices={"F1": 97.0},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    for cycle in range(1, 4):
        run(main.trading_cycle(broker, rm, None, monitor, ledger))
        assert pos.state == PositionState.UNCERTAIN, f"цикл {cycle}: позиция переоткрыта"
        assert pos.state != PositionState.OPEN
        assert pos.quantity == 7, f"цикл {cycle}: количество изменилось без подтверждения"
        assert pos.close_order_id == "sell-1", f"цикл {cycle}: заявка потеряна"
        assert pos.close_applied_lots == 3
        assert len(pos.close_applications) == 1, f"цикл {cycle}: повторное применение исполнения"
        assert broker.orders_sent == [], f"цикл {cycle}: повторная SELL отправлена"
        assert len(read_trades(tmp_path)) == 0

    # Явный контроль SL/TP: цена ниже SL во всех циклах, закрытий нет.
    to_close = run(monitor.check_all({"F1": 96.0}))
    assert to_close == []
    assert broker.orders_sent == []


# ============================================================================
# Сценарий 3: CANCELLED с частичным исполнением — штатное разрешение остатка
# ============================================================================

def test_3_cancelled_partial_resolves_open_after_broker_match(env):
    """После CANCELLED с исполненными 3 лотами и брокерским остатком 7
    позиция разрешается как OPEN, количество 7, старая заявка больше не
    считается активной. Далее SL/TP штатно отправляет НОВУЮ SELL ровно
    на 7 лотов (штатное возобновление защиты позиции)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(7, "LONG")}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: PARTIALLYFILL 3 + брокер 7 -> UNCERTAIN (сценарий 1) ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 7

    # --- цикл 2: заявка ДОСТИГЛА терминального статуса CANCELLED (3 лот.) ---
    broker.order_states["sell-1"] = FakeOrderState(3, 3, 300)  # CANCELLED, цена 100.0
    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.OPEN, "остаток не разрешён после терминального отчёта"
    assert pos.quantity == 7                  # подтверждённые 3 лота учтены
    assert pos.close_order_id is None, "завершённая заявка не очищена"
    assert pos.close_applied_lots == 0        # legacy-зеркало очищено вместе с заявкой
    assert pos.close_requested_lots == 0
    # история заявки сохранена (журнал не обрезается)
    assert pos.close_orders["sell-1"]["status"] == "CANCELLED"
    assert pos.close_orders["sell-1"]["applied_lots"] == 3
    assert len(pos.close_applications) == 1   # повторного применения нет
    assert pos.executed_lots == 3
    assert broker.orders_sent == []           # разрешение — БЕЗ новых заявок
    assert main.reconcile_done is True
    assert broker.uncertain_state is False
    assert len(read_trades(tmp_path)) == 0    # позиция жива — CSV нет

    # --- цикл 3: SL пробит — штатная НОВАЯ SELL ровно на остаток 7 ---
    broker.prices["F1"] = 97.0
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert len(broker.orders_sent) == 1, "после разрешения остатка SL/TP не отправили новую SELL"
    assert broker.orders_sent[0][0] == "F1"
    assert broker.orders_sent[0][1] == 7, "новая SELL отправлена не на остаток (7)"
    assert pos.state == PositionState.CLOSING
    assert pos.close_order_id is not None and pos.close_order_id != "sell-1"
    assert pos.close_requested_lots == 7


# ============================================================================
# Сценарий 4: снимок брокера ещё не согласован (10 вместо 7)
# ============================================================================

def test_4_snapshot_not_reconciled_stays_uncertain(env):
    """PARTIALLYFILL 3 (локальный остаток 7), брокер показывает 10 — снимок
    ещё не согласован. Позиция остаётся неопределённой, повторная SELL
    запрещена, расхождение фиксируется сверкой (uncertain_state=True,
    reconcile_done=False)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(10, "LONG")}, prices={"F1": 97.0},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.UNCERTAIN, "позиция переоткрыта по несогласованному снимку"
    assert pos.state != PositionState.OPEN
    assert pos.quantity == 7
    assert pos.close_order_id == "sell-1"
    assert pos.close_applied_lots == 3
    assert len(pos.close_applications) == 1
    assert broker.orders_sent == []
    assert broker.uncertain_state is True     # расхождение зафиксировано
    assert main.reconcile_done is False       # сверка не завершена
    assert len(read_trades(tmp_path)) == 0

    # SL/TP не действуют на неопределённую позицию.
    to_close = run(monitor.check_all({"F1": 97.0}))
    assert to_close == []
    assert broker.orders_sent == []


# ============================================================================
# Сценарий 5: NEW без исполнения — до терминала SL/TP не включаются
# ============================================================================

def test_5_new_order_does_not_reopen_for_sltp(env):
    """NEW, 0 исполнено, брокер показывает 10. Позиция не должна повторно
    открываться для SL/TP до терминального статуса заявки: разрешение
    UNCERTAIN переводит её в CLOSING (заявка жива), check_all действует
    только на OPEN — новых SELL нет."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="NEW", executed=0)
    pos = make_buy_pos(
        quantity=10, state=PositionState.UNCERTAIN,
        close_reason="SL", close_order_id="sell-1",
        close_requested_lots=10,
        close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
    )
    broker = FakeBroker(positions={"F1": PosData(10, "LONG")}, prices={"F1": 97.0},
                        order_states={"sell-1": FakeOrderState(4, 0, 0)})  # NEW, 0 лот.
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    for cycle in (1, 2):
        run(main.trading_cycle(broker, rm, None, monitor, ledger))
        assert pos.state != PositionState.OPEN, f"цикл {cycle}: позиция переоткрыта при живой NEW-заявке"
        assert pos.state == PositionState.CLOSING  # заявка жива — закрытие в процессе
        assert pos.close_order_id == "sell-1"
        assert pos.quantity == 10                  # исполнений нет — количество не тронуто
        assert pos.close_applied_lots == 0
        assert pos.close_applications == []        # применений нет
        assert broker.orders_sent == [], f"цикл {cycle}: повторная SELL отправлена"
        assert len(read_trades(tmp_path)) == 0

    # Явный контроль: цена пробивает SL, но позиция CLOSING — закрытий нет.
    to_close = run(monitor.check_all({"F1": 96.0}))
    assert to_close == []
    assert broker.orders_sent == []


# ============================================================================
# Сценарий 6: недостоверный снимок (None) при активной частичной SELL
# ============================================================================

def test_6_none_snapshot_does_not_weaken_state(env):
    """get_positions()=None при активной частичной SELL. Состояние не
    ослабляется: UNCERTAIN не разрешается (ни в OPEN, ни в CLOSED),
    кандидат В3/сверка не трогаются, повторная SELL не отправляется;
    восстановление достоверного снимка не открывает позицию, пока заявка
    жива."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(7, "LONG")}, prices={"F1": 97.0},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: PARTIALLYFILL 3 + брокер 7 -> UNCERTAIN ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 7

    # --- цикл 2: НЕДОСТОВЕРНЫЙ снимок (get_positions()=None) ---
    broker.positions = None
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN       # не ослабился и не закрылся
    assert pos.state != PositionState.OPEN
    assert pos.quantity == 7
    assert pos.close_order_id == "sell-1"
    assert pos.close_applied_lots == 3
    assert len(pos.close_applications) == 1
    assert broker.orders_sent == []
    assert broker.uncertain_state is True             # недостоверный снимок — неопределённость
    assert main.reconcile_done is False
    assert len(read_trades(tmp_path)) == 0

    # --- цикл 3: достоверный снимок восстановлен (7 лотов) — заявка всё ещё
    # жива: позиция НЕ переоткрывается ---
    broker.positions = {"F1": PosData(7, "LONG")}
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN, "позиция переоткрыта после None-снимка при живой SELL"
    assert pos.state != PositionState.OPEN
    assert pos.close_order_id == "sell-1"
    assert broker.orders_sent == []
    assert len(read_trades(tmp_path)) == 0


# ============================================================================
# Сценарий 7: повторные отчёты после терминального разрешения
# ============================================================================

def test_7_duplicate_reports_after_terminal_resolution(env):
    """После терминального разрешения (CANCELLED 3 -> OPEN 7) повторный
    отчёт по старой заявке не меняет количество и не создаёт повторную
    запись CSV:
      - идемпотентный повтор CANCELLED (3) — полностью игнорируется;
      - запоздалый PARTIALLYFILL (3) — игнорируется (состояние не
        сбрасывается);
      - противоречивый рост FILL (10) после терминала — fail-closed:
        применение запрещено, количество не меняется, UNCERTAIN;
        следующий достоверный снимок штатно возвращает OPEN."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = closing_pos_with_sell()
    broker = FakeBroker(positions={"F1": PosData(7, "LONG")}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- разрешение: PARTIALLYFILL -> CANCELLED -> OPEN 7 (сценарий 3) ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN and pos.quantity == 7
    broker.order_states["sell-1"] = FakeOrderState(3, 3, 300)
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.OPEN
    assert pos.quantity == 7
    assert pos.close_order_id is None
    assert len(pos.close_applications) == 1

    # --- повторные отчёты по СТАРОЙ заявке ---
    # 1) идемпотентный повтор терминального CANCELLED (3): игнорируется
    r1 = monitor.apply_close_execution(pos, "sell-1", 3, "CANCELLED", requested_lots=10)
    assert r1 is False
    assert pos.state == PositionState.OPEN            # состояние не сброшено
    assert pos.quantity == 7                          # количество не изменено
    assert len(pos.close_applications) == 1           # повторного применения нет

    # 2) запоздалый PARTIALLYFILL (3) по завершённой заявке: игнорируется
    r2 = monitor.apply_close_execution(pos, "sell-1", 3, "PARTIALLYFILL", requested_lots=10)
    assert r2 is False
    assert pos.state == PositionState.OPEN            # раньше повтор PF сбрасывал в UNCERTAIN
    assert pos.quantity == 7
    assert len(pos.close_applications) == 1

    # 3) противоречивый рост FILL (10) после учтённого терминала: fail-closed
    r3 = monitor.apply_close_execution(pos, "sell-1", 10, "FILL", requested_lots=10)
    assert r3 is False
    assert pos.quantity == 7, "противоречивый отчёт изменил количество"
    assert pos.state == PositionState.UNCERTAIN       # противоречие требует внимания
    assert len(pos.close_applications) == 1           # применение не задвоено
    assert pos.close_orders["sell-1"]["applied_lots"] == 3

    # --- полный цикл: противоречие разрешается штатной сверкой ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.OPEN            # снимок 7 == локальные 7
    assert pos.quantity == 7
    assert pos.close_order_id is None
    assert len(pos.close_applications) == 1           # по-прежнему одно применение 0 -> 3
    assert pos.close_orders["sell-1"]["applied_lots"] == 3
    assert broker.orders_sent == []                   # ни одной лишней заявки
    assert main.reconcile_done is True
    assert broker.uncertain_state is False
    # позиция ни разу не закрывалась учётом — ни одной CSV-строки и дублей
    rows = read_trades(tmp_path)
    assert len(rows) == 0
    events = [e["event"] for e in read_events(tmp_path)]
    assert events.count("POSITION_CLOSED_BY_LEDGER") == 0
    assert events.count("POSITION_CLOSED_EXTERNAL") == 0
