# tests/test_v232_close_defects.py
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ v2.3.2-stage1 (аудит v2.3.1: два блокирующих дефекта).
#
# Дефект 1. Нетерминальная SELL-заявка (_try_resolve_absent_position, В2).
#   Тест D1: SELL на 10 лот., исполнено 3, статус PARTIALLYFILL, позиция у
#   брокера отсутствует. Локальное ОКОНЧАТЕЛЬНОЕ закрытие запрещено: учёт не
#   закрывается, SELL повторно не отправляется, позиция UNCERTAIN, новые
#   входы блокированы. Разрешение — только терминальный отчёт заявки.
#
# Дефект 2. Обход двойного подтверждения через UNCERTAIN
#   (resolve_uncertain в execution.py + process_closing в main.py).
#   Раньше UNCERTAIN + отсутствие позиции у брокера закрывало учёт по ОДНОМУ
#   снимку в обход механизма В3. Теперь:
#     1. UNCERTAIN без close_order_id, позиция исчезла — только кандидат В3,
#        закрытие после двух достоверных снимков с минимальным интервалом.
#     2. UNCERTAIN с SELL PARTIALLYFILL, позиция исчезла — учёт не закрывается;
#        терминальный отчёт собственной SELL сохраняет штатное закрытие (В2).
#     3. UNCERTAIN с SELL NEW, позиция исчезла — никакого мгновенного
#        закрытия и повторной SELL; далее двойное подтверждение В3.
#     4. UNCERTAIN с SELL REJECTED, позиция исчезла — возврат в OPEN,
#        кандидат В3, затем двойное подтверждение.
#     5. get_positions()=None между двумя подтверждениями — ни подтверждает,
#        ни сбрасывает подозрение (кандидат не тронут).
#     6. Рестарт между первым и вторым подтверждением — мгновенного закрытия
#        нет: двойное подтверждение начинается заново (кандидаты не
#        персистятся намеренно — консервативное поведение).
#     7. Повторный отчёт об исполнении после разрешения ситуации —
#        идемпотентен (per-order cursor), «воскрешения» и дубля CSV нет.
#
# Ограничения ТЗ соблюдены: торговая стратегия, OrderLedger, P0-5, per-order
# cursor и fail-closed не ослаблены.
#
# Запуск из корня сборки: pytest tests/test_v232_close_defects.py -v
# Вне окружения репозитория используются заглушки SDK из tests/conftest.py.

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

# Оригиналы глобальных функций main (восстанавливаются после тестов, которые
# могли подменить их MagicMock'ами без возврата — см. fixture env).
_ORIG_IS_TRADING_ENABLED = main.is_trading_enabled
_ORIG_SAVE_STATE = main._save_state

MIN_AGE = 30.0  # производственный минимум интервала двойного подтверждения В3


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
        # В этих тестах робот НЕ ДОЛЖЕН отправлять заявок: каждая отправка —
        # нарушение запрета повторной отправки SELL. Фиксируем для ассерта.
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


def age_candidate(figi, seconds=MIN_AGE + 1):
    """Сдвигает first_seen кандидата В3 в прошлое (имитация прошедшего времени
    между двумя достоверными снимками; разнос >= RECONCILE_CONFIRM_MIN_AGE_SECONDS)."""
    ts = main._external_close_candidates.get(figi)
    assert ts, f"кандидат {figi} не зарегистрирован — сдвигать нечего"
    dt = datetime.fromisoformat(ts) - timedelta(seconds=seconds)
    main._external_close_candidates[figi] = dt.isoformat()


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Изолированная среда: все записи в tmp_path; торговля выключена.
    Минимальный интервал В3 оставлен ПРОИЗВОДИТЕЛЬНЫМ (MIN_AGE=30 c):
    разнос двух достоверных снимков имитируется age_candidate()."""
    monkeypatch.setattr(main, "is_trading_enabled", _ORIG_IS_TRADING_ENABLED)
    monkeypatch.setattr(main, "_save_state", _ORIG_SAVE_STATE)
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv"))
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))
    monkeypatch.setattr(main, "TRADING_FLAG", str(tmp_path / "trading.flag"))
    monkeypatch.setattr(config, "RECONCILE_CONFIRM_MIN_AGE_SECONDS", MIN_AGE)
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


# ============================================================================
# Дефект 1: нетерминальная SELL-заявка
# ============================================================================

def test_defect1_sell_partialfill_absent_no_final_close(env):
    """D1: SELL на 10 лот., исполнено 3, статус PARTIALLYFILL, позиция у
    брокера отсутствует. Локальное окончательное закрытие ЗАПРЕЩЕНО:
      - учёт не закрывается (позиция UNCERTAIN, quantity уменьшен только на
        подтверждённые per-order cursor'ом 3 лота);
      - SELL повторно НЕ отправляется;
      - новые входы блокированы (client.uncertain_state=True,
        reconcile_done=False);
      - терминальный отчёт заявки (FILL 10) разрешает закрытие штатно
        с реальной ценой исполнения."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})  # PARTIALLYFILL, 3 лот.
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: нетерминальный статус — окончательное закрытие запрещено ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.UNCERTAIN, "учёт закрыт по живой SELL — дефект 1"
    assert pos.quantity == 7          # 3 подтверждённых лота применены cursor'ом, НЕ «закрыто всё»
    assert pos.close_reason == "SL"   # причина не подменена (нет LEDGER_CLOSE/EXTERNAL_CLOSE)
    assert pos.logged is False
    assert broker.orders_sent == []   # повторная отправка SELL запрещена
    assert broker.uncertain_state is True
    assert main.reconcile_done is False
    assert len(read_trades(tmp_path)) == 0   # CSV не пишется — закрытия не было
    events = [e["event"] for e in read_events(tmp_path)]
    assert "SELL_NON_TERMINAL_CLOSE_BLOCKED" in events

    # --- цикл 2: заявка ДОСТИГЛА терминального статуса (FILL 10 лот.) ---
    broker.order_states["sell-1"] = FakeOrderState(1, 10, 1000)  # FILL, цена 100.0
    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.CLOSED          # теперь закрытие разрешено
    assert pos.quantity == 0
    assert pos.executed_lots == 10
    assert pos.exit_price_confirmed is True           # цена из отчёта СВОЕЙ SELL
    assert pos.execution_price == pytest.approx(100.0)
    assert broker.orders_sent == []                   # по-прежнему ни одной заявки
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    assert rows[0]["exit_price"] == "100.0000"
    assert rows[0]["net_pnl_rub"] != ""               # реальный P&L по терминальному отчёту


# ============================================================================
# Дефект 2: обход двойного подтверждения через UNCERTAIN
# ============================================================================

def test_defect2_1_uncertain_no_close_order_absent_waits_v3(env):
    """Сценарий 1: UNCERTAIN без close_order_id, позиция исчезла.
    Цикл 1 — учёт НЕ закрывается (кандидат В3); цикл 2 (второй достоверный
    снимок, разнос >= MIN_AGE) — закрытие EXTERNAL_CLOSE через ЕДИНЫЙ механизм
    В3; цикл 3 — CSV без фиктивного P&L."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN, close_reason=None)
    broker = FakeBroker(positions={}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # Прямой контракт исправления: resolve_uncertain при отсутствии позиции
    # НЕ закрывает учёт (раньше — мгновенный CLOSED, дефект 2).
    monitor.resolve_uncertain(pos, {})
    assert pos.state == PositionState.UNCERTAIN
    assert pos.quantity == 10
    assert pos.close_reason is None

    # --- цикл 1: первое обнаружение — только кандидат В3 ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN       # НЕ закрыт по одному снимку
    assert pos.quantity == 10
    assert "F1" in main._external_close_candidates    # подозрение зафиксировано
    assert broker.orders_sent == []
    assert len(read_trades(tmp_path)) == 0
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_EXTERNAL_CLOSE_SUSPECTED" in events

    # --- цикл 2: двойное подтверждение через В3 ---
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.exit_price_confirmed is False          # цена исполнения неизвестна
    assert "P&L не рассчитан" in pos.error_note
    assert broker.orders_sent == []
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_EXTERNAL" in events
    assert "SELL_NON_TERMINAL_CLOSE_BLOCKED" not in events

    # --- цикл 3: CSV-строка без фиктивного P&L ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    assert rows[0]["exit_reason"] == "EXTERNAL_CLOSE"
    assert rows[0]["exit_price"] == ""
    assert rows[0]["net_pnl_rub"] == ""


def test_defect2_2_uncertain_sell_partialfill_absent_no_close(env):
    """Сценарий 2: UNCERTAIN с SELL PARTIALLYFILL (3 из 10), позиция исчезла.
    Цикл 1 — окончательное закрытие запрещено (дефект 1): учёт остаётся в
    UNCERTAIN. Цикл 2 — заявка достигла терминального статуса (CANCELLED с
    3 исполненными): закрытие по ДОСТОВЕРНОМУ ТЕРМИНАЛЬНОМУ отчёту собственной
    SELL-заявки СОХРАНЕНО (В2, реальная цена)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="PARTIALLYFILL", executed=3)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(5, 3, 300)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: нетерминальная SELL — учёт не закрывается ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN       # НЕ закрыто
    assert pos.quantity == 7                          # cursor применил только 3 подтверждённых лота
    assert pos.close_reason == "SL"                   # без подмены причины
    assert broker.orders_sent == []
    assert broker.uncertain_state is True
    assert len(read_trades(tmp_path)) == 0
    events = [e["event"] for e in read_events(tmp_path)]
    assert "SELL_NON_TERMINAL_CLOSE_BLOCKED" in events

    # --- цикл 2: терминальный отчёт (CANCELLED, исполнено 3) разрешает В2 ---
    broker.order_states["sell-1"] = FakeOrderState(3, 3, 300)  # CANCELLED, 3 лот., цена 100.0
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED          # закрытие по терминальному отчёту
    assert pos.close_reason == "SL"
    assert pos.executed_lots == 3
    assert pos.exit_price_confirmed is True
    assert pos.execution_price == pytest.approx(100.0)
    assert broker.orders_sent == []
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_BY_LEDGER" in events      # В2: закрытие реестром


def test_defect2_3_uncertain_sell_new_absent_waits_v3(env):
    """Сценарий 3: UNCERTAIN с SELL NEW (0 из 10), позиция исчезла.
    Исполнение собственной заявки НЕ подтверждено — никакого мгновенного
    закрытия, повторной SELL нет; закрытие — только двойное подтверждение В3
    (цикл 2, разнос >= MIN_AGE)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="NEW", executed=0)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(4, 0, 0)})  # NEW, 0 лот.
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: учёт не закрывается, SELL повторно не отправляется ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state in (PositionState.UNCERTAIN, PositionState.CLOSING)
    assert pos.state != PositionState.CLOSED          # НЕ закрыто по одному снимку
    assert pos.quantity == 10
    assert "F1" in main._external_close_candidates    # кандидат В3 зафиксирован
    assert broker.orders_sent == []                   # обречённых повторных SELL нет
    assert len(read_trades(tmp_path)) == 0
    assert broker.uncertain_state is True

    # --- цикл 2: двойное подтверждение через единый механизм В3 ---
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.exit_price_confirmed is False
    assert broker.orders_sent == []
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_EXTERNAL" in events


def test_defect2_4_uncertain_sell_rejected_absent_waits_v3(env):
    """Сценарий 4: UNCERTAIN с SELL REJECTED (0 из 10), позиция исчезла.
    Отказ без исполнения не доказывает закрытие позиции.
    v2.3.3-stage1: завершённая заявка очищается, но возврат в OPEN —
    ТОЛЬКО после подтверждения совпадения количества брокерским снимком
    (resolve_uncertain); при ОТСУТСТВУЮЩЕЙ позиции подтверждения нет —
    позиция остаётся UNCERTAIN (безопасное состояние: SL/TP не действуют,
    повторная SELL невозможна), отсутствие у брокера — кандидат В3;
    закрытие после двойного подтверждения (цикл 2).
    Прежнее поведение v2.3.2 (немедленный OPEN без сверки) позволяло
    SL/TP отправить обречённую SELL по отсутствующей позиции."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="REJECTED", executed=0)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(2, 0, 0)})  # REJECTED
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: отказ без исполнения -> заявка очищена, позиция остаётся
    # UNCERTAIN (v2.3.3: OPEN только после подтверждения брокером) + кандидат В3 ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN       # возврат в OPEN — только после сверки с брокером
    assert pos.state != PositionState.OPEN            # немедленного повторного открытия нет
    assert pos.close_order_id is None                 # завершённая заявка очищена
    assert pos.quantity == 10
    assert pos.state != PositionState.CLOSED          # мгновенного закрытия нет
    assert "F1" in main._external_close_candidates
    assert broker.orders_sent == []                   # никакого повторного SELL «на удачу»
    assert len(read_trades(tmp_path)) == 0
    assert broker.uncertain_state is True

    # --- цикл 2: двойное подтверждение через В3 ---
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.exit_price_confirmed is False
    assert broker.orders_sent == []
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_EXTERNAL" in events


def test_defect2_5_none_snapshot_neither_confirms_nor_resets(env):
    """Сценарий 5: get_positions()=None между двумя подтверждениями.
    Недостоверный снимок НИ подтверждает, НИ сбрасывает подозрение:
    кандидат В3 сохраняется с прежним first_seen, учёт не меняется; следующий
    ДОСТОВЕРНЫЙ снимок (с разносом >= MIN_AGE от первого) подтверждает В3."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN, close_reason=None)
    broker = FakeBroker(positions={}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: первое обнаружение (кандидат) ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN
    first_seen = main._external_close_candidates["F1"]

    # --- цикл 2: НЕДОСТОВЕРНЫЙ снимок (get_positions()=None) ---
    broker.positions = None
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN       # подтверждения не было
    assert main._external_close_candidates.get("F1") == first_seen  # кандидат не тронут
    assert len(read_trades(tmp_path)) == 0
    assert broker.uncertain_state is True

    # --- цикл 3: снова достоверный снимок без позиции — В3 подтверждает ---
    broker.positions = {}
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert broker.orders_sent == []


def test_defect2_6_restart_between_confirmations_no_instant_close(env):
    """Сценарий 6: рестарт между первым и вторым подтверждением.
    Кандидаты В3 не персистятся (консервативно): после рестарта мгновенного
    закрытия НЕТ — двойное подтверждение начинается заново (два новых
    достоверных снимка), и только затем EXTERNAL_CLOSE."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN, close_reason=None)
    broker = FakeBroker(positions={}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: первое обнаружение до рестарта ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN
    assert "F1" in main._external_close_candidates
    assert main._save_state(monitor, rm) is True      # state.json сохранён перед «рестартом»

    # --- РЕСТАРТ: новый процесс = новый monitor из state.json, кандидаты пусты ---
    monitor2 = ExecutionMonitor(broker)
    monitor2.import_state(json.load(open(tmp_path / "state.json", encoding="utf-8")))
    pos2 = monitor2.positions["F1"]
    assert pos2.state == PositionState.UNCERTAIN      # UNCERTAIN пережил рестарт
    main._external_close_candidates.clear()           # in-memory кандидаты не персистятся
    main.reconcile_done = False

    # --- цикл 2 (после рестарта): мгновенного закрытия НЕТ ---
    run(main.trading_cycle(broker, rm, None, monitor2, ledger))
    assert pos2.state == PositionState.UNCERTAIN      # НЕ закрыт «по памяти» о прошлом снимке
    assert pos2.quantity == 10
    assert "F1" in main._external_close_candidates    # первое обнаружение началось заново
    assert len(read_trades(tmp_path)) == 0
    assert broker.orders_sent == []

    # --- цикл 3: два новых достоверных снимка состоялись — В3 подтверждает ---
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor2, ledger))
    assert pos2.state == PositionState.CLOSED
    assert pos2.close_reason == "EXTERNAL_CLOSE"
    assert pos2.exit_price_confirmed is False
    assert broker.orders_sent == []


def test_defect2_7_repeated_report_after_resolution_idempotent(env):
    """Сценарий 7: повторный отчёт об исполнении после разрешения ситуации.
    Ситуация разрешена терминальным отчётом собственной SELL (FILL 10 лот.,
    реальная цена). Повторные отчёты той же заявки (тот же cumulative
    lots_executed): per-order cursor делает их идемпотентными — «воскрешения»
    нет, повторного применения нет, дубль CSV-строки не пишется."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    record_close_sell(ledger, oid="sell-1", status="INTENT", executed=None)
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="TP", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=2)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 99.9},
                        order_states={"sell-1": FakeOrderState(1, 10, 990)})  # FILL, цена 99.0
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # --- цикл 1: разрешение по терминальному отчёту (реальная цена) ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.quantity == 0
    assert pos.executed_lots == 10
    assert pos.exit_price_confirmed is True
    assert pos.execution_price == pytest.approx(99.0)
    assert len(pos.close_applications) == 1           # одно применение: 0 -> 10
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    assert rows[0]["exit_price"] == "99.0000"
    assert rows[0]["net_pnl_rub"] != ""

    # --- ПОВТОРНЫЕ отчёты той же заявки (тот же cumulative=10) ---
    run(monitor.resolve_closing(pos))
    run(monitor.resolve_closing(pos))
    assert pos.state == PositionState.CLOSED          # «воскрешения» нет
    assert pos.quantity == 0
    assert pos.executed_lots == 10
    assert len(pos.close_applications) == 1           # per-order cursor: дубль не применён
    assert pos.execution_price == pytest.approx(99.0)

    # --- цикл 2: полный цикл после разрешения — изменений и дубля CSV нет ---
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.logged is True
    rows = read_trades(tmp_path)
    assert len(rows) == 1                             # дубль CSV-строки не записан
    assert broker.orders_sent == []                   # никаких новых заявок
    events = [e["event"] for e in read_events(tmp_path)]
    assert events.count("POSITION_CLOSED_BY_LEDGER") == 0
    assert events.count("POSITION_CLOSED_EXTERNAL") == 0
    assert events.count("SELL_NON_TERMINAL_CLOSE_BLOCKED") == 0
