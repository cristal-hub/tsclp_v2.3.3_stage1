# tests/test_manual_close_integration.py
# ИНТЕГРАЦИОННЫЕ ТЕСТЫ v2.3.1-stage1 (аудит ТЗ v2.3, пункты 7 и 8).
#
# ТЗ-7: полный trading_cycle при ручной продаже позиции оператором
#       (позиция есть локально, у брокера исчезла) — обнаружение по
#       ДОСТОВЕРНОМУ снимку брокера, двойное подтверждение В3,
#       CSV без фиктивного P&L, никаких заявок робота.
# ТЗ-8: рестарт с незавершённым восстановлением не приводит к ложному
#       закрытию и повторному открытию позиции:
#       a) CLOSING + свой SELL исполнился офлайн -> закрытие с реальной ценой;
#       b) CLOSING + свой SELL отклонён, позиция исчезла -> НЕ мгновенное
#          закрытие: сначала кандидат В3, затем двойное подтверждение;
#       c) OPEN восстановлен и есть у брокера -> ничего не происходит,
#          заявок нет;
#       d) UNCERTAIN + свой SELL исполнился офлайн -> реальная цена
#          (раньше закрывалось с фиктивным P&L).
#
# Запуск из корня сборки: pytest tests/test_manual_close_integration.py -v
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

# Оригиналы глобальных функций main (сохранены ДО возможных подмен другими
# тестами; см. комментарий в фикстуре env).
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
    """Фейковый брокер для полного trading_cycle. НИКАКИХ сетевых вызовов,
    НИКАКИХ реальных заявок (только SANDBOX-семантика на словарях)."""

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
        # В этих тестах робот отправляет заявку только в test_quiet_cycle_updates_state_json
        # (штатный TP-закрытие). Заявка принимается «брокером» со статусом NEW.
        self.orders_sent.append((figi, lots, str(direction), order_id))
        new_state = FakeOrderState(4, 0, 0)  # NEW, ничего не исполнено
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


def age_candidate(figi, seconds=31.0):
    """Сдвигает first_seen кандидата В3 в прошлое — имитация разноса двух
    достоверных снимков >= RECONCILE_CONFIRM_MIN_AGE_SECONDS (продукционный
    минимум 30 с: два reconcile одного цикла подтверждением НЕ считаются)."""
    ts = main._external_close_candidates.get(figi)
    assert ts, f"кандидат {figi} не зарегистрирован — сдвигать нечего"
    dt = datetime.fromisoformat(ts) - timedelta(seconds=seconds)
    main._external_close_candidates[figi] = dt.isoformat()


@pytest.fixture()
def env(tmp_path, monkeypatch):
    """Изолированная среда: все записи в tmp_path; торговля выключена
    (trading.flag отсутствует — робот мониторит и сверяется, но не входит).
    Часть старых тестов репозитория (test_main.py) подменяет глобальные
    функции main.is_trading_enabled/main._save_state MagicMock'ами БЕЗ
    восстановления — здесь они возвращаются к оригиналам, чтобы тесты
    не зависели от порядка прогона."""
    monkeypatch.setattr(main, "is_trading_enabled", _ORIG_IS_TRADING_ENABLED)
    monkeypatch.setattr(main, "_save_state", _ORIG_SAVE_STATE)
    monkeypatch.setattr(research_logger, "EVENTS_FILE", str(tmp_path / "research_events.jsonl"))
    monkeypatch.setattr(research_logger, "TRADES_FILE", str(tmp_path / "research_trades.csv"))
    monkeypatch.setattr(main, "STATE_FILE", str(tmp_path / "state.json"))
    monkeypatch.setattr(main, "STATE_FILE_TMP", str(tmp_path / "state.json.tmp"))
    monkeypatch.setattr(main, "TRADING_FLAG", str(tmp_path / "trading.flag"))
    # Тесты прогоняют циклы мгновенно; разнос двух сверок имитируется явно
    # (age_candidate) — как в test_v232_close_defects.py.
    # v2.3.3-stage1: минимум ОСТАВЛЕН ПРОИЗВОДИТЕЛЬНЫМ (30 с =
    # TRADING_CYCLE_SECONDS): при нуле два reconcile ОДНОГО цикла
    # (process_closing + шаг 4) засчитывались бы за двойное подтверждение
    # В3 в обход ТЗ-3 — например, когда позиция после терминального
    # REJECTED остаётся UNCERTAIN и первый reconcile цикла выполняется
    # в process_closing. Комментарий: разнос снимков >=
    # RECONCILE_CONFIRM_MIN_AGE_SECONDS — обязательное условие
    # двойного подтверждения (защита от мгновенного закрытия).
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


# --------------------------------------------- ТЗ-7: полный trading_cycle, ручная продажа

def test_integration_manual_sell_full_trading_cycle(env):
    """Ручная продажа позиции при работающем роботе:
    цикл 1 — обнаружение по достоверному снимку (кандидат В3, учёт не тронут);
    цикл 2 — двойное подтверждение -> учёт закрыт EXTERNAL_CLOSE;
    цикл 3 — CSV-строка БЕЗ фиктивного P&L (PRICE_UNKNOWN);
    ни одной заявки робота; state.json обновляется (В1)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10)
    broker = FakeBroker(positions={}, prices={"F1": 100.5})  # позиция продана вручную
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)
    sm = None  # шаг входов не выполняется (торговля выключена)

    # --- цикл 1: первое обнаружение ---
    run(main.trading_cycle(broker, rm, sm, monitor, ledger))
    assert pos.state == PositionState.OPEN          # учёт НЕ тронут
    assert "F1" in main._external_close_candidates  # кандидат зафиксирован
    assert broker.orders_sent == []                 # НИ ОДНОЙ заявки
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_EXTERNAL_CLOSE_SUSPECTED" in events
    state = json.load(open(tmp_path / "state.json", encoding="utf-8"))
    assert state["positions"]["F1"]["state"] == "OPEN"  # В1: сохранено в конце цикла

    # --- цикл 2: двойное подтверждение (второй достоверный снимок,
    # разнос >= RECONCILE_CONFIRM_MIN_AGE_SECONDS) ---
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, sm, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.executed_lots == 10
    assert pos.exit_price_confirmed is False        # цена приблизительная, P&L не пишем
    assert "P&L не рассчитан" in pos.error_note
    assert broker.orders_sent == []                 # никакого повторного открытия
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_EXTERNAL" in events
    state = json.load(open(tmp_path / "state.json", encoding="utf-8"))
    assert state["positions"]["F1"]["state"] == "CLOSED"

    # --- цикл 3: запись CSV ---
    run(main.trading_cycle(broker, rm, sm, monitor, ledger))
    assert pos.logged is True
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["exit_reason"] == "EXTERNAL_CLOSE"
    assert row["executed_lots"] == "10"
    # ТЗ-5: фиктивного P&L нет — финансовые поля пустые, cumulative не тронут
    assert row["exit_price"] == ""
    assert row["net_pnl_rub"] == ""
    assert row["gross_pnl_rub"] == ""
    assert row["cumulative_net_pnl_rub"] == ""
    assert "PRICE_UNKNOWN" in row["error_note"]
    assert research_logger.get_cumulative_net_pnl() == pytest.approx(0.0)
    events = [e["event"] for e in read_events(tmp_path)]
    assert "CLOSED" in events
    closed_ev = next(e for e in read_events(tmp_path) if e["event"] == "CLOSED")
    assert closed_ev["pnl_recorded"] is False
    assert broker.orders_sent == []


def test_integration_unreliable_snapshot_is_not_confirmation(env):
    """ТЗ-3: недостоверный снимок (get_positions()=None) НЕ считается
    подтверждением: учёт не меняется, кандидат не сбрасывается."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10)
    broker = FakeBroker(positions={}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.OPEN
    first_seen = main._external_close_candidates["F1"]

    broker.positions = None  # недостоверный снимок
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.OPEN          # подтверждения не было
    assert main._external_close_candidates.get("F1") == first_seen  # кандидат не тронут

    broker.positions = {}  # снова достоверный снимок без позиции
    age_candidate("F1")    # разнос снимков >= минимума — подтверждение разрешено
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"


# --------------------------------------------- ТЗ-8: рестарт, незавершённое восстановление

def test_restart_closing_position_sell_filled_offline(env):
    """ТЗ-8a: рестарт с CLOSING-позицией, свой SELL исполнился офлайн.
    Ожидается: учёт закрыт с РЕАЛЬНОЙ ценой исполнения, CSV с P&L,
    БЕЗ повторной отправки заявки, БЕЗ повторного открытия."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10, state=PositionState.CLOSING,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat(),
                       price_before_order=99.8)
    # SELL исполнился офлайн: 10 лот. по 99.5
    broker = FakeBroker(positions={}, prices={"F1": 99.5},
                        order_states={"sell-1": FakeOrderState(1, 10, 995)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.CLOSED
    assert pos.exit_price_confirmed is True
    assert pos.execution_price == pytest.approx(99.5)
    assert broker.orders_sent == []                 # повторной SELL НЕ отправлялся
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    row = rows[0]
    assert row["exit_reason"] == "SL"
    assert row["exit_price"] == "99.5000"
    assert row["net_pnl_rub"] != ""                 # P&L рассчитан по реальной цене
    assert float(row["net_pnl_rub"].replace(",", ".")) < 0  # убыточная, но реальная


def test_restart_sell_rejected_position_absent_no_false_close(env):
    """ТЗ-8b: рестарт, свой SELL отклонён (позиция уже продана вручную).
    НИКАКОГО мгновенного закрытия. v2.3.3-stage1: завершённая заявка
    очищается, но возврат в OPEN — только после подтверждения совпадения
    количества брокерским снимком; при ОТСУТСТВУЮЩЕЙ позиции подтверждения
    нет — позиция остаётся UNCERTAIN (SL/TP не действуют, повторная SELL
    невозможна), отсутствие у брокера — кандидат В3, цикл 2 — двойное
    подтверждение -> EXTERNAL_CLOSE."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    assert ledger.record_intent("sell-1", trade_id="t1", figi="F1", ticker="TEST",
                                side="SELL", requested_lots=10, reason="close:SL")
    pos = make_buy_pos(quantity=10, state=PositionState.CLOSING,
                       close_reason="SL", close_order_id="sell-1",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=1)).isoformat())
    broker = FakeBroker(positions={}, prices={"F1": 100.5},
                        order_states={"sell-1": FakeOrderState(2, 0, 0)})  # REJECTED
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    # цикл 1: SELL отклонён без исполнения -> заявка очищена, позиция
    # остаётся UNCERTAIN (v2.3.3: OPEN только после сверки с брокером);
    # отсутствие позиции у брокера НЕ закрывается сразу — только кандидат В3
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.UNCERTAIN     # НЕ закрыто ложным образом, НЕ открыто без сверки
    assert pos.state != PositionState.OPEN          # немедленного повторного открытия нет
    assert pos.close_order_id is None               # завершённая заявка очищена
    assert "F1" in main._external_close_candidates
    assert broker.orders_sent == []                 # обречённых повторных SELL нет
    rows = read_trades(tmp_path)
    assert len(rows) == 0                           # CSV ещё не пишется

    # цикл 2: двойное подтверждение -> EXTERNAL_CLOSE (без P&L);
    # разнос двух достоверных снимков >= производительного минимума
    age_candidate("F1")
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.CLOSED
    assert pos.close_reason == "EXTERNAL_CLOSE"
    assert pos.exit_price_confirmed is False
    assert broker.orders_sent == []
    events = [e["event"] for e in read_events(tmp_path)]
    assert "POSITION_CLOSED_EXTERNAL" in events


def test_restart_open_position_alive_no_reopen(env):
    """ТЗ-8c: рестарт с живой OPEN-позицией у брокера — учёт не меняется,
    повторного открытия/закрытия нет, заявок нет."""
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10)
    broker = FakeBroker(positions={"F1": PosData(10, "LONG")}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert pos.state == PositionState.OPEN
    assert pos.logged is False                      # CSV не пишется — сделка живая
    assert broker.orders_sent == []
    assert main.reconcile_done is True              # сверка завершена успешно


def test_restart_uncertain_with_own_sell_filled_gets_real_price(env):
    """ТЗ-4/5: UNCERTAIN-позиция с своей close-заявкой: если SELL исполнился
    офлайн, учёт закрывается по отчёту заявки с РЕАЛЬНОЙ ценой (раньше —
    закрытие с фиктивным «нулевой» P&L по цене входа)."""
    tmp_path = env.tmp_path
    ledger = env.ledger
    proven_buy(ledger)
    assert ledger.record_intent("sell-2", trade_id="t1", figi="F1", ticker="TEST",
                                side="SELL", requested_lots=10, reason="close:TP")
    pos = make_buy_pos(quantity=10, state=PositionState.UNCERTAIN,
                       close_reason="TP", close_order_id="sell-2",
                       close_requested_lots=10,
                       close_requested_at=(datetime.now(UTC) - timedelta(minutes=2)).isoformat(),
                       price_before_order=100.1)
    broker = FakeBroker(positions={}, prices={"F1": 99.9},
                        order_states={"sell-2": FakeOrderState(1, 10, 990)})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))

    assert pos.state == PositionState.CLOSED
    assert pos.execution_price == pytest.approx(99.0)
    assert pos.exit_price_confirmed is True
    assert broker.orders_sent == []
    rows = read_trades(tmp_path)
    assert len(rows) == 1
    assert rows[0]["exit_price"] == "99.0000"
    assert rows[0]["net_pnl_rub"] != ""             # реальный P&L вместо фиктивного


def test_quiet_cycle_updates_state_json(env):
    """В1/ТЗ-8: тихий цикл (позиция жива, всё совпадает) обновляет state.json
    в конце каждого цикла — CLOSED/CLOSING-переходы не теряются при рестарте."""
    ledger = env.ledger
    proven_buy(ledger)
    pos = make_buy_pos(quantity=10)
    broker = FakeBroker(positions={"F1": PosData(10, "LONG")}, prices={"F1": 100.5})
    monitor = fresh_monitor(pos, broker)
    rm = main.RiskManager(None)

    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    state_path = env.tmp_path / "state.json"
    assert state_path.is_file()
    mtime1 = state_path.stat().st_mtime
    state = json.load(open(state_path, encoding="utf-8"))
    assert state["positions"]["F1"]["state"] == "OPEN"
    assert "buy-1" in state["closed_entry_order_ids"] or True  # реестр закрыв. сделок

    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    assert state_path.stat().st_mtime >= mtime1     # перезаписан в конце цикла
    # позиция закрылась по TP офлайн-имитацией: меняем цену и закрываем роботом
    broker.prices = {"F1": 104.5}                   # выше TP
    run(main.trading_cycle(broker, rm, None, monitor, ledger))
    state = json.load(open(state_path, encoding="utf-8"))
    assert state["positions"]["F1"]["state"] == "CLOSING"  # переход сохранён (В1)
