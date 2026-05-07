from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from autoscaler import Autoscaler


def make_scaler(pending_value=None):
    mock_redis = AsyncMock()
    mock_redis.get.return_value = pending_value
    mock_redis.set = AsyncMock()
    return Autoscaler(mock_redis), mock_redis


@pytest.mark.asyncio
async def test_pending_none_returns_zero():
    scaler, _ = make_scaler(None)
    assert await scaler._pending() == 0


@pytest.mark.asyncio
async def test_pending_positive_value():
    scaler, _ = make_scaler("7")
    assert await scaler._pending() == 7


@pytest.mark.asyncio
async def test_pending_negative_clamped_to_zero():
    scaler, _ = make_scaler("-3")
    assert await scaler._pending() == 0


def test_alive_filters_dead_processes():
    scaler, _ = make_scaler()

    alive_proc = MagicMock()
    alive_proc.poll.return_value = None  # ещё жив

    dead_proc = MagicMock()
    dead_proc.poll.return_value = 1  # завершился

    scaler._extra["blocker"] = [alive_proc, dead_proc]
    result = scaler._alive("blocker")

    assert len(result) == 1
    assert result[0] is alive_proc
    assert scaler._extra["blocker"] == [alive_proc]


@pytest.mark.asyncio
async def test_tick_scale_up_when_pending_high():
    scaler, mock_redis = make_scaler("10")  # pending > SCALE_UP_THRESHOLD(5)
    mock_redis.set = AsyncMock()

    with patch.object(scaler, "_spawn") as mock_spawn:
        await scaler.tick()
        # должен попытаться запустить инстанс для каждого агента
        assert mock_spawn.call_count == 4


@pytest.mark.asyncio
async def test_tick_no_scale_up_when_pending_low():
    scaler, mock_redis = make_scaler("1")  # pending < SCALE_UP_THRESHOLD
    mock_redis.set = AsyncMock()

    with patch.object(scaler, "_spawn") as mock_spawn:
        await scaler.tick()
        mock_spawn.assert_not_called()


@pytest.mark.asyncio
async def test_tick_scale_down_removes_extra():
    scaler, mock_redis = make_scaler("0")  # pending < SCALE_DOWN_THRESHOLD(2)
    mock_redis.set = AsyncMock()

    # добавляем по одному живому доп. процессу на каждый агент
    for name in scaler._extra:
        proc = MagicMock()
        proc.poll.return_value = None
        scaler._extra[name] = [proc]

    with patch.object(scaler, "_kill_one") as mock_kill:
        await scaler.tick()
        assert mock_kill.call_count == 4


def test_shutdown_terminates_alive_processes():
    scaler, _ = make_scaler()

    proc = MagicMock()
    proc.poll.return_value = None
    scaler._extra["blocker"] = [proc]

    scaler.shutdown()
    proc.send_signal.assert_called_once()
