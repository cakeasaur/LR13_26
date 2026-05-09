from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from autoscaler import Autoscaler
from docker.errors import NotFound


def make_scaler(pending_value=None):
    mock_redis = AsyncMock()
    mock_redis.get.return_value = pending_value
    mock_redis.set = AsyncMock()
    with patch("autoscaler.docker.from_env"):
        scaler = Autoscaler(mock_redis)
    return scaler, mock_redis


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

    alive_c = MagicMock()
    alive_c.status = "running"

    dead_c = MagicMock()
    dead_c.status = "exited"

    scaler._docker.containers.get.side_effect = (
        lambda cid: alive_c if cid == "alive-id" else dead_c
    )
    scaler._extra["blocker"] = ["alive-id", "dead-id"]

    result = scaler._alive("blocker")

    assert result == ["alive-id"]
    assert scaler._extra["blocker"] == ["alive-id"]


@pytest.mark.asyncio
async def test_tick_scale_up_when_pending_high():
    scaler, mock_redis = make_scaler("10")  # pending > SCALE_UP_THRESHOLD(5)
    mock_redis.set = AsyncMock()

    with patch.object(scaler, "_spawn") as mock_spawn:
        await scaler.tick()
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

    for name in scaler._extra:
        scaler._extra[name] = ["fake-container-id"]

    with patch.object(scaler, "_alive", return_value=["fake-container-id"]), \
         patch.object(scaler, "_kill_one") as mock_kill:
        await scaler.tick()
        assert mock_kill.call_count == 4


def test_shutdown_terminates_alive_processes():
    scaler, _ = make_scaler()

    container_mock = MagicMock()
    scaler._docker.containers.get.return_value = container_mock
    scaler._extra["blocker"] = ["fake-container-id"]

    scaler.shutdown()
    container_mock.stop.assert_called_once()
