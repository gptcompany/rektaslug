"""Unit tests for retry logic utility."""

import time
import pytest
from unittest.mock import MagicMock

from src.liquidationheatmap.utils.retry import retry_on_error


def test_retry_on_error_success_first_attempt():
    """Should return result immediately if first attempt succeeds."""
    func = MagicMock(return_value="success")
    result = retry_on_error(func)
    
    assert result == "success"
    assert func.call_count == 1


def test_retry_on_error_success_after_failure(monkeypatch):
    """Should retry and eventually succeed after a failure."""
    # Mock time.sleep to speed up tests
    monkeypatch.setattr(time, "sleep", lambda x: None)
    
    func = MagicMock()
    # First attempt fails, second succeeds
    func.side_effect = [Exception("failure"), "success"]
    
    result = retry_on_error(func, max_attempts=3)
    
    assert result == "success"
    assert func.call_count == 2


def test_retry_on_error_all_attempts_fail(monkeypatch):
    """Should raise the last exception if all attempts fail."""
    monkeypatch.setattr(time, "sleep", lambda x: None)
    
    func = MagicMock(side_effect=Exception("permanent failure"))
    
    with pytest.raises(Exception, match="permanent failure"):
        retry_on_error(func, max_attempts=3)
        
    assert func.call_count == 3


def test_retry_on_error_exponential_backoff(monkeypatch):
    """Should use exponential backoff for sleep times."""
    sleep_times = []
    monkeypatch.setattr(time, "sleep", lambda x: sleep_times.append(x))
    
    func = MagicMock(side_effect=Exception("failure"))
    
    with pytest.raises(Exception):
        retry_on_error(func, max_attempts=4, backoff_seconds=1.0)
        
    # Sleep times: 1s, 2s, 4s (total 3 sleeps for 4 attempts)
    assert sleep_times == [1.0, 2.0, 4.0]
