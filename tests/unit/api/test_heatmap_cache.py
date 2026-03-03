"""Unit tests for HeatmapCache in API."""

import time
from src.liquidationheatmap.api.main import HeatmapCache


def test_heatmap_cache_set_get():
    """Should store and retrieve value from cache."""
    cache = HeatmapCache(ttl_seconds=60)
    
    cache.set("BTCUSDT", "2024-01-01", "2024-01-02", "1h", 100.0, "default", {"data": "test"})
    
    result = cache.get("BTCUSDT", "2024-01-01", "2024-01-02", "1h", 100.0, "default")
    assert result == {"data": "test"}
    assert cache._hits == 1
    assert cache._misses == 0


def test_heatmap_cache_expiry():
    """Should not return expired cache entry."""
    # 0 second TTL = immediate expiry
    cache = HeatmapCache(ttl_seconds=0)
    
    cache.set("BTCUSDT", "2024-01-01", "2024-01-02", "1h", 100.0, "default", {"data": "test"})
    
    # Small sleep to ensure time.time() increments if needed, though 0 TTL should expire anyway
    time.sleep(0.01)
    
    result = cache.get("BTCUSDT", "2024-01-01", "2024-01-02", "1h", 100.0, "default")
    assert result is None
    assert cache._misses == 1


def test_heatmap_cache_miss():
    """Should return None on cache miss."""
    cache = HeatmapCache()
    result = cache.get("ETHUSDT", None, None, "1h", 100.0, None)
    assert result is None
    assert cache._misses == 1


def test_heatmap_cache_max_size():
    """Should evict oldest entry when max size reached."""
    cache = HeatmapCache(max_size=2)
    
    cache.set("1", None, None, "1h", 100.0, None, "v1")
    time.sleep(0.01)
    cache.set("2", None, None, "1h", 100.0, None, "v2")
    time.sleep(0.01)
    cache.set("3", None, None, "1h", 100.0, None, "v3") # Should evict "1"
    
    assert cache.get("1", None, None, "1h", 100.0, None) is None
    assert cache.get("2", None, None, "1h", 100.0, None) == "v2"
    assert cache.get("3", None, None, "1h", 100.0, None) == "v3"
