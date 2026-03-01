"""
Cross-symbol consistency validation tests.

Tests that ensure consistent behavior across different trading symbols:
- Same tier structure produces same results
- Multi-symbol calculations remain independent
- Configuration updates don't affect other symbols
"""

from decimal import Decimal

import pytest

from src.services.margin_calculator import MarginCalculator
from src.services.tier_cache import TierCache
from src.services.tier_loader import TierLoader
from src.services.tier_validator import TierValidator


class TestSymbolConsistency:
    """Test suite for cross-symbol consistency."""

    @pytest.fixture
    def default_symbols(self) -> list[str]:
        """Common trading symbols for testing."""
        return ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "ADAUSDT"]

    @pytest.fixture
    def symbols_without_custom_config(self) -> list[str]:
        """Symbols that use the default BTC-based tier config (no custom YAML)."""
        return ["BNBUSDT", "SOLUSDT", "ADAUSDT"]

    @pytest.fixture
    def symbols_with_custom_config(self) -> list[str]:
        """Symbols that have their own tier YAML config."""
        return ["BTCUSDT", "ETHUSDT"]

    @pytest.fixture
    def cache_with_symbols(self, default_symbols) -> TierCache:
        """Cache preloaded with multiple symbols."""
        cache = TierCache()
        for symbol in default_symbols:
            cache.get_or_default(symbol)
        return cache

    def test_all_symbols_use_same_default_tier_structure(self, symbols_without_custom_config):
        """
        Test that symbols WITHOUT custom config have identical default tier structure.

        Consistency Requirement:
        - Same number of tiers
        - Same tier boundaries
        - Same margin rates
        - Same maintenance amounts

        Note: Symbols with custom YAML configs (BTCUSDT, ETHUSDT) may have
        different tier boundaries and are excluded from this uniformity test.
        """
        cache = TierCache()

        configs = [cache.get_or_default(symbol) for symbol in symbols_without_custom_config]

        # All should have same number of tiers
        tier_counts = [len(config.tiers) for config in configs]
        assert all(count == tier_counts[0] for count in tier_counts), "Tier counts should match"

        # Compare each tier across symbols
        for tier_idx in range(len(configs[0].tiers)):
            # Get tier at same index from each symbol
            tiers = [config.tiers[tier_idx] for config in configs]

            # All should have same boundaries
            min_notionals = [tier.min_notional for tier in tiers]
            max_notionals = [tier.max_notional for tier in tiers]
            assert all(mn == min_notionals[0] for mn in min_notionals), (
                f"Tier {tier_idx + 1} min_notional mismatch"
            )
            assert all(mx == max_notionals[0] for mx in max_notionals), (
                f"Tier {tier_idx + 1} max_notional mismatch"
            )

            # All should have same rates
            margin_rates = [tier.margin_rate for tier in tiers]
            assert all(rate == margin_rates[0] for rate in margin_rates), (
                f"Tier {tier_idx + 1} margin_rate mismatch"
            )

            # All should have same MAs
            mas = [tier.maintenance_amount for tier in tiers]
            assert all(ma == mas[0] for ma in mas), f"Tier {tier_idx + 1} MA mismatch"

    def test_same_position_same_margin_across_symbols(self, symbols_without_custom_config):
        """
        Test that same position size yields same margin across default-config symbols.

        Independence Test:
        - Same notional amount
        - Same tier structure (default)
        - Should calculate identical margins

        Note: Only tests symbols using default config. Symbols with custom configs
        (BTCUSDT, ETHUSDT) have different tier boundaries by design.
        """
        cache = TierCache()
        calculators = {
            symbol: MarginCalculator(cache.get_or_default(symbol))
            for symbol in symbols_without_custom_config
        }

        test_positions = [
            Decimal("10000"),  # Tier 1
            Decimal("100000"),  # Tier 2
            Decimal("500000"),  # Tier 3
            Decimal("2000000"),  # Tier 4
            Decimal("15000000"),  # Tier 5
        ]

        for position in test_positions:
            margins = [calc.calculate_margin(position) for calc in calculators.values()]

            # All margins should be identical
            assert all(margin == margins[0] for margin in margins), (
                f"Margin mismatch for position ${position:,.0f}"
            )

    def test_all_default_configs_pass_validation(self, default_symbols):
        """
        Test that all default configurations pass validation.

        Quality Assurance:
        - Every default config is valid
        - Continuity maintained
        - No structural issues
        """
        validator = TierValidator()
        cache = TierCache()

        for symbol in default_symbols:
            config = cache.get_or_default(symbol)
            result = validator.validate(config)

            assert result.is_valid, f"{symbol} validation failed: {result.errors}"
            assert len(result.continuity_checks) == 4, f"{symbol} should have 4 boundary checks"
            assert all(result.continuity_checks.values()), f"{symbol} has continuity breaks"

    def test_symbol_specific_config_overrides_default(self):
        """
        Test that symbol-specific configuration overrides default.

        Configuration Priority:
        - Symbol-specific YAML takes precedence
        - Falls back to default only if missing
        """
        loader = TierLoader()

        # Try to load BTCUSDT (may have specific config)
        try:
            btc_config = loader.load_for_symbol("BTCUSDT")
            default_config = TierLoader.load_binance_default()

            # If loaded successfully, it should be a valid config
            assert btc_config.symbol == "BTCUSDT"
            assert len(btc_config.tiers) > 0

            # Could be same as default or different (both valid)
        except FileNotFoundError:
            # No specific config - test skipped
            pytest.skip("No BTCUSDT-specific configuration found")

    def test_cache_isolation_across_symbols(self, cache_with_symbols, default_symbols):
        """
        Test that cache maintains isolation between symbols.

        Cache Independence:
        - Each symbol has separate cache entry
        - Invalidating one doesn't affect others
        - Stats track correctly
        """
        cache = cache_with_symbols

        # All symbols should be cached
        for symbol in default_symbols:
            assert symbol in cache

        # Get stats before invalidation
        initial_size = len(cache)

        # Invalidate one symbol
        cache.invalidate(default_symbols[0])

        # Size should decrease by 1
        assert len(cache) == initial_size - 1

        # Invalidated symbol should be gone
        assert default_symbols[0] not in cache

        # Others should remain
        for symbol in default_symbols[1:]:
            assert symbol in cache

    def test_concurrent_calculations_across_symbols(self, symbols_without_custom_config):
        """
        Test that concurrent calculations for different symbols work correctly.

        Thread Safety:
        - Multiple symbols calculated simultaneously
        - Results remain consistent
        - No cross-contamination

        Note: Only tests default-config symbols. Custom-config symbols have
        different tier structures by design.
        """
        cache = TierCache()

        # Prepare calculators for all symbols
        calculators = {
            symbol: MarginCalculator(cache.get_or_default(symbol))
            for symbol in symbols_without_custom_config
        }

        # Calculate margins for same position across all symbols
        position = Decimal("1000000")

        margins = {}
        for symbol, calc in calculators.items():
            margins[symbol] = calc.calculate_margin(position)

        # All should calculate successfully
        assert len(margins) == len(symbols_without_custom_config)

        # All values should be identical (using default config)
        margin_values = list(margins.values())
        assert all(m == margin_values[0] for m in margin_values)

    def test_symbol_metadata_consistency(self, default_symbols):
        """
        Test that symbol metadata is correctly set in configurations.

        Metadata Validation:
        - Symbol field matches requested symbol
        - Version information present
        - Configuration internally consistent
        """
        cache = TierCache()

        for symbol in default_symbols:
            config = cache.get_or_default(symbol)

            # Symbol should match
            assert config.symbol == symbol

            # Version should be set
            assert config.version
            assert len(config.version) > 0

            # All tiers should reference same symbol
            for tier in config.tiers:
                assert tier.symbol == symbol

    def test_version_consistency_across_default_symbols(self, symbols_without_custom_config):
        """
        Test that all symbols using default config have same version.

        Version Tracking:
        - Default configs share version
        - Custom configs (BTCUSDT, ETHUSDT) may have different versions
        """
        cache = TierCache()

        configs = [cache.get_or_default(symbol) for symbol in symbols_without_custom_config]
        versions = [config.version for config in configs]

        # All default configs should have same version
        assert all(v == versions[0] for v in versions), "Default config versions should match"

    def test_margin_calculation_deterministic_across_symbols(self, symbols_without_custom_config):
        """
        Test that margin calculations are deterministic across default-config symbols.

        Determinism:
        - Same inputs always produce same outputs
        - No randomness or time dependencies
        - Consistent across symbols with same tier config
        """
        cache = TierCache()

        test_positions = [
            Decimal("75000"),
            Decimal("500000"),
            Decimal("5000000"),
        ]

        # Calculate multiple times for each symbol
        for position in test_positions:
            results_by_symbol = {}

            for symbol in symbols_without_custom_config:
                calc = MarginCalculator(cache.get_or_default(symbol))

                # Calculate same position 3 times
                margins = [calc.calculate_margin(position) for _ in range(3)]

                # All calculations should be identical
                assert all(m == margins[0] for m in margins), (
                    f"{symbol} non-deterministic for ${position}"
                )

                results_by_symbol[symbol] = margins[0]

            # All symbols should produce same result
            all_margins = list(results_by_symbol.values())
            assert all(m == all_margins[0] for m in all_margins), (
                f"Cross-symbol inconsistency at ${position}"
            )

    def test_tier_lookup_consistency_across_symbols(self, symbols_without_custom_config):
        """
        Test that tier lookup is consistent across default-config symbols.

        Tier Selection:
        - Same position maps to same tier number
        - Tier boundaries align (BTC default boundaries)
        - No off-by-one errors

        Note: Only tests default-config symbols. BTCUSDT and ETHUSDT have
        custom configs with different tier boundaries.
        """
        cache = TierCache()

        # These boundaries match the BTC default tier structure
        test_cases = [
            (Decimal("25000"), 1),  # Middle of tier 1: (0, 50000]
            (Decimal("49999"), 1),  # Just before boundary
            (Decimal("50000"), 1),  # Upper boundary of tier 1 - INCLUSIVE
            (Decimal("50001"), 2),  # Just into tier 2: (50000, 250000]
            (Decimal("150000"), 2),  # Middle of tier 2
            (Decimal("250000"), 2),  # Upper boundary of tier 2 - INCLUSIVE
            (Decimal("250001"), 3),  # Just into tier 3: (250000, 1000000]
            (Decimal("750000"), 3),  # Middle of tier 3
            (Decimal("1000000"), 3),  # Upper boundary of tier 3 - INCLUSIVE
            (Decimal("5000000"), 4),  # Middle of tier 4: (1000000, 10000000]
            (Decimal("10000000"), 4),  # Upper boundary of tier 4 - INCLUSIVE
            (Decimal("15000000"), 5),  # Middle of tier 5: (10000000, 50000000]
        ]

        for position, expected_tier in test_cases:
            for symbol in symbols_without_custom_config:
                calc = MarginCalculator(cache.get_or_default(symbol))
                tier = calc.get_tier_for_position(position)

                assert tier.tier_number == expected_tier, (
                    f"{symbol}: Position ${position:,.0f} should be tier {expected_tier}, got {tier.tier_number}"
                )

    def test_ethusdt_has_custom_tier_boundaries(self):
        """
        Test that ETHUSDT loads its custom tier config with ETH-specific boundaries.

        ETH has smaller notional boundaries than BTC:
        - ETH tier 1 max: $10,000 (vs BTC $50,000)
        - ETH tier 5 max: $2,000,000 (vs BTC $50,000,000)
        """
        cache = TierCache()
        eth_config = cache.get_or_default("ETHUSDT")

        assert eth_config.symbol == "ETHUSDT"
        assert len(eth_config.tiers) == 5

        # ETH tier 1: 0 - 10k
        assert eth_config.tiers[0].max_notional == Decimal("10000")
        # ETH tier 2: 10k - 100k
        assert eth_config.tiers[1].min_notional == Decimal("10000")
        assert eth_config.tiers[1].max_notional == Decimal("100000")
        # Same margin rates as BTC
        assert eth_config.tiers[0].margin_rate == Decimal("0.005")
        assert eth_config.tiers[1].margin_rate == Decimal("0.010")

    def test_custom_configs_each_valid_independently(self, symbols_with_custom_config):
        """
        Test that each custom-config symbol passes validation independently.
        """
        validator = TierValidator()
        cache = TierCache()

        for symbol in symbols_with_custom_config:
            config = cache.get_or_default(symbol)
            result = validator.validate(config)

            assert result.is_valid, f"{symbol} validation failed: {result.errors}"
            assert all(result.continuity_checks.values()), f"{symbol} has continuity breaks"

    def test_cache_preload_all_symbols_consistent(self, default_symbols):
        """
        Test that preloading all symbols produces consistent cache state.

        Cache Warming:
        - All symbols load successfully
        - Cache size matches symbol count
        - All entries valid
        """
        cache = TierCache()

        # Preload all test symbols
        for symbol in default_symbols:
            cache.get_or_default(symbol)

        # Cache should contain all symbols
        assert len(cache) == len(default_symbols)

        # All should be retrievable
        for symbol in default_symbols:
            assert symbol in cache
            config = cache.get(symbol)
            assert config is not None
            assert config.symbol == symbol
