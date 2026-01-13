#!/usr/bin/env python3
"""
S&P 500 Hybrid Consensus Reconstruction Engine
===============================================
A robust engine that reconstructs historical S&P 500 index composition
using a "Hybrid Consensus" approach to solve for data gaps in the EODHD API.

Usage:
    python sp500_engine.py YOUR_API_KEY       # Live mode with API
    python sp500_engine.py demo               # Demo mode with simulated data
"""

import json
import sys
import time
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
from dataclasses import dataclass, field, asdict
from enum import Enum
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib

try:
    import pandas as pd
    import numpy as np
    import requests
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False
    print("Warning: Optional dependencies not installed. Run: pip install -r requirements.txt")


# =============================================================================
# ENUMS AND CONSTANTS
# =============================================================================

class ConflictResolution(Enum):
    """Types of conflict resolution strategies."""
    BIAS_ACTION = "bias_towards_action"
    BIAS_PRESENCE = "bias_towards_presence"
    UNION_FALLBACK = "union_fallback"


class DataSource(Enum):
    """Data source indicators."""
    MASTER_LIST = "master_list"
    BACKWARD_ROLL = "backward_roll"
    BOTH = "both"
    UNION_FALLBACK = "union_fallback"


# =============================================================================
# DATA CLASSES
# =============================================================================

@dataclass
class ConstituentRecord:
    """Represents an S&P 500 constituent with all relevant data."""
    ticker: str
    name: str = ""
    sector: str = "Unknown"
    industry: str = ""
    market_cap: Optional[float] = None
    shares_outstanding: Optional[float] = None
    close_price: Optional[float] = None
    weight: float = 0.0
    source: str = ""
    membership_days: int = 0
    market_cap_estimated: bool = False


@dataclass
class ChangeEvent:
    """Represents an add/remove event in S&P 500 composition."""
    date: str
    ticker: str
    action: str  # 'add' or 'remove'
    name: str = ""
    reason: str = ""


@dataclass
class DailyIndexRecord:
    """Snapshot of S&P 500 composition for a specific date."""
    date: str
    source_agreement: str  # Percentage string like "99.8%"
    agreement_value: float = 0.0  # Numeric value
    conflict_tickers: List[str] = field(default_factory=list)
    final_count: int = 0
    sectors: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    constituents: Dict[str, Dict] = field(default_factory=dict)
    resolution_stats: Dict[str, int] = field(default_factory=dict)
    method_a_count: int = 0
    method_b_count: int = 0


@dataclass
class DataQualityMetrics:
    """Comprehensive metrics for data quality reporting."""
    total_dates_processed: int = 0
    dates_with_conflicts: int = 0
    conflict_free_dates: int = 0
    avg_source_agreement: float = 0.0
    min_source_agreement: float = 100.0
    max_source_agreement: float = 0.0
    std_source_agreement: float = 0.0
    total_conflicts_resolved: int = 0
    conflicts_by_resolution: Dict[str, int] = field(default_factory=dict)
    top_conflict_tickers: List[Tuple[str, int]] = field(default_factory=list)
    missing_market_cap_count: int = 0
    avg_constituent_count: float = 0.0
    min_constituent_count: int = 0
    max_constituent_count: int = 0
    sector_coverage: Dict[str, int] = field(default_factory=dict)


# =============================================================================
# EODHD API CLIENT
# =============================================================================

class EODHDClient:
    """
    EODHD API Client with rate limiting, caching, and error recovery.
    """

    def __init__(self, api_key: str, cache_ttl: int = 3600):
        """
        Initialize the EODHD API client.

        Args:
            api_key: EODHD API key
            cache_ttl: Cache time-to-live in seconds (default: 1 hour)
        """
        self.api_key = api_key
        self.base_url = "https://eodhd.com/api"
        self.cache_ttl = cache_ttl
        self._cache: Dict[str, Tuple[float, Any]] = {}
        self._rate_limit_remaining = 1000
        self._rate_limit_reset = time.time() + 60
        self._request_count = 0
        self._last_request_time = 0
        self._last_request_time = 0
        self._min_request_interval = 1.0  # 1s between requests to be safe

    def _get_cache_key(self, endpoint: str, params: Dict) -> str:
        """Generate a cache key for an API request."""
        param_str = json.dumps(params, sort_keys=True)
        return hashlib.md5(f"{endpoint}:{param_str}".encode()).hexdigest()

    def _check_cache(self, cache_key: str) -> Optional[Any]:
        """Check if a cached response exists and is still valid."""
        if cache_key in self._cache:
            timestamp, data = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                return data
            else:
                del self._cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, data: Any) -> None:
        """Store data in the cache."""
        self._cache[cache_key] = (time.time(), data)

    def _rate_limit(self) -> None:
        """Implement rate limiting."""
        current_time = time.time()

        # Ensure minimum interval between requests
        elapsed = current_time - self._last_request_time
        if elapsed < self._min_request_interval:
            time.sleep(self._min_request_interval - elapsed)

        self._last_request_time = time.time()
        self._request_count += 1

    def _make_request(
        self,
        endpoint: str,
        params: Optional[Dict] = None,
        retries: int = 3
    ) -> Optional[Any]:
        """
        Make an API request with error handling and retries.

        Args:
            endpoint: API endpoint path
            params: Query parameters
            retries: Number of retry attempts

        Returns:
            Response data or None on failure
        """
        if params is None:
            params = {}

        params["api_token"] = self.api_key
        params["fmt"] = "json"

        cache_key = self._get_cache_key(endpoint, params)
        cached = self._check_cache(cache_key)
        if cached is not None:
            return cached

        url = f"{self.base_url}/{endpoint}"

        for attempt in range(retries):
            try:
                self._rate_limit()
                response = requests.get(url, params=params, timeout=60)

                # Handle rate limiting
                if response.status_code == 429:
                    wait_time = 2 ** attempt
                    print(f"Rate limited. Waiting {wait_time}s...")
                    time.sleep(wait_time)
                    continue

                response.raise_for_status()
                data = response.json()
                self._set_cache(cache_key, data)
                return data

            except requests.exceptions.Timeout:
                print(f"Timeout on attempt {attempt + 1}/{retries}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

            except requests.exceptions.RequestException as e:
                # Fail fast on 404 (Not Found) - do not retry
                if isinstance(e, requests.exceptions.HTTPError) and e.response.status_code == 404:
                    print(f"Warning: Endpoint not found (404): {url}")
                    return None
                
                print(f"Request error on attempt {attempt + 1}/{retries}: {e}")
                if attempt < retries - 1:
                    time.sleep(2 ** attempt)

            except json.JSONDecodeError as e:
                print(f"JSON decode error: {e}")
                return None

        return None

    def get_index_components(self, index: str = "GSPC.INDX") -> Dict:
        """Get historical components for an index."""
        return self._make_request(f"fundamentals/{index}") or {}

    def get_index_changes(self, index: str = "GSPC.INDX") -> List:
        """Get historical add/remove events for an index."""
        return self._make_request(f"index-composition-history/{index}") or []

    def get_eod_bulk(self, exchange: str = "US", date: Optional[str] = None) -> List:
        """Get bulk EOD prices for an exchange."""
        params = {}
        if date:
            params["date"] = date
        return self._make_request(f"eod-bulk-last-day/{exchange}", params) or []

    def get_fundamentals(self, ticker: str) -> Dict:
        """Get fundamentals for a ticker."""
        return self._make_request(f"fundamentals/{ticker}.US") or {}

    def get_request_stats(self) -> Dict:
        """Get API request statistics."""
        return {
            "total_requests": self._request_count,
            "cache_size": len(self._cache)
        }


# =============================================================================
# DEMO DATA GENERATOR
# =============================================================================

class DemoDataGenerator:
    """Generates realistic simulated data for demo mode."""

    SECTORS = [
        "Technology", "Healthcare", "Financials", "Consumer Discretionary",
        "Communication Services", "Industrials", "Consumer Staples",
        "Energy", "Utilities", "Real Estate", "Materials"
    ]

    TOP_COMPANIES = [
        ("AAPL", "Apple Inc.", "Technology", 3000000000000, 15500000000),
        ("MSFT", "Microsoft Corporation", "Technology", 2800000000000, 7430000000),
        ("GOOGL", "Alphabet Inc.", "Communication Services", 1800000000000, 12350000000),
        ("AMZN", "Amazon.com Inc.", "Consumer Discretionary", 1700000000000, 10400000000),
        ("NVDA", "NVIDIA Corporation", "Technology", 1500000000000, 24800000000),
        ("META", "Meta Platforms Inc.", "Communication Services", 900000000000, 2560000000),
        ("TSLA", "Tesla Inc.", "Consumer Discretionary", 800000000000, 3170000000),
        ("BRK.B", "Berkshire Hathaway Inc.", "Financials", 750000000000, 1450000000),
        ("UNH", "UnitedHealth Group Inc.", "Healthcare", 500000000000, 924000000),
        ("JNJ", "Johnson & Johnson", "Healthcare", 450000000000, 2410000000),
        ("JPM", "JPMorgan Chase & Co.", "Financials", 480000000000, 2870000000),
        ("V", "Visa Inc.", "Financials", 470000000000, 2080000000),
        ("PG", "Procter & Gamble Co.", "Consumer Staples", 380000000000, 2360000000),
        ("XOM", "Exxon Mobil Corporation", "Energy", 450000000000, 4000000000),
        ("HD", "Home Depot Inc.", "Consumer Discretionary", 340000000000, 1010000000),
        ("MA", "Mastercard Inc.", "Financials", 360000000000, 933000000),
        ("CVX", "Chevron Corporation", "Energy", 300000000000, 1860000000),
        ("MRK", "Merck & Co. Inc.", "Healthcare", 280000000000, 2540000000),
        ("ABBV", "AbbVie Inc.", "Healthcare", 270000000000, 1770000000),
        ("PEP", "PepsiCo Inc.", "Consumer Staples", 250000000000, 1380000000),
        ("KO", "Coca-Cola Company", "Consumer Staples", 260000000000, 4320000000),
        ("LLY", "Eli Lilly and Company", "Healthcare", 400000000000, 950000000),
        ("COST", "Costco Wholesale Corp.", "Consumer Staples", 240000000000, 443000000),
        ("WMT", "Walmart Inc.", "Consumer Staples", 420000000000, 8050000000),
        ("BAC", "Bank of America Corp.", "Financials", 250000000000, 7930000000),
        ("AVGO", "Broadcom Inc.", "Technology", 350000000000, 465000000),
        ("TMO", "Thermo Fisher Scientific", "Healthcare", 200000000000, 384000000),
        ("DIS", "Walt Disney Company", "Communication Services", 180000000000, 1830000000),
        ("CSCO", "Cisco Systems Inc.", "Technology", 210000000000, 4060000000),
        ("ADBE", "Adobe Inc.", "Technology", 220000000000, 449000000),
        ("CRM", "Salesforce Inc.", "Technology", 200000000000, 970000000),
        ("NKE", "Nike Inc.", "Consumer Discretionary", 150000000000, 1510000000),
        ("NFLX", "Netflix Inc.", "Communication Services", 190000000000, 432000000),
        ("INTC", "Intel Corporation", "Technology", 140000000000, 4200000000),
        ("AMD", "Advanced Micro Devices", "Technology", 180000000000, 1620000000),
        ("QCOM", "Qualcomm Inc.", "Technology", 160000000000, 1120000000),
        ("TXN", "Texas Instruments Inc.", "Technology", 150000000000, 910000000),
        ("IBM", "IBM Corporation", "Technology", 130000000000, 911000000),
        ("GE", "General Electric Company", "Industrials", 120000000000, 1090000000),
        ("CAT", "Caterpillar Inc.", "Industrials", 140000000000, 512000000),
    ]

    HISTORICAL_CHANGES = [
        ("2024-03-15", "SMCI", "add", "Super Micro Computer Inc.", "Market cap increase"),
        ("2024-03-15", "WHR", "remove", "Whirlpool Corporation", "Market cap decline"),
        ("2024-06-24", "CRWD", "add", "CrowdStrike Holdings", "Growth qualification"),
        ("2024-06-24", "RHI", "remove", "Robert Half International", "Size criteria"),
        ("2024-09-23", "PLTR", "add", "Palantir Technologies", "Market cap growth"),
        ("2024-09-23", "AAL", "remove", "American Airlines Group", "Restructuring"),
        ("2024-12-23", "AXON", "add", "Axon Enterprise Inc.", "Sector rebalance"),
        ("2024-12-23", "QRVO", "remove", "Qorvo Inc.", "Sector rebalance"),
        ("2025-01-06", "UBER", "add", "Uber Technologies Inc.", "Growth qualification"),
        ("2025-01-06", "BIO", "remove", "Bio-Rad Laboratories", "Size criteria"),
    ]

    def __init__(self):
        """Initialize the demo data generator."""
        self._master_list = None
        self._change_events = None
        self._current_constituents = None

    def generate_all(self) -> Dict:
        """Generate all demo data."""
        return {
            "master_list": self._generate_master_list(),
            "change_events": self._generate_change_events(),
            "current_constituents": self._generate_current_constituents(),
            "bulk_prices": self._generate_bulk_prices()
        }

    def _generate_master_list(self) -> List[Dict]:
        """Generate master list of historical components."""
        if self._master_list:
            return self._master_list

        master_list = []

        # Add top companies
        for ticker, name, sector, market_cap, shares in self.TOP_COMPANIES:
            days_member = 365 * 10 + (hash(ticker) % 3650)
            start_date = (datetime.now() - timedelta(days=days_member)).strftime("%Y-%m-%d")
            master_list.append({
                "code": ticker,
                "name": name,
                "sector": sector,
                "start_date": start_date,
                "end_date": None,
                "market_cap": market_cap,
                "shares_outstanding": shares
            })

        # Generate additional tickers to reach ~500
        for i in range(460):
            sector = self.SECTORS[i % len(self.SECTORS)]
            ticker = f"DEMO{i:03d}"
            name = f"Demo Company {i} Inc."
            market_cap = 15000000000 + (i * 50000000)
            shares = int(market_cap / (50 + i % 200))
            days_member = 365 * 5 + (i * 30) % 3650

            start_date = (datetime.now() - timedelta(days=days_member)).strftime("%Y-%m-%d")

            master_list.append({
                "code": ticker,
                "name": name,
                "sector": sector,
                "start_date": start_date,
                "end_date": None,
                "market_cap": market_cap,
                "shares_outstanding": shares
            })

        # Add historical members that were removed
        removed_companies = [
            ("WHR", "Whirlpool Corporation", "Consumer Discretionary", "2015-01-01", "2024-03-14", 8000000000),
            ("RHI", "Robert Half International", "Industrials", "2010-01-01", "2024-06-23", 6000000000),
            ("AAL", "American Airlines Group", "Industrials", "2015-03-01", "2024-09-22", 7500000000),
            ("QRVO", "Qorvo Inc.", "Technology", "2015-06-01", "2024-12-22", 9000000000),
            ("BIO", "Bio-Rad Laboratories", "Healthcare", "2012-01-01", "2025-01-05", 10000000000),
        ]

        for ticker, name, sector, start, end, market_cap in removed_companies:
            master_list.append({
                "code": ticker,
                "name": name,
                "sector": sector,
                "start_date": start,
                "end_date": end,
                "market_cap": market_cap,
                "shares_outstanding": int(market_cap / 100)
            })

        self._master_list = master_list
        return master_list

    def _generate_change_events(self) -> List[ChangeEvent]:
        """Generate historical change events."""
        if self._change_events:
            return self._change_events

        events = []
        for date, ticker, action, name, reason in self.HISTORICAL_CHANGES:
            events.append(ChangeEvent(
                date=date,
                ticker=ticker,
                action=action,
                name=name,
                reason=reason
            ))

        self._change_events = sorted(events, key=lambda x: x.date, reverse=True)
        return self._change_events

    def _generate_current_constituents(self) -> List[Dict]:
        """Generate current constituent list."""
        if self._current_constituents:
            return self._current_constituents

        master = self._generate_master_list()
        current = [
            item for item in master
            if item.get("end_date") is None
        ][:500]

        self._current_constituents = current
        return current

    def _generate_bulk_prices(self) -> Dict[str, Dict]:
        """Generate bulk price data keyed by ticker."""
        prices = {}
        master = self._generate_master_list()

        for item in master:
            ticker = item["code"]
            market_cap = item.get("market_cap", 50000000000)
            shares = item.get("shares_outstanding", 1000000000)
            price = market_cap / shares if shares else 100.0

            prices[ticker] = {
                "code": ticker,
                "close": round(price, 2),
                "adjusted_close": round(price, 2),
                "volume": 5000000 + (hash(ticker) % 10000000),
                "market_cap": market_cap,
                "shares_outstanding": shares
            }

        return prices


# =============================================================================
# MAIN ENGINE
# =============================================================================

class SP500HybridEngine:
    """
    S&P 500 Hybrid Consensus Reconstruction Engine.

    Implements a robust approach to reconstruct historical S&P 500 composition
    using two complementary data methods with intelligent conflict resolution.
    """

    def __init__(
        self,
        api_key: Optional[str] = None,
        demo_mode: bool = False,
        verbose: bool = True
    ):
        """
        Initialize the SP500HybridEngine.

        Args:
            api_key: EODHD API key for live data access
            demo_mode: If True, use simulated data instead of API calls
            verbose: If True, print progress messages
        """
        self.api_key = api_key
        self.demo_mode = demo_mode
        self.verbose = verbose

        # Initialize API client or demo data
        if demo_mode:
            self._demo_generator = DemoDataGenerator()
            self._demo_data = self._demo_generator.generate_all()
            self._client = None
        else:
            if not api_key:
                raise ValueError("API key required when not in demo mode")
            self._client = EODHDClient(api_key)
            self._demo_data = None

        # Caches
        self._master_list_cache: Optional[List[Dict]] = None
        self._change_events_cache: Optional[List[ChangeEvent]] = None
        self._current_constituents_cache: Optional[List[Dict]] = None
        self._bulk_prices_cache: Dict[str, Dict] = {}

    def _log(self, message: str) -> None:
        """Print a message if verbose mode is enabled."""
        if self.verbose:
            print(message)

    # -------------------------------------------------------------------------
    # DATA FETCHING
    # -------------------------------------------------------------------------

    def _fetch_master_list(self) -> List[Dict]:
        """Fetch the master list of historical S&P 500 components."""
        if self._master_list_cache:
            return self._master_list_cache

        if self.demo_mode:
            self._master_list_cache = self._demo_data["master_list"]
            return self._master_list_cache

        data = self._client.get_index_components()
        components = data.get("Components", {})

        master_list = []
        for ticker, info in components.items():
            master_list.append({
                "code": ticker,
                "name": info.get("Name", ""),
                "sector": info.get("Sector", "Unknown"),
                "start_date": info.get("StartDate"),
                "end_date": info.get("EndDate"),
                "market_cap": info.get("MarketCapitalization"),
                "shares_outstanding": info.get("SharesOutstanding")
            })

        self._master_list_cache = master_list
        return master_list

    def _fetch_change_events(self) -> List[ChangeEvent]:
        """Fetch historical add/remove events."""
        if self._change_events_cache:
            return self._change_events_cache

        if self.demo_mode:
            self._change_events_cache = self._demo_data["change_events"]
            return self._change_events_cache

        data = self._client.get_index_changes()
        if not data:
            print("Warning: No historical changes found (or API returned 404). Method B (Backward Roll) will be effectively disabled.")
            return []

        events = []
        for event in data:
            events.append(ChangeEvent(
                date=event.get("Date", ""),
                ticker=event.get("Code", ""),
                action=event.get("Action", "").lower(),
                name=event.get("Name", ""),
                reason=event.get("Reason", "")
            ))

        self._change_events_cache = sorted(events, key=lambda x: x.date, reverse=True)
        return self._change_events_cache

    def _fetch_current_constituents(self) -> List[Dict]:
        """Fetch current S&P 500 constituents."""
        if self._current_constituents_cache:
            return self._current_constituents_cache

        if self.demo_mode:
            self._current_constituents_cache = self._demo_data["current_constituents"]
            return self._current_constituents_cache

        data = self._client.get_index_components()
        components = data.get("Components", {})

        current = [
            {
                "code": ticker,
                "name": info.get("Name", ""),
                "sector": info.get("Sector", "Unknown"),
                "market_cap": info.get("MarketCapitalization"),
                "shares_outstanding": info.get("SharesOutstanding")
            }
            for ticker, info in components.items()
            if not info.get("EndDate")
        ]

        self._current_constituents_cache = current
        return current

    def _fetch_bulk_prices(self, date: str) -> Dict[str, Dict]:
        """Fetch bulk EOD prices for a date."""
        if date in self._bulk_prices_cache:
            return self._bulk_prices_cache[date]

        if self.demo_mode:
            self._bulk_prices_cache[date] = self._demo_data["bulk_prices"]
            return self._bulk_prices_cache[date]

        data = self._client.get_eod_bulk(date=date)
        prices = {}

        for item in data:
            ticker = item.get("code", "")
            prices[ticker] = {
                "code": ticker,
                "close": item.get("close"),
                "adjusted_close": item.get("adjusted_close"),
                "volume": item.get("volume"),
                "market_cap": item.get("market_cap"),
                "shares_outstanding": item.get("shares_outstanding")
            }

        self._bulk_prices_cache[date] = prices
        return prices

    # -------------------------------------------------------------------------
    # METHOD A: MASTER LIST FILTER
    # -------------------------------------------------------------------------

    def _method_a_master_list(self, target_date: str) -> Set[str]:
        """
        Method A (The Filter): Get constituents from master list.
        Filter by start/end dates to find members on target date.
        """
        master_list = self._fetch_master_list()
        target = datetime.strptime(target_date, "%Y-%m-%d")

        active_tickers = set()

        for item in master_list:
            start_date = item.get("start_date")
            end_date = item.get("end_date")

            # Check start date
            if start_date:
                try:
                    start = datetime.strptime(start_date, "%Y-%m-%d")
                    if start > target:
                        continue
                except ValueError:
                    pass

            # Check end date
            if end_date:
                try:
                    end = datetime.strptime(end_date, "%Y-%m-%d")
                    if end < target:
                        continue
                except ValueError:
                    pass

            active_tickers.add(item["code"])

        return active_tickers

    # -------------------------------------------------------------------------
    # METHOD B: BACKWARD ROLL
    # -------------------------------------------------------------------------

    def _method_b_backward_roll(self, target_date: str) -> Set[str]:
        """
        Method B (The Roll): Start with current list and apply changes backwards.
        Take today's list and apply historical add/remove events in reverse.
        """
        current = self._fetch_current_constituents()
        events = self._fetch_change_events()

        # Start with current constituents
        active_tickers = {item["code"] for item in current}

        target = datetime.strptime(target_date, "%Y-%m-%d")
        today = datetime.now()

        # Apply events backwards (from most recent to oldest)
        for event in events:
            try:
                event_date = datetime.strptime(event.date, "%Y-%m-%d")
            except ValueError:
                continue

            # Only apply events between target date and today
            if event_date <= target:
                continue
            if event_date > today:
                continue

            # Reverse the action (going backwards in time)
            if event.action == "add":
                # Was added after target date - remove it
                active_tickers.discard(event.ticker)
            elif event.action == "remove":
                # Was removed after target date - add it back
                active_tickers.add(event.ticker)

        return active_tickers

    # -------------------------------------------------------------------------
    # CONFLICT RECONCILIATION
    # -------------------------------------------------------------------------

    def _calculate_membership_days(self, ticker: str, target_date: str) -> int:
        """Calculate membership duration as of target date."""
        master_list = self._fetch_master_list()
        target = datetime.strptime(target_date, "%Y-%m-%d")

        for item in master_list:
            if item["code"] == ticker:
                start_date = item.get("start_date")
                if start_date:
                    try:
                        start = datetime.strptime(start_date, "%Y-%m-%d")
                        return max(0, (target - start).days)
                    except ValueError:
                        pass
        return 0

    def _has_recent_change_event(
        self,
        ticker: str,
        target_date: str,
        window_days: int = 30
    ) -> Optional[ChangeEvent]:
        """Check if ticker has a change event near the target date."""
        events = self._fetch_change_events()
        target = datetime.strptime(target_date, "%Y-%m-%d")

        for event in events:
            if event.ticker == ticker:
                try:
                    event_date = datetime.strptime(event.date, "%Y-%m-%d")
                    if abs((event_date - target).days) <= window_days:
                        return event
                except ValueError:
                    continue

        return None

    def _reconcile_conflicts(
        self,
        method_a: Set[str],
        method_b: Set[str],
        target_date: str
    ) -> Tuple[Set[str], List[str], Dict[str, str]]:
        """
        Reconcile conflicts between Method A and Method B using tie-breaker logic.

        Tie-breaker rules:
        1. Bias towards Action: Trust explicit change events (Method B)
        2. Bias towards Presence: Trust master list for 100+ day members
        3. Union Fallback: Include if uncertain

        Returns:
            (final_tickers, conflict_tickers, resolution_details)
        """
        agreed = method_a & method_b
        only_a = method_a - method_b
        only_b = method_b - method_a

        final_tickers = set(agreed)
        conflict_tickers = []
        resolution_details = {}

        # Process tickers only in Method A (master list)
        for ticker in only_a:
            conflict_tickers.append(ticker)

            # Check for recent change event
            event = self._has_recent_change_event(ticker, target_date, window_days=30)

            if event:
                # Bias towards Action: Trust the change event
                if event.action == "add":
                    target = datetime.strptime(target_date, "%Y-%m-%d")
                    event_date = datetime.strptime(event.date, "%Y-%m-%d")
                    if event_date <= target:
                        final_tickers.add(ticker)
                resolution_details[ticker] = ConflictResolution.BIAS_ACTION.value
            else:
                # Check membership duration
                membership_days = self._calculate_membership_days(ticker, target_date)

                if membership_days >= 100:
                    # Bias towards Presence: Long-term member
                    final_tickers.add(ticker)
                    resolution_details[ticker] = ConflictResolution.BIAS_PRESENCE.value
                else:
                    # Union Fallback: Include if uncertain
                    final_tickers.add(ticker)
                    resolution_details[ticker] = ConflictResolution.UNION_FALLBACK.value

        # Process tickers only in Method B (backward roll)
        for ticker in only_b:
            conflict_tickers.append(ticker)

            # Check for explicit change event
            event = self._has_recent_change_event(ticker, target_date, window_days=60)

            if event:
                # Bias towards Action: Trust the change event
                target = datetime.strptime(target_date, "%Y-%m-%d")
                event_date = datetime.strptime(event.date, "%Y-%m-%d")

                if event.action == "add" and event_date <= target:
                    final_tickers.add(ticker)
                elif event.action == "remove" and event_date > target:
                    final_tickers.add(ticker)

                resolution_details[ticker] = ConflictResolution.BIAS_ACTION.value
            else:
                # Union Fallback: Include if no clear resolution
                final_tickers.add(ticker)
                resolution_details[ticker] = ConflictResolution.UNION_FALLBACK.value

        return final_tickers, conflict_tickers, resolution_details

    # -------------------------------------------------------------------------
    # WEIGHT CALCULATION
    # -------------------------------------------------------------------------

    def _get_ticker_info(self, ticker: str) -> Dict:
        """Get detailed information for a ticker."""
        master_list = self._fetch_master_list()

        for item in master_list:
            if item["code"] == ticker:
                return item

        current = self._fetch_current_constituents()
        for item in current:
            if item["code"] == ticker:
                return item

        return {"code": ticker, "name": "", "sector": "Unknown", "market_cap": None}

    def _calculate_sector_median_market_cap(
        self,
        sector: str,
        constituents: Dict[str, Dict]
    ) -> float:
        """Calculate median market cap for a sector (for fallback)."""
        sector_caps = [
            info["market_cap"] for info in constituents.values()
            if info.get("sector") == sector and info.get("market_cap")
        ]

        if sector_caps:
            return statistics.median(sector_caps)

        # Fallback to overall median
        all_caps = [
            info["market_cap"] for info in constituents.values()
            if info.get("market_cap")
        ]

        if all_caps:
            return statistics.median(all_caps)

        return 20000000000  # Default: $20B

    def _calculate_weights(
        self,
        tickers: Set[str],
        target_date: str,
        resolution_details: Dict[str, str]
    ) -> Dict[str, ConstituentRecord]:
        """
        Calculate market-cap weights for constituents.
        Uses sector-median fallback for missing data.
        """
        # Get price data
        prices = self._fetch_bulk_prices(target_date)

        # Build constituent info
        constituents_info = {}
        for ticker in tickers:
            info = self._get_ticker_info(ticker)
            price_data = prices.get(ticker, {})

            # Try to get market cap from multiple sources
            market_cap = (
                price_data.get("market_cap") or
                info.get("market_cap")
            )

            shares = (
                price_data.get("shares_outstanding") or
                info.get("shares_outstanding")
            )

            close_price = price_data.get("close") or price_data.get("adjusted_close")

            # Calculate market cap from price * shares if needed
            if not market_cap and close_price and shares:
                market_cap = close_price * shares

            constituents_info[ticker] = {
                "ticker": ticker,
                "name": info.get("name", ""),
                "sector": info.get("sector", "Unknown"),
                "market_cap": market_cap,
                "shares_outstanding": shares,
                "close_price": close_price,
                "source": resolution_details.get(ticker, DataSource.BOTH.value)
            }

        # Fill missing market caps with sector median
        for ticker, info in constituents_info.items():
            if info["market_cap"] is None:
                sector = info["sector"]
                info["market_cap"] = self._calculate_sector_median_market_cap(
                    sector, constituents_info
                )
                info["market_cap_estimated"] = True
            else:
                info["market_cap_estimated"] = False

        # Calculate total market cap
        total_market_cap = sum(
            info["market_cap"] for info in constituents_info.values()
            if info["market_cap"]
        )

        # Build ConstituentRecord objects with weights
        result = {}
        for ticker, info in constituents_info.items():
            weight = 0.0
            if total_market_cap > 0 and info["market_cap"]:
                weight = (info["market_cap"] / total_market_cap) * 100

            result[ticker] = ConstituentRecord(
                ticker=ticker,
                name=info["name"],
                sector=info["sector"],
                market_cap=info["market_cap"],
                shares_outstanding=info["shares_outstanding"],
                close_price=info["close_price"],
                weight=round(weight, 6),
                source=info["source"],
                membership_days=self._calculate_membership_days(ticker, target_date),
                market_cap_estimated=info.get("market_cap_estimated", False)
            )

        return result

    # -------------------------------------------------------------------------
    # MAIN RECONSTRUCTION
    # -------------------------------------------------------------------------

    def reconstruct(
        self,
        start_date: str,
        end_date: str,
        frequency: str = "daily"
    ) -> Dict[str, DailyIndexRecord]:
        """
        Reconstruct S&P 500 composition for a date range.

        Args:
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            frequency: 'daily', 'weekly', or 'monthly'

        Returns:
            Dictionary keyed by date with DailyIndexRecord objects
        """
        self._log(f"\nReconstructing S&P 500 from {start_date} to {end_date}")
        self._log(f"Frequency: {frequency}")

        # Pre-fetch data
        self._log("Fetching master list...")
        self._fetch_master_list()
        self._log("Fetching change events...")
        self._fetch_change_events()
        self._log("Fetching current constituents...")
        self._fetch_current_constituents()

        results = {}

        start = datetime.strptime(start_date, "%Y-%m-%d")
        end = datetime.strptime(end_date, "%Y-%m-%d")

        # Determine date increment
        if frequency == "weekly":
            delta = timedelta(days=7)
        elif frequency == "monthly":
            delta = timedelta(days=30)
        else:
            delta = timedelta(days=1)

        # Build list of dates (skip weekends for daily)
        dates_to_process = []
        current_date = start

        while current_date <= end:
            if frequency == "daily":
                # Skip weekends
                if current_date.weekday() < 5:
                    dates_to_process.append(current_date.strftime("%Y-%m-%d"))
            else:
                dates_to_process.append(current_date.strftime("%Y-%m-%d"))
            current_date += delta

        self._log(f"Processing {len(dates_to_process)} dates...")

        for i, date_str in enumerate(dates_to_process):
            if (i + 1) % 10 == 0 or i == 0 or i == len(dates_to_process) - 1:
                self._log(f"  [{i + 1}/{len(dates_to_process)}] {date_str}")

            # Get constituents from both methods
            method_a_tickers = self._method_a_master_list(date_str)
            method_b_tickers = self._method_b_backward_roll(date_str)

            # Reconcile conflicts
            final_tickers, conflict_tickers, resolution_details = self._reconcile_conflicts(
                method_a_tickers, method_b_tickers, date_str
            )

            # Calculate agreement percentage
            if method_a_tickers or method_b_tickers:
                agreed = method_a_tickers & method_b_tickers
                total_unique = method_a_tickers | method_b_tickers
                agreement_value = len(agreed) / len(total_unique) * 100 if total_unique else 100
            else:
                agreement_value = 100

            # Calculate weights
            constituents = self._calculate_weights(
                final_tickers, date_str, resolution_details
            )

            # Update source for agreed tickers
            for ticker in (method_a_tickers & method_b_tickers):
                if ticker in constituents:
                    constituents[ticker].source = DataSource.BOTH.value

            # Calculate sector aggregations
            sectors = defaultdict(lambda: {"count": 0, "weight": 0.0, "constituents": []})
            for const in constituents.values():
                sectors[const.sector]["count"] += 1
                sectors[const.sector]["weight"] += const.weight
                sectors[const.sector]["constituents"].append(const.ticker)

            # Round sector weights
            for sector in sectors:
                sectors[sector]["weight"] = round(sectors[sector]["weight"], 4)

            # Count resolutions
            resolution_stats = defaultdict(int)
            for resolution in resolution_details.values():
                resolution_stats[resolution] += 1

            # Create record
            record = DailyIndexRecord(
                date=date_str,
                source_agreement=f"{agreement_value:.1f}%",
                agreement_value=round(agreement_value, 2),
                conflict_tickers=conflict_tickers,
                final_count=len(final_tickers),
                sectors=dict(sectors),
                constituents={
                    ticker: asdict(const) for ticker, const in constituents.items()
                },
                resolution_stats=dict(resolution_stats),
                method_a_count=len(method_a_tickers),
                method_b_count=len(method_b_tickers)
            )

            results[date_str] = record

        self._log(f"\nReconstruction complete. Processed {len(results)} dates.")
        return results

    # -------------------------------------------------------------------------
    # QUALITY REPORT
    # -------------------------------------------------------------------------

    def generate_quality_report(
        self,
        results: Dict[str, DailyIndexRecord]
    ) -> DataQualityMetrics:
        """Generate comprehensive data quality metrics."""
        metrics = DataQualityMetrics()

        if not results:
            return metrics

        metrics.total_dates_processed = len(results)

        # Agreement statistics
        agreements = [r.agreement_value for r in results.values()]
        metrics.avg_source_agreement = round(statistics.mean(agreements), 2)
        metrics.min_source_agreement = round(min(agreements), 2)
        metrics.max_source_agreement = round(max(agreements), 2)

        if len(agreements) > 1:
            metrics.std_source_agreement = round(statistics.stdev(agreements), 2)

        # Conflict statistics
        conflict_counter = defaultdict(int)
        for record in results.values():
            if record.conflict_tickers:
                metrics.dates_with_conflicts += 1
                for ticker in record.conflict_tickers:
                    conflict_counter[ticker] += 1
            else:
                metrics.conflict_free_dates += 1

            for resolution, count in record.resolution_stats.items():
                metrics.conflicts_by_resolution[resolution] = \
                    metrics.conflicts_by_resolution.get(resolution, 0) + count

        metrics.total_conflicts_resolved = sum(conflict_counter.values())
        metrics.top_conflict_tickers = sorted(
            conflict_counter.items(), key=lambda x: -x[1]
        )[:10]

        # Constituent statistics
        counts = [r.final_count for r in results.values()]
        metrics.avg_constituent_count = round(statistics.mean(counts), 1)
        metrics.min_constituent_count = min(counts)
        metrics.max_constituent_count = max(counts)

        # Market cap and sector coverage
        all_sectors = defaultdict(int)
        missing_count = 0

        for record in results.values():
            for sector, data in record.sectors.items():
                all_sectors[sector] = max(all_sectors[sector], data["count"])

            for ticker, const in record.constituents.items():
                if const.get("market_cap_estimated"):
                    missing_count += 1

        metrics.missing_market_cap_count = missing_count
        metrics.sector_coverage = dict(all_sectors)

        return metrics

    def print_quality_report(self, metrics: DataQualityMetrics) -> None:
        """Print a formatted data quality report."""
        print("\n" + "=" * 70)
        print("                    DATA QUALITY REPORT")
        print("=" * 70)

        print(f"\n{'PROCESSING SUMMARY':=^70}")
        print(f"  Total dates processed:     {metrics.total_dates_processed:>10}")
        print(f"  Days with conflicts:       {metrics.dates_with_conflicts:>10}")
        print(f"  Conflict-free days:        {metrics.conflict_free_dates:>10}")

        print(f"\n{'AGREEMENT METRICS':=^70}")
        print(f"  Average agreement:         {metrics.avg_source_agreement:>10.2f}%")
        print(f"  Minimum agreement:         {metrics.min_source_agreement:>10.2f}%")
        print(f"  Maximum agreement:         {metrics.max_source_agreement:>10.2f}%")
        print(f"  Std deviation:             {metrics.std_source_agreement:>10.2f}%")

        print(f"\n{'CONFLICT RESOLUTION':=^70}")
        print(f"  Total conflicts resolved:  {metrics.total_conflicts_resolved:>10}")
        if metrics.conflicts_by_resolution:
            print("  Resolution breakdown:")
            for resolution, count in sorted(metrics.conflicts_by_resolution.items()):
                print(f"    - {resolution:35s} {count:>5}")

        if metrics.top_conflict_tickers:
            print(f"\n{'TOP 10 CONFLICT TICKERS':=^70}")
            for ticker, count in metrics.top_conflict_tickers:
                bar = "█" * min(count, 30)
                print(f"    {ticker:10s} {count:>3} conflicts  {bar}")

        print(f"\n{'CONSTITUENT STATISTICS':=^70}")
        print(f"  Average count:             {metrics.avg_constituent_count:>10.1f}")
        print(f"  Minimum count:             {metrics.min_constituent_count:>10}")
        print(f"  Maximum count:             {metrics.max_constituent_count:>10}")

        print(f"\n{'MARKET CAP DATA':=^70}")
        print(f"  Missing market cap entries:{metrics.missing_market_cap_count:>10}")
        print("  (Filled using sector-median fallback)")

        print(f"\n{'SECTOR COVERAGE (max constituents)':=^70}")
        for sector, count in sorted(
            metrics.sector_coverage.items(), key=lambda x: -x[1]
        ):
            bar = "█" * (count // 5)
            print(f"    {sector:28s} {count:>3}  {bar}")

        print("\n" + "=" * 70)

    # -------------------------------------------------------------------------
    # OUTPUT
    # -------------------------------------------------------------------------

    def to_json(
        self,
        results: Dict[str, DailyIndexRecord],
        filepath: Optional[str] = None,
        compact: bool = False
    ) -> str:
        """
        Export results to JSON format.

        Args:
            results: Dictionary of DailyIndexRecord objects
            filepath: Optional file path to save JSON
            compact: If True, minimize output size

        Returns:
            JSON string
        """
        output = {}
        for date, record in results.items():
            output[date] = asdict(record)

        indent = None if compact else 2
        json_str = json.dumps(output, indent=indent, default=str)

        if filepath:
            with open(filepath, 'w') as f:
                f.write(json_str)
            self._log(f"Results saved to {filepath}")

        return json_str


# =============================================================================
# FACTORY FUNCTION
# =============================================================================

def create_demo_engine(verbose: bool = True) -> SP500HybridEngine:
    """
    Factory function to create an engine in demo mode.

    Args:
        verbose: If True, print progress messages

    Returns:
        SP500HybridEngine configured for demo mode
    """
    return SP500HybridEngine(demo_mode=True, verbose=verbose)


# =============================================================================
# MAIN ENTRY POINT
# =============================================================================

def main():
    """Main entry point for command-line usage."""
    print("=" * 70)
    print("       S&P 500 HYBRID CONSENSUS RECONSTRUCTION ENGINE")
    print("=" * 70)

    # Parse command line arguments
    if len(sys.argv) < 2:
        print("\nUsage:")
        print("  python sp500_engine.py YOUR_API_KEY    # Live mode with EODHD API")
        print("  python sp500_engine.py demo            # Demo mode with simulated data")
        print("\nRunning in demo mode...\n")
        api_key = "demo"
    else:
        api_key = sys.argv[1]

    # Initialize engine
    demo_mode = api_key.lower() == "demo"
    engine = SP500HybridEngine(
        api_key=None if demo_mode else api_key,
        demo_mode=demo_mode,
        verbose=True
    )

    mode_str = "DEMO MODE" if demo_mode else "LIVE MODE"
    print(f"Running in {mode_str}")

    # Reconstruct for 2025 (or recent period in demo mode)
    if demo_mode:
        start_date = "2025-01-01"
        end_date = "2025-01-31"
        frequency = "weekly"
    else:
        start_date = "2025-01-01"
        end_date = datetime.now().strftime("%Y-%m-%d")
        frequency = "daily"

    print(f"\nDate range: {start_date} to {end_date}")
    print(f"Frequency: {frequency}")

    # Run reconstruction
    results = engine.reconstruct(
        start_date=start_date,
        end_date=end_date,
        frequency=frequency
    )

    # Print sample output
    print("\n" + "-" * 70)
    print("SAMPLE OUTPUT (First Date)")
    print("-" * 70)

    if results:
        first_date = list(results.keys())[0]
        record = results[first_date]

        print(f"\nDate: {record.date}")
        print(f"Source Agreement: {record.source_agreement}")
        print(f"Method A Count: {record.method_a_count}")
        print(f"Method B Count: {record.method_b_count}")
        print(f"Final Count: {record.final_count}")
        print(f"Conflict Tickers: {len(record.conflict_tickers)}")

        print(f"\nSector Weights:")
        sorted_sectors = sorted(
            record.sectors.items(),
            key=lambda x: x[1]["weight"],
            reverse=True
        )
        for sector, data in sorted_sectors[:5]:
            print(f"  {sector:30s} {data['weight']:>6.2f}% ({data['count']} stocks)")

        print(f"\nTop 10 Constituents by Weight:")
        sorted_constituents = sorted(
            record.constituents.items(),
            key=lambda x: x[1]["weight"],
            reverse=True
        )[:10]

        for ticker, data in sorted_constituents:
            print(
                f"  {ticker:8s} {data['weight']:>8.4f}%  "
                f"{data['sector']:25s} {data['name'][:25]}"
            )

    # Generate and print quality report
    metrics = engine.generate_quality_report(results)
    engine.print_quality_report(metrics)

    # Save to JSON
    output_file = "sp500_reconstruction_output.json"
    print(f"\nSaving results to {output_file}...")
    engine.to_json(results, output_file)

    print("\n" + "=" * 70)
    print("                    RECONSTRUCTION COMPLETE")
    print("=" * 70)

    if not demo_mode:
        stats = engine._client.get_request_stats()
        print(f"\nAPI Statistics:")
        print(f"  Total requests: {stats['total_requests']}")
        print(f"  Cache size: {stats['cache_size']}")


if __name__ == "__main__":
    main()
