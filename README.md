# S&P 500 Hybrid Consensus Reconstruction Engine

A robust Python engine that reconstructs historical S&P 500 index composition using a "Hybrid Consensus" approach to solve for data gaps in the EODHD API.

## Overview

This engine addresses a common challenge in financial data: reconstructing accurate historical index compositions when dealing with imperfect data sources. It combines two complementary data methods with intelligent conflict resolution to produce high-fidelity daily records of S&P 500 constituents and weights.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    SP500HybridEngine                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────┐         ┌─────────────────┐                        │
│  │   Method A      │         │   Method B      │                        │
│  │  Master List    │         │  Backward Roll  │                        │
│  │   (Filter)      │         │    (Apply)      │                        │
│  └────────┬────────┘         └────────┬────────┘                        │
│           │                           │                                  │
│           └─────────┬─────────────────┘                                  │
│                     │                                                    │
│                     ▼                                                    │
│           ┌─────────────────┐                                           │
│           │  Reconciliation │                                           │
│           │     Engine      │                                           │
│           │  ┌───────────┐  │                                           │
│           │  │Bias Action│  │  Trust explicit add/remove events         │
│           │  ├───────────┤  │                                           │
│           │  │Bias Pres. │  │  Trust 100+ day members                   │
│           │  ├───────────┤  │                                           │
│           │  │Union Fall.│  │  Include if uncertain                     │
│           │  └───────────┘  │                                           │
│           └────────┬────────┘                                           │
│                    │                                                     │
│                    ▼                                                     │
│           ┌─────────────────┐                                           │
│           │Weight Calculator│                                           │
│           │ (Market Cap +   │                                           │
│           │ Sector Median   │                                           │
│           │   Fallback)     │                                           │
│           └────────┬────────┘                                           │
│                    │                                                     │
│                    ▼                                                     │
│           ┌─────────────────┐                                           │
│           │  JSON Output +  │                                           │
│           │ Quality Report  │                                           │
│           └─────────────────┘                                           │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

## The Problem

We have two imperfect data methods from the EODHD API:

1. **Method A (Master List)**: Historical components list with start/end dates
   - *Issue*: Sometimes missing start/end dates for older tickers

2. **Method B (Backward Roll)**: Current list + add/remove events applied backwards
   - *Issue*: Can drift if an event is missing from the change log

## The Solution: Hybrid Consensus Logic

### Step 1: Build Two Sets of Daily Lists
- **List A (The Filter)**: Generate daily ticker lists using the Master Historical Components (filtering by Start/End date)
- **List B (The Roll)**: Generate daily ticker lists by taking today's list and applying "Historical Changes" (adds/drops) backwards

### Step 2: Reconciliation Loop (The "Repair" Phase)
For each date, compare List A vs List B:

1. **If they match**: Proceed with the agreed-upon list
2. **If they differ**: Apply tie-breaker logic:
   - **Bias towards Action**: If a ticker appears in the Changes log (List B) with an explicit add/remove event near that date, trust List B
   - **Bias towards Presence**: If a ticker has been in List A for 100+ days but suddenly vanishes in List B without a specific "Remove" event, trust List A
   - **Union Fallback**: If unsure, take the union of both lists (better to include a small tail stock than miss a large one)

### Step 3: Weights & Sectors (The "Construction" Phase)
Once the Consensus List is finalized:
1. **Batch Price Fetch**: Query cached bulk EOD price data
2. **Market Cap Calculation**: Close × Shares Outstanding
   - *Refinement*: If Shares Outstanding is missing, use sector median market cap as fallback
3. **Weighting**: Calculate individual weights and aggregate by sector

## Installation

```bash
# Clone the repository
git clone https://github.com/stwilb4690/US-Large-Cap-Data.git
cd US-Large-Cap-Data

# Install dependencies
pip install -r requirements.txt
```

## Usage

### Command Line

```bash
# Demo mode (no API key required)
python sp500_engine.py demo

# Live mode with EODHD API
python sp500_engine.py YOUR_API_KEY
```

### Python API

```python
from sp500_engine import SP500HybridEngine, create_demo_engine

# Demo mode
engine = create_demo_engine()

# Or with API key
engine = SP500HybridEngine(api_key="your_api_key")

# Reconstruct for a date range
results = engine.reconstruct(
    start_date="2025-01-01",
    end_date="2025-01-31",
    frequency="daily"  # or "weekly", "monthly"
)

# Generate quality report
metrics = engine.generate_quality_report(results)
engine.print_quality_report(metrics)

# Export to JSON
engine.to_json(results, "output.json")
```

## Output Format

The engine produces JSON output keyed by date:

```json
{
  "2025-01-03": {
    "date": "2025-01-03",
    "source_agreement": "99.8%",
    "agreement_value": 99.8,
    "conflict_tickers": ["XYZ", "ABC"],
    "final_count": 503,
    "method_a_count": 502,
    "method_b_count": 501,
    "resolution_stats": {
      "bias_towards_action": 1,
      "bias_towards_presence": 2,
      "union_fallback": 0
    },
    "sectors": {
      "Technology": {
        "count": 75,
        "weight": 29.5,
        "constituents": ["AAPL", "MSFT", "NVDA", "..."]
      },
      "Healthcare": {
        "count": 63,
        "weight": 12.8,
        "constituents": ["UNH", "JNJ", "LLY", "..."]
      }
    },
    "constituents": {
      "AAPL": {
        "ticker": "AAPL",
        "name": "Apple Inc.",
        "sector": "Technology",
        "market_cap": 3000000000000,
        "shares_outstanding": 15500000000,
        "close_price": 193.55,
        "weight": 7.234567,
        "source": "both",
        "membership_days": 4523,
        "market_cap_estimated": false
      }
    }
  }
}
```

## Data Quality Report

The engine generates a comprehensive data quality report:

```
======================================================================
                    DATA QUALITY REPORT
======================================================================

==========================PROCESSING SUMMARY==========================
  Total dates processed:              22
  Days with conflicts:                 5
  Conflict-free days:                 17

===========================AGREEMENT METRICS===========================
  Average agreement:              99.60%
  Minimum agreement:              98.80%
  Maximum agreement:             100.00%
  Std deviation:                   0.35%

=========================CONFLICT RESOLUTION==========================
  Total conflicts resolved:           12
  Resolution breakdown:
    - bias_towards_action               4
    - bias_towards_presence             5
    - union_fallback                    3

========================TOP 10 CONFLICT TICKERS========================
    SMCI         5 conflicts  █████
    WHR          3 conflicts  ███
    UBER         2 conflicts  ██

========================CONSTITUENT STATISTICS=========================
  Average count:                   500.5
  Minimum count:                     500
  Maximum count:                     503

============================MARKET CAP DATA============================
  Missing market cap entries:          0
  (Filled using sector-median fallback)

==================SECTOR COVERAGE (max constituents)===================
    Technology                    75  ███████████████
    Healthcare                    63  ████████████
    Financials                    71  ██████████████
    Consumer Discretionary        58  ███████████
    ...
```

## Classes

### SP500HybridEngine

Main engine class for reconstruction.

```python
class SP500HybridEngine:
    def __init__(
        self,
        api_key: Optional[str] = None,
        demo_mode: bool = False,
        verbose: bool = True
    )

    def reconstruct(
        self,
        start_date: str,
        end_date: str,
        frequency: str = "daily"
    ) -> Dict[str, DailyIndexRecord]

    def generate_quality_report(
        self,
        results: Dict[str, DailyIndexRecord]
    ) -> DataQualityMetrics

    def print_quality_report(self, metrics: DataQualityMetrics) -> None

    def to_json(
        self,
        results: Dict[str, DailyIndexRecord],
        filepath: Optional[str] = None,
        compact: bool = False
    ) -> str
```

### EODHDClient

API client with rate limiting, caching, and error recovery.

```python
class EODHDClient:
    def __init__(self, api_key: str, cache_ttl: int = 3600)
    def get_index_components(self, index: str = "GSPC.INDX") -> Dict
    def get_index_changes(self, index: str = "GSPC.INDX") -> List
    def get_eod_bulk(self, exchange: str = "US", date: Optional[str] = None) -> List
    def get_fundamentals(self, ticker: str) -> Dict
    def get_request_stats(self) -> Dict
```

### Data Classes

```python
@dataclass
class ConstituentRecord:
    ticker: str
    name: str
    sector: str
    market_cap: Optional[float]
    shares_outstanding: Optional[float]
    close_price: Optional[float]
    weight: float
    source: str
    membership_days: int
    market_cap_estimated: bool

@dataclass
class DailyIndexRecord:
    date: str
    source_agreement: str
    agreement_value: float
    conflict_tickers: List[str]
    final_count: int
    sectors: Dict[str, Dict[str, Any]]
    constituents: Dict[str, Dict]
    resolution_stats: Dict[str, int]
    method_a_count: int
    method_b_count: int

@dataclass
class DataQualityMetrics:
    total_dates_processed: int
    dates_with_conflicts: int
    conflict_free_dates: int
    avg_source_agreement: float
    min_source_agreement: float
    max_source_agreement: float
    std_source_agreement: float
    total_conflicts_resolved: int
    conflicts_by_resolution: Dict[str, int]
    top_conflict_tickers: List[Tuple[str, int]]
    missing_market_cap_count: int
    avg_constituent_count: float
    min_constituent_count: int
    max_constituent_count: int
    sector_coverage: Dict[str, int]
```

## Demo Mode

The engine includes a comprehensive demo mode with simulated data for testing without an API key:

```python
# Using factory function
engine = create_demo_engine()

# Or directly
engine = SP500HybridEngine(demo_mode=True)
```

Demo mode includes:
- ~500 simulated constituents
- Realistic sector distribution
- Historical change events
- Market cap and price data
- Demonstrates all conflict resolution scenarios

## Dependencies

- Python 3.8+
- pandas >= 2.0.0
- numpy >= 1.24.0
- requests >= 2.28.0

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.
