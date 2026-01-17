import os
import json
import time
import random
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional

import httpx
from openai import OpenAI

# -----------------------------
# Configuration
# -----------------------------
MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
BASE_DIR = Path(__file__).resolve().parent
PROMPTS_DIR = BASE_DIR / "prompts"
RUNS_DIR = BASE_DIR / "runs"

# TwelveData API configuration
TWELVEDATA_API_KEY = os.getenv("TWELVEDATA_API_KEY", "")
TWELVEDATA_BASE_URL = "https://api.twelvedata.com"

# Optional: Study parameters
ALLOWED_SYMBOLS = {"EURUSD", "AUDJPY"}
SYMBOLS_STR = os.getenv("STUDY_SYMBOLS", "EURUSD,AUDJPY")
REQUESTED_SYMBOLS = [s.strip() for s in SYMBOLS_STR.split(",") if s.strip()]
IGNORED_SYMBOLS = [s for s in REQUESTED_SYMBOLS if s not in ALLOWED_SYMBOLS]
SYMBOLS = [s for s in REQUESTED_SYMBOLS if s in ALLOWED_SYMBOLS]

# Legacy support: single symbol via STUDY_SYMBOL
if len(SYMBOLS) == 0:
    SYMBOL = os.getenv("STUDY_SYMBOL", "EURUSD")
    SYMBOLS = [SYMBOL]
else:
    SYMBOL = SYMBOLS[0]  # For backward compatibility

TIMEFRAME = os.getenv("STUDY_TIMEFRAME", "5m")
NOTES = os.getenv("STUDY_NOTES", "")
RUN_TIMES_LOCAL = os.getenv("STUDY_RUN_TIMES_LOCAL", "10:00,15:00")

MARKET_DATA_JSON_PATH = os.getenv("MARKET_DATA_JSON_PATH", "")


# -----------------------------
# JSON Schema for structured output
# -----------------------------
SIGNAL_SCHEMA: Dict[str, Any] = {
    "name": "trading_signal_output",
    "schema": {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "symbol": {"type": "string"},
            "timeframe": {"type": "string"},
            "timestamp_utc": {"type": "string"},
            "signal": {"type": "string", "enum": ["BUY", "SELL", "HOLD"]},
            "confidence": {"type": "number", "minimum": 0, "maximum": 1},
            "entry": {"type": ["number", "null"]},
            "stop": {"type": ["number", "null"]},
            "targets": {
                "type": "array",
                "items": {"type": "number"},
                "maxItems": 5
            },
            "rationale": {"type": "string"},
            "invalidation": {"type": "string"},
            "prompt_name": {"type": "string"},
            "raw_notes": {"type": "string"},
            "current_price": {"type": ["number", "null"]},
            "entry_distance_pips": {"type": ["number", "null"]}
        },
        "required": [
            "symbol", "timeframe", "timestamp_utc", "signal", "confidence",
            "entry", "stop", "targets", "rationale", "invalidation",
            "prompt_name", "raw_notes", "current_price", "entry_distance_pips"
        ],
    },
}


@dataclass
class PromptSpec:
    name: str
    path: Path


PROMPTS: List[PromptSpec] = [
    PromptSpec("prompt1", PROMPTS_DIR / "prompt1.txt"),
    PromptSpec("prompt2", PROMPTS_DIR / "prompt2.txt"),
    PromptSpec("prompt3", PROMPTS_DIR / "prompt3.txt"),
    PromptSpec("prompt4", PROMPTS_DIR / "prompt4.txt"),
]


# -----------------------------
# Helpers
# -----------------------------
def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def safe_read_text(path: Path) -> str:
    if not path.exists():
        raise FileNotFoundError(f"Prompt file not found: {path}")
    content = path.read_text(encoding="utf-8").strip()
    if not content:
        raise ValueError(f"Prompt file is empty: {path}")
    return content


def get_pip_value(symbol: str) -> float:
    """Get pip value for a symbol."""
    pip_values = {
        "AUDJPY": 1.00,
        "EURUSD": 0.0100,
        "XAUUSD": 10.00,
        "GBPUSD": 0.0100,
        "GBPJPY": 1.00,
        "AUDUSD": 0.0100,
        "EURJPY": 1.00,
        "NZDUSD": 0.0100,
        "CADJPY": 1.00,
        "CHFJPY": 1.00,
        "USDJPY": 1.00,
    }
    # Default: try to infer from symbol
    if "JPY" in symbol:
        return 1.00
    elif "XAU" in symbol or "GOLD" in symbol.upper():
        return 10.00
    else:
        return 0.0100  # Default for major pairs


def calculate_pip_distance(entry: Optional[float], current_price: Optional[float], symbol: str) -> Optional[float]:
    """Calculate pip distance between entry and current price."""
    if entry is None or current_price is None:
        return None
    
    pip_value = get_pip_value(symbol)
    distance = abs(entry - current_price)
    pips = distance / pip_value
    return round(pips, 2)


def format_symbol_for_twelvedata(symbol: str) -> str:
    """
    Convert symbol format to TwelveData format.
    EURUSD -> EUR/USD, AUDJPY -> AUD/JPY, etc.
    """
    # For XAUUSD (Gold), use XAU/USD
    if symbol == "XAUUSD":
        return "XAU/USD"
    
    # For other pairs, insert / after first 3 characters
    if len(symbol) == 6:
        return f"{symbol[:3]}/{symbol[3:]}"
    
    # If already has /, return as is
    if "/" in symbol:
        return symbol
    
    # Default: try to split at 3 chars
    return symbol


def fetch_current_market_data(symbol: str) -> Optional[Dict[str, Any]]:
    """
    Fetch current market data using TwelveData API.
    Returns dict with current_price, timestamp, and price_source.
    """
    if not TWELVEDATA_API_KEY:
        print(f"Warning: TWELVEDATA_API_KEY not set. Cannot fetch real-time data for {symbol}.")
        return None
    
    try:
        # Format symbol for TwelveData (EURUSD -> EUR/USD)
        formatted_symbol = format_symbol_for_twelvedata(symbol)
        
        url = f"{TWELVEDATA_BASE_URL}/price"
        params = {
            "symbol": formatted_symbol,
            "apikey": TWELVEDATA_API_KEY
        }
        
        with httpx.Client(trust_env=False, http2=False, timeout=10.0) as client:
            response = client.get(url, params=params)
            response.raise_for_status()
            data = response.json()
        
        # Check for API errors
        if "code" in data:
            error_msg = data.get("message", "Unknown error")
            print(f"Warning: TwelveData API error for {symbol} ({formatted_symbol}): {error_msg}")
            return None
        
        # Extract price
        if "price" not in data:
            print(f"Warning: No price data in response for {symbol} ({formatted_symbol})")
            return None
        
        current_price = float(data["price"])
        timestamp = data.get("timestamp", int(time.time()))
        
        return {
            "current_price": current_price,
            "timestamp": timestamp,
            "price_source": "TwelveData",
            "symbol": symbol
        }
        
    except httpx.HTTPError as e:
        print(f"Warning: HTTP error fetching market data for {symbol}: {type(e).__name__}: {str(e)}")
        return None
    except (ValueError, KeyError) as e:
        print(f"Warning: Invalid response format for {symbol}: {type(e).__name__}: {str(e)}")
        return None
    except Exception as e:
        print(f"Warning: Could not fetch market data for {symbol}: {type(e).__name__}: {str(e)}")
        return None


def load_market_data() -> Optional[Dict[str, Any]]:
    """Load market data from file if path is provided."""
    if not MARKET_DATA_JSON_PATH:
        return None
    p = Path(MARKET_DATA_JSON_PATH)
    if not p.exists():
        raise FileNotFoundError(f"MARKET_DATA_JSON_PATH does not exist: {p}")
    return json.loads(p.read_text(encoding="utf-8"))


def render_prompt(template: str, variables: Dict[str, Any]) -> str:
    """Simple template substitution via {KEY}"""
    out = template
    for k, v in variables.items():
        out = out.replace("{" + k + "}", str(v))
    return out


def backoff_sleep(attempt: int) -> None:
    """Exponential backoff with jitter"""
    base = min(2 ** attempt, 30)
    time.sleep(base + random.uniform(0, 0.7))


def ensure_dirs() -> None:
    RUNS_DIR.mkdir(parents=True, exist_ok=True)


def save_json(path: Path, obj: Any) -> None:
    path.write_text(json.dumps(obj, ensure_ascii=False, indent=2), encoding="utf-8")


def append_markdown(path: Path, md: str) -> None:
    with path.open("a", encoding="utf-8") as f:
        f.write(md.rstrip() + "\n")


def generate_html_report(results: List[Dict[str, Any]], output_path: Path, timestamp_utc: str, symbols: List[str], timeframe: str) -> None:
    """Generate an HTML report with a table of all results including rationale and invalidation."""
    
    def get_signal_color(signal: str) -> str:
        """Return color for signal type."""
        colors = {
            "BUY": "#10b981",
            "SELL": "#ef4444",
            "HOLD": "#6b7280"
        }
        return colors.get(signal, "#000000")
    
    def format_targets(targets: List[float]) -> str:
        """Format targets array as string."""
        if not targets:
            return "-"
        return ", ".join([f"{t:.5f}" for t in targets])
    
    def format_number(value: Optional[float]) -> str:
        """Format number or return dash."""
        if value is None:
            return "-"
        return f"{value:.5f}"
    
    def format_confidence(conf: float) -> str:
        """Format confidence with color coding."""
        color = "#10b981" if conf >= 0.7 else "#f59e0b" if conf >= 0.4 else "#ef4444"
        return f'<span style="color: {color}; font-weight: bold;">{conf:.2f}</span>'
    
    # Sort results: by symbol, then by prompt_name
    sorted_results = sorted(results, key=lambda x: (x.get("symbol", ""), x.get("prompt_name", "")))
    
    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Signals Report - {timestamp_utc}</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1800px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2em;
            margin-bottom: 10px;
        }}
        
        .header .meta {{
            font-size: 0.9em;
            opacity: 0.9;
            margin-top: 10px;
        }}
        
        .controls {{
            padding: 20px 30px;
            background: #f8f9fa;
            border-bottom: 1px solid #dee2e6;
            display: flex;
            gap: 15px;
            flex-wrap: wrap;
            align-items: center;
        }}
        
        .controls label {{
            font-weight: 600;
            color: #495057;
        }}
        
        .controls select, .controls input {{
            padding: 8px 12px;
            border: 1px solid #ced4da;
            border-radius: 6px;
            font-size: 14px;
        }}
        
        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 20px 30px;
            background: #f8f9fa;
        }}
        
        .stat-card {{
            background: white;
            padding: 15px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        
        .stat-card .label {{
            font-size: 0.85em;
            color: #6c757d;
            margin-bottom: 5px;
        }}
        
        .stat-card .value {{
            font-size: 1.5em;
            font-weight: bold;
            color: #212529;
        }}
        
        .table-container {{
            overflow-x: auto;
            padding: 30px;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        
        thead {{
            background: #f8f9fa;
            position: sticky;
            top: 0;
            z-index: 10;
        }}
        
        th {{
            padding: 15px;
            text-align: left;
            font-weight: 600;
            color: #495057;
            border-bottom: 2px solid #dee2e6;
            white-space: nowrap;
        }}
        
        td {{
            padding: 12px 15px;
            border-bottom: 1px solid #e9ecef;
        }}
        
        tbody tr {{
            transition: background-color 0.2s;
        }}
        
        tbody tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .signal-badge {{
            display: inline-block;
            padding: 4px 12px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.85em;
            color: white;
        }}
        
        .text-cell {{
            max-width: 400px;
            word-wrap: break-word;
            line-height: 1.4;
            font-size: 0.9em;
        }}
        
        .targets-cell {{
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }}
        
        .hidden {{
            display: none;
        }}
        
        @media print {{
            body {{
                background: white;
                padding: 0;
            }}
            .controls {{
                display: none;
            }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Trading Signals Report</h1>
            <div class="meta">
                <div>Zeitpunkt: {timestamp_utc}</div>
                <div>Symbole: {', '.join(symbols)} | Timeframe: {timeframe}</div>
            </div>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="label">Gesamt Signale</div>
                <div class="value">{len(results)}</div>
            </div>
            <div class="stat-card">
                <div class="label">BUY Signale</div>
                <div class="value" style="color: #10b981;">{sum(1 for r in results if r.get('signal') == 'BUY')}</div>
            </div>
            <div class="stat-card">
                <div class="label">SELL Signale</div>
                <div class="value" style="color: #ef4444;">{sum(1 for r in results if r.get('signal') == 'SELL')}</div>
            </div>
            <div class="stat-card">
                <div class="label">HOLD Signale</div>
                <div class="value" style="color: #6b7280;">{sum(1 for r in results if r.get('signal') == 'HOLD')}</div>
            </div>
            <div class="stat-card">
                <div class="label">Durchschn. Confidence</div>
                <div class="value">{sum(r.get('confidence', 0) for r in results) / len(results) if results else 0:.2f}</div>
            </div>
        </div>
        
        <div class="controls">
            <label>
                Symbol filtern:
                <select id="symbolFilter" onchange="filterTable()">
                    <option value="">Alle</option>
                    {''.join([f'<option value="{s}">{s}</option>' for s in symbols])}
                </select>
            </label>
            <label>
                Signal filtern:
                <select id="signalFilter" onchange="filterTable()">
                    <option value="">Alle</option>
                    <option value="BUY">BUY</option>
                    <option value="SELL">SELL</option>
                    <option value="HOLD">HOLD</option>
                </select>
            </label>
            <label>
                Prompt filtern:
                <select id="promptFilter" onchange="filterTable()">
                    <option value="">Alle</option>
                    <option value="prompt1">Prompt 1</option>
                    <option value="prompt2">Prompt 2</option>
                    <option value="prompt3">Prompt 3</option>
                </select>
            </label>
            <label>
                <input type="text" id="searchBox" placeholder="Suche..." onkeyup="filterTable()" style="width: 200px;">
            </label>
        </div>
        
        <div class="table-container">
            <table id="resultsTable">
                <thead>
                    <tr>
                        <th>Symbol</th>
                        <th>Prompt</th>
                        <th>Signal</th>
                        <th>Confidence</th>
                        <th>Current Price</th>
                        <th>Entry</th>
                        <th>Stop</th>
                        <th>Targets</th>
                        <th>Entry Dist. (Pips)</th>
                        <th>Rationale</th>
                        <th>Invalidation</th>
                        <th>Zeitpunkt</th>
                    </tr>
                </thead>
                <tbody>
"""
    
    for result in sorted_results:
        signal = result.get("signal", "HOLD")
        signal_color = get_signal_color(signal)
        rationale = result.get("rationale", "-") or "-"
        invalidation = result.get("invalidation", "-") or "-"
        
        html += f"""                    <tr data-symbol="{result.get('symbol', '')}" 
                            data-signal="{signal}" 
                            data-prompt="{result.get('prompt_name', '')}">
                        <td><strong>{result.get('symbol', '-')}</strong></td>
                        <td>{result.get('prompt_name', '-')}</td>
                        <td>
                            <span class="signal-badge" style="background-color: {signal_color};">
                                {signal}
                            </span>
                        </td>
                        <td>{format_confidence(result.get('confidence', 0))}</td>
                        <td>{format_number(result.get('current_price'))}</td>
                        <td>{format_number(result.get('entry'))}</td>
                        <td>{format_number(result.get('stop'))}</td>
                        <td class="targets-cell">{format_targets(result.get('targets', []))}</td>
                        <td>{result.get('entry_distance_pips') if result.get('entry_distance_pips') is not None else '-'}</td>
                        <td class="text-cell">{rationale}</td>
                        <td class="text-cell">{invalidation}</td>
                        <td style="font-size: 0.85em; color: #6c757d;">{result.get('timestamp_utc', '-')}</td>
                    </tr>
"""
    
    html += """                </tbody>
            </table>
        </div>
    </div>
    
    <script>
        function filterTable() {
            const symbolFilter = document.getElementById('symbolFilter').value.toLowerCase();
            const signalFilter = document.getElementById('signalFilter').value;
            const promptFilter = document.getElementById('promptFilter').value;
            const searchBox = document.getElementById('searchBox').value.toLowerCase();
            
            const rows = document.querySelectorAll('#resultsTable tbody tr');
            
            rows.forEach(row => {
                const symbol = row.getAttribute('data-symbol').toLowerCase();
                const signal = row.getAttribute('data-signal');
                const prompt = row.getAttribute('data-prompt');
                const text = row.textContent.toLowerCase();
                
                const matchSymbol = !symbolFilter || symbol === symbolFilter.toLowerCase();
                const matchSignal = !signalFilter || signal === signalFilter;
                const matchPrompt = !promptFilter || prompt === promptFilter;
                const matchSearch = !searchBox || text.includes(searchBox);
                
                if (matchSymbol && matchSignal && matchPrompt && matchSearch) {
                    row.classList.remove('hidden');
                } else {
                    row.classList.add('hidden');
                }
            });
        }
    </script>
</body>
</html>"""
    
    output_path.write_text(html, encoding="utf-8")


def validate_entry_price(
    result: Dict[str, Any],
    current_price: Optional[float],
    symbol: str
) -> Dict[str, Any]:
    """
    Validate entry price against current market price.
    Returns validation result with potential signal override.
    """
    validation = {
        "valid": True,
        "original_signal": result.get("signal"),
        "final_signal": result.get("signal"),
        "violation_reason": None,
        "entry_distance_pips": None,
        "current_price": current_price,
        "warnings": []
    }
    
    # If HOLD, no validation needed
    if result.get("signal") == "HOLD":
        validation["entry_distance_pips"] = None
        return validation
    
    # If no current price, cannot validate
    if current_price is None:
        validation["warnings"].append("No current price available for validation")
        validation["entry_distance_pips"] = None
        return validation
    
    entry = result.get("entry")
    
    # If no entry, invalid
    if entry is None:
        validation["valid"] = False
        validation["final_signal"] = "HOLD"
        validation["violation_reason"] = "Entry price is null but signal is not HOLD"
        validation["warnings"].append("Entry price is null - forcing HOLD")
        return validation
    
    # Calculate pip distance
    pip_distance = calculate_pip_distance(entry, current_price, symbol)
    validation["entry_distance_pips"] = pip_distance
    
    # Check if within 100 pips
    if pip_distance is not None and pip_distance > 100:
        validation["valid"] = False
        validation["final_signal"] = "HOLD"
        validation["violation_reason"] = f"Entry price ({entry}) is {pip_distance:.2f} pips away from current price ({current_price}), exceeding 100 pip limit"
        validation["warnings"].append(f"Entry too far from current price: {pip_distance:.2f} pips (max 100)")
    
    # Check if entry is unrealistic (negative or zero for most symbols)
    if entry <= 0:
        validation["valid"] = False
        validation["final_signal"] = "HOLD"
        validation["violation_reason"] = f"Entry price ({entry}) is unrealistic (non-positive)"
        validation["warnings"].append("Entry price is non-positive - forcing HOLD")
    
    return validation


# -----------------------------
# OpenAI Call
# -----------------------------
def call_model_structured(
    client: OpenAI,
    prompt: str,
    prompt_name: str,
    symbol: str,
    timeframe: str,
    timestamp_utc: str,
    notes: str,
    current_price: Optional[float],
    price_source: str,
    price_timestamp: Optional[int],
    current_datetime: Optional[datetime] = None,
    max_retries: int = 5,
) -> Dict[str, Any]:
    """
    Call the model with structured outputs using JSON schema.
    Includes real-time price data and validation.
    """
    system = (
        "You are a trading signal analysis assistant for a study. "
        "Do NOT provide investment advice. Your role is to analyze market conditions and provide actionable trading signals. "
        "CRITICAL: You MUST use the REAL-TIME market price provided in the prompt. "
        "NEVER invent or estimate prices - use ONLY the current_price provided. "
        "SIGNAL GUIDELINES: "
        "- Prefer BUY or SELL signals when there is any reasonable basis for a directional view. "
        "- HOLD is acceptable ONLY when there is genuinely NO trade opportunity. "
        "- Be decisive: if you can identify any trend, pattern, or market condition, provide a BUY or SELL signal. "
        "ENTRY PRICE REQUIREMENTS (CRITICAL): "
        "- Entry price MUST be within 100 PIPS of the CURRENT market price provided. "
        "- For JPY pairs (AUDJPY): 100 pips = 1.00. "
        "- For major pairs (EURUSD): 100 pips = 0.0100. "
        "- For XAUUSD (Gold): 100 pips = 10.00. "
        "- Entry must be close to current market price - maximum 100 pips away. "
        "- If you cannot set an entry within 100 pips of current price, return HOLD. "
        "- Use the EXACT current_price provided as reference. "
        "TIMING REQUIREMENTS: "
        "- Trades must be executable within the NEXT HOUR from the current timestamp. "
        "- Entry prices must be realistic and achievable within 1 hour. "
        "- Trades should be designed for short-term execution on the 5-minute timeframe. "
        "- Maximum trade duration: 5 hours."
    )
    
    # Format current date/time info
    current_datetime = current_datetime or datetime.now(timezone.utc)
    current_date_str = current_datetime.strftime("%Y-%m-%d")
    current_time_str = current_datetime.strftime("%H:%M:%S UTC")
    current_day_name = current_datetime.strftime("%A")
    
    # Format price timestamp
    if price_timestamp:
        price_time = datetime.fromtimestamp(price_timestamp, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        price_time = "Unknown"
    
    user = (
        f"CURRENT DATE AND TIME:\n"
        f"- Date: {current_date_str} ({current_day_name})\n"
        f"- Time: {current_time_str}\n"
        f"- Timestamp UTC: {timestamp_utc}\n\n"
        f"REAL-TIME MARKET PRICE:\n"
        f"- Current market price = {current_price if current_price is not None else 'NOT AVAILABLE'}\n"
        f"- Price source: {price_source}\n"
        f"- Price timestamp: {price_time}\n\n"
        f"TRADING PARAMETERS:\n"
        f"- Symbol: {symbol}\n"
        f"- Timeframe: {timeframe}\n"
        f"- Prompt: {prompt_name}\n"
        f"- Notes: {notes}\n\n"
        f"CRITICAL: Entry price MUST be within 100 pips of the current market price ({current_price}). "
        f"Use this EXACT price as your reference. Do NOT estimate or invent prices.\n\n"
        f"TASK:\n{prompt}\n"
    )
    
    last_err = None
    for attempt in range(max_retries):
        try:
            response = client.chat.completions.create(
                model=MODEL,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user},
                ],
                response_format={
                    "type": "json_schema",
                    "json_schema": {
                        "name": SIGNAL_SCHEMA["name"],
                        "strict": True,
                        "schema": SIGNAL_SCHEMA["schema"],
                    }
                },
            )
            
            if not response.choices or not response.choices[0].message.content:
                raise ValueError("Empty response from OpenAI API")
            
            # Parse JSON response
            data = json.loads(response.choices[0].message.content)
            
            # Ensure required fields are set
            data["prompt_name"] = prompt_name
            data["symbol"] = data.get("symbol") or symbol
            data["timeframe"] = data.get("timeframe") or timeframe
            data["timestamp_utc"] = data.get("timestamp_utc") or timestamp_utc
            data["raw_notes"] = data.get("raw_notes") or notes
            data["current_price"] = current_price
            data["entry_distance_pips"] = calculate_pip_distance(
                data.get("entry"),
                current_price,
                symbol
            )
            
            return data
            
        except Exception as e:
            last_err = f"{type(e).__name__}: {str(e)}"
            print(f"Attempt {attempt + 1}/{max_retries} failed: {last_err}")
            if attempt < max_retries - 1:
                backoff_sleep(attempt)
    
    raise RuntimeError(
        f"OpenAI call failed after {max_retries} attempts. Last error: {last_err}"
    )


def main() -> None:
    """Main function to run all prompts and save results."""
    try:
        ensure_dirs()
        if IGNORED_SYMBOLS:
            print(f"⚠ Ignoring unsupported symbols: {', '.join(IGNORED_SYMBOLS)}")
        
        # Create httpx client with specified parameters
        http_client = httpx.Client(
            trust_env=False,
            http2=False,
            timeout=60.0,
        )
        
        # Create OpenAI client with custom http_client
        client = OpenAI(
            api_key=os.environ.get("OPENAI_API_KEY"),
            http_client=http_client,
        )
        
        timestamp_utc = utc_now_iso()
        run_day = datetime.now().strftime("%Y-%m-%d")
        out_dir = RUNS_DIR / run_day
        out_dir.mkdir(parents=True, exist_ok=True)
        
        # Variables for prompt templates
        try:
            market_data = load_market_data()
        except Exception as e:
            print(f"Warning: Could not load market data: {type(e).__name__}: {str(e)}")
            market_data = None
        
        all_results: List[Dict[str, Any]] = []
        
        # Get current date/time info
        current_datetime = datetime.now(timezone.utc)
        current_date_str = current_datetime.strftime("%Y-%m-%d")
        current_time_str = current_datetime.strftime("%H:%M:%S UTC")
        current_day_name = current_datetime.strftime("%A")
        
        # Process each symbol
        for symbol in SYMBOLS:
            print(f"\n{'='*60}")
            print(f"Processing symbol: {symbol}")
            print(f"{'='*60}\n")
            
            # Fetch current market data for this symbol
            market_data_dict = fetch_current_market_data(symbol)
            
            if not market_data_dict:
                print(f"  ⚠ WARNING: Could not fetch real-time price for {symbol}. Continuing without validation.")
                current_price = None
                price_source = "Not Available"
                price_timestamp = None
            else:
                current_price = market_data_dict.get("current_price")
                price_source = market_data_dict.get("price_source", "Unknown")
                price_timestamp = market_data_dict.get("timestamp")
                print(f"  ✓ Current price: {current_price} (Source: {price_source})")
            
            variables = {
                "SYMBOL": symbol,
                "TIMEFRAME": TIMEFRAME,
                "TIMESTAMP_UTC": timestamp_utc,
                "CURRENT_DATE": current_date_str,
                "CURRENT_TIME": current_time_str,
                "CURRENT_DAY": current_day_name,
                "CURRENT_PRICE": current_price if current_price is not None else "NOT AVAILABLE",
                "PRICE_SOURCE": price_source,
                "PRICE_TIMESTAMP": price_timestamp if price_timestamp else "Unknown",
                "MARKET_DATA_JSON": json.dumps(market_data, ensure_ascii=False) if market_data else "",
                "NOTES": NOTES,
            }
            
            # Create symbol-specific markdown report
            md_path = out_dir / f"{timestamp_utc.replace(':', '-')}_{symbol}_report.md"
            append_markdown(md_path, f"# Run {timestamp_utc} - {symbol}\n")
            append_markdown(
                md_path,
                f"- Symbol: {symbol}\n- Timeframe: {TIMEFRAME}\n- Run Times (Local): {RUN_TIMES_LOCAL}\n- Notes: {NOTES}\n",
            )
            if current_price:
                append_markdown(md_path, f"- Current Price: {current_price} (Source: {price_source})\n")
            
            # Process each prompt for this symbol
            for spec in PROMPTS:
                try:
                    print(f"Processing {symbol} - {spec.name}...")
                    template = safe_read_text(spec.path)
                    prompt = render_prompt(template, variables)
                    
                    result = call_model_structured(
                        client=client,
                        prompt=prompt,
                        prompt_name=spec.name,
                        symbol=symbol,
                        timeframe=TIMEFRAME,
                        timestamp_utc=timestamp_utc,
                        notes=NOTES,
                        current_price=current_price,
                        price_source=price_source,
                        price_timestamp=price_timestamp,
                    )
                    
                    # Validate entry price
                    validation = validate_entry_price(result, current_price, symbol)
                    
                    # Apply validation result
                    if not validation["valid"]:
                        print(f"  ⚠ WARNING: Entry price validation failed for {symbol} - {spec.name}")
                        print(f"     Reason: {validation['violation_reason']}")
                        for warning in validation["warnings"]:
                            print(f"     - {warning}")
                        
                        # Override signal to HOLD
                        result["signal"] = validation["final_signal"]
                        result["validation"] = validation
                        result["original_signal"] = validation["original_signal"]
                    else:
                        result["validation"] = validation
                        if validation["entry_distance_pips"] is not None:
                            print(f"  ✓ Entry distance: {validation['entry_distance_pips']:.2f} pips")
                    
                    all_results.append(result)
                    
                    # Save individual prompt result with symbol in filename
                    json_path = out_dir / f"{timestamp_utc.replace(':', '-')}_{symbol}_{spec.name}.json"
                    save_json(json_path, result)
                    
                    # Append to markdown report
                    append_markdown(md_path, f"\n## {spec.name}\n")
                    append_markdown(md_path, f"- Signal: **{result['signal']}**\n")
                    if validation.get("original_signal") != result["signal"]:
                        append_markdown(md_path, f"- Original Signal: {validation['original_signal']} (overridden by validation)\n")
                    append_markdown(md_path, f"- Confidence: {result['confidence']}\n")
                    append_markdown(md_path, f"- Current Price: {current_price if current_price else 'N/A'}\n")
                    append_markdown(md_path, f"- Entry: {result['entry']}\n- Stop: {result['stop']}\n")
                    append_markdown(md_path, f"- Entry Distance: {validation['entry_distance_pips'] if validation['entry_distance_pips'] else 'N/A'} pips\n")
                    append_markdown(md_path, f"- Targets: {result['targets']}\n")
                    append_markdown(md_path, f"- Rationale: {result['rationale']}\n")
                    append_markdown(md_path, f"- Invalidation: {result['invalidation']}\n")
                    if validation.get("warnings"):
                        append_markdown(md_path, f"- Validation Warnings: {', '.join(validation['warnings'])}\n")
                    
                    print(f"  ✓ {symbol} - {spec.name} completed successfully")
                    
                except Exception as e:
                    error_msg = f"{type(e).__name__}: {str(e)}"
                    print(f"  ✗ {symbol} - {spec.name} failed: {error_msg}")
                    raise
        
        # Save combined results for all symbols
        combined_path = out_dir / f"{timestamp_utc.replace(':', '-')}_all.json"
        save_json(combined_path, all_results)
        
        # Generate HTML report for this run
        html_path = out_dir / f"{timestamp_utc.replace(':', '-')}_report.html"
        generate_html_report(all_results, html_path, timestamp_utc, SYMBOLS, TIMEFRAME)
        print(f"  ✓ HTML report generated: {html_path.name}")
        
        # Update overall overview HTML
        try:
            all_historical_results = []
            for day_dir in sorted(RUNS_DIR.iterdir()):
                if not day_dir.is_dir():
                    continue
                for json_file in day_dir.glob("*_all.json"):
                    try:
                        with open(json_file, 'r', encoding='utf-8') as f:
                            results = json.load(f)
                            if isinstance(results, list):
                                all_historical_results.extend(results)
                            else:
                                all_historical_results.append(results)
                    except Exception:
                        pass
            
            if all_historical_results:
                import sys
                sys.path.insert(0, str(BASE_DIR))
                from generate_all_reports import generate_overview_html
                
                overview_path = RUNS_DIR / "all_runs_overview.html"
                generate_overview_html(all_historical_results, overview_path)
                print(f"  ✓ Overall overview updated: {overview_path.name}")
        except Exception as e:
            print(f"  ⚠ Could not update overview: {type(e).__name__}: {str(e)}")
        
        # Create summary markdown
        summary_path = out_dir / f"{timestamp_utc.replace(':', '-')}_summary.md"
        append_markdown(summary_path, f"# Summary - Run {timestamp_utc}\n")
        append_markdown(summary_path, f"- Symbols: {', '.join(SYMBOLS)}\n")
        append_markdown(summary_path, f"- Timeframe: {TIMEFRAME}\n")
        append_markdown(summary_path, f"- Run Times (Local): {RUN_TIMES_LOCAL}\n")
        append_markdown(summary_path, f"- Total prompts processed: {len(all_results)}\n")
        
        print(f"\n✓ Success: All results saved to {out_dir}")
        print(f"  Processed {len(SYMBOLS)} symbol(s): {', '.join(SYMBOLS)}")
        print(f"  Total results: {len(all_results)}")
        
    except Exception as e:
        error_msg = f"{type(e).__name__}: {str(e)}"
        print(f"\n✗ Fatal error: {error_msg}")
        raise
    
    finally:
        # Clean up http client
        if 'http_client' in locals():
            http_client.close()


if __name__ == "__main__":
    main()
