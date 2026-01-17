#!/usr/bin/env python3
"""
Generate HTML overview of all runs across all days.
Usage: python generate_all_reports.py
"""

import json
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

BASE_DIR = Path(__file__).resolve().parent
RUNS_DIR = BASE_DIR / "runs"


def load_all_results() -> List[Dict[str, Any]]:
    """Load all results from all run directories."""
    all_results = []
    
    for day_dir in sorted(RUNS_DIR.iterdir()):
        if not day_dir.is_dir():
            continue
        
        # Find all _all.json files in this day
        for json_file in day_dir.glob("*_all.json"):
            try:
                with open(json_file, 'r', encoding='utf-8') as f:
                    results = json.load(f)
                    if isinstance(results, list):
                        all_results.extend(results)
                    else:
                        all_results.append(results)
            except Exception as e:
                print(f"Warning: Could not load {json_file}: {e}")
    
    return all_results


def generate_overview_html(results: List[Dict[str, Any]], output_path: Path) -> None:
    """Generate HTML overview of all runs."""
    
    # Group by date
    by_date: Dict[str, List[Dict[str, Any]]] = {}
    for result in results:
        timestamp = result.get('timestamp_utc', '')
        date = timestamp.split('T')[0] if 'T' in timestamp else 'unknown'
        if date not in by_date:
            by_date[date] = []
        by_date[date].append(result)
    
    # Sort dates
    sorted_dates = sorted(by_date.keys(), reverse=True)
    prompt_names = sorted({r.get('prompt_name') for r in results if r.get('prompt_name')})
    
    html = f"""<!DOCTYPE html>
<html lang="de">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Trading Signals - Alle Runs</title>
    <style>
        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}
        
        body {{
            font-family: "Avenir Next", "Avenir", "Gill Sans", "Trebuchet MS", sans-serif;
            background: linear-gradient(135deg, #0b4f6c 0%, #0f766e 100%);
            padding: 20px;
            min-height: 100vh;
        }}
        
        .container {{
            max-width: 1600px;
            margin: 0 auto;
            background: white;
            border-radius: 12px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            overflow: hidden;
        }}
        
        .header {{
            background: linear-gradient(135deg, #0b4f6c 0%, #0f766e 100%);
            color: white;
            padding: 30px;
            text-align: center;
        }}
        
        .header h1 {{
            font-size: 2.5em;
            margin-bottom: 10px;
        }}
        
        .controls {{
            padding: 20px 30px 10px;
            background: #f5f7f9;
            border-bottom: 1px solid #e2e8f0;
        }}

        .filters {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 12px;
            align-items: end;
        }}

        .filter-group label {{
            display: block;
            font-size: 0.85em;
            color: #475569;
            margin-bottom: 6px;
            font-weight: 600;
        }}

        .filter-group select {{
            width: 100%;
            padding: 10px 12px;
            border-radius: 8px;
            border: 1px solid #cbd5f5;
            background: white;
            font-size: 0.95em;
        }}

        .filter-actions {{
            display: flex;
            gap: 10px;
            align-items: center;
        }}

        .filter-actions button {{
            border: 0;
            border-radius: 999px;
            padding: 10px 14px;
            font-weight: 600;
            cursor: pointer;
            background: #0b4f6c;
            color: white;
        }}

        .filter-actions button.secondary {{
            background: #e2e8f0;
            color: #0f172a;
        }}

        .stats {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            padding: 30px;
            background: #f8f9fa;
        }}
        
        .stat-card {{
            background: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
            animation: rise 0.6s ease both;
        }}
        
        .stat-card .label {{
            font-size: 0.9em;
            color: #6c757d;
            margin-bottom: 8px;
        }}
        
        .stat-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #212529;
        }}
        
        .date-section {{
            margin: 30px;
            border: 1px solid #dee2e6;
            border-radius: 8px;
            overflow: hidden;
            animation: rise 0.6s ease both;
        }}
        
        .date-header {{
            background: #f8f9fa;
            padding: 15px 20px;
            font-weight: 600;
            font-size: 1.2em;
            border-bottom: 2px solid #dee2e6;
        }}
        
        .table-container {{
            overflow-x: auto;
        }}
        
        table {{
            width: 100%;
            border-collapse: collapse;
            font-size: 13px;
        }}
        
        th {{
            padding: 12px;
            text-align: left;
            font-weight: 600;
            background: #f8f9fa;
            border-bottom: 2px solid #dee2e6;
            position: sticky;
            top: 0;
        }}
        
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #e9ecef;
        }}
        
        tbody tr:hover {{
            background-color: #f8f9fa;
        }}
        
        .signal-badge {{
            display: inline-block;
            padding: 4px 10px;
            border-radius: 12px;
            font-weight: 600;
            font-size: 0.85em;
            color: white;
        }}
        
        .signal-BUY {{ background-color: #10b981; }}
        .signal-SELL {{ background-color: #ef4444; }}
        .signal-HOLD {{ background-color: #6b7280; }}
        
        .text-cell {{
            max-width: 400px;
            word-wrap: break-word;
            line-height: 1.4;
            font-size: 0.9em;
        }}

        @keyframes rise {{
            from {{ transform: translateY(10px); opacity: 0; }}
            to {{ transform: translateY(0); opacity: 1; }}
        }}

        @media (max-width: 900px) {{
            .date-section {{ margin: 20px; }}
            .stats {{ padding: 20px; }}
            .controls {{ padding: 20px; }}
        }}
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📊 Trading Signals - Alle Runs</h1>
            <p>Übersicht über alle bisherigen Analysen</p>
        </div>

        <div class="controls">
            <div class="filters">
                <div class="filter-group">
                    <label for="promptFilter">Prompt</label>
                    <select id="promptFilter">
                        <option value="ALL">Alle Prompts</option>
                        {"".join([f'<option value="{name}">{name}</option>' for name in prompt_names])}
                    </select>
                </div>
                <div class="filter-group">
                    <label for="signalFilter">Signal</label>
                    <select id="signalFilter">
                        <option value="ALL">Alle Signale</option>
                        <option value="BUY">BUY</option>
                        <option value="SELL">SELL</option>
                        <option value="HOLD">HOLD</option>
                    </select>
                </div>
                <div class="filter-group">
                    <label for="dateFilter">Tag</label>
                    <select id="dateFilter">
                        <option value="ALL">Alle Tage</option>
                        {"".join([f'<option value="{d}">{d}</option>' for d in sorted_dates])}
                    </select>
                </div>
                <div class="filter-actions">
                    <button id="applyFilters" type="button">Filtern</button>
                    <button id="resetFilters" class="secondary" type="button">Zurücksetzen</button>
                </div>
            </div>
        </div>
        
        <div class="stats">
            <div class="stat-card">
                <div class="label">Gesamt Signale</div>
                <div class="value" id="kpiTotal">{len(results)}</div>
            </div>
            <div class="stat-card">
                <div class="label">BUY Signale</div>
                <div class="value" id="kpiBuy" style="color: #10b981;">{sum(1 for r in results if r.get('signal') == 'BUY')}</div>
            </div>
            <div class="stat-card">
                <div class="label">SELL Signale</div>
                <div class="value" id="kpiSell" style="color: #ef4444;">{sum(1 for r in results if r.get('signal') == 'SELL')}</div>
            </div>
            <div class="stat-card">
                <div class="label">HOLD Signale</div>
                <div class="value" id="kpiHold" style="color: #6b7280;">{sum(1 for r in results if r.get('signal') == 'HOLD')}</div>
            </div>
            <div class="stat-card">
                <div class="label">Anzahl Tage</div>
                <div class="value" id="kpiDays">{len(sorted_dates)}</div>
            </div>
            <div class="stat-card">
                <div class="label">BUY/SELL Ratio</div>
                <div class="value" id="kpiRatio">-</div>
            </div>
        </div>
"""
    
    for date in sorted_dates:
        date_results = by_date[date]
        html += f"""
        <div class="date-section">
            <div class="date-header">
                📅 {date} ({len(date_results)} Signale)
            </div>
            <div class="table-container">
                <table>
                    <thead>
                        <tr>
                            <th>Symbol</th>
                            <th>Prompt</th>
                            <th>Signal</th>
                            <th>Confidence</th>
                            <th>Entry</th>
                            <th>Stop</th>
                            <th>Targets</th>
                            <th>Rationale</th>
                            <th>Invalidation</th>
                            <th>Zeitpunkt</th>
                        </tr>
                    </thead>
                    <tbody>
"""
        for result in sorted(date_results, key=lambda x: (x.get('symbol', ''), x.get('prompt_name', ''))):
            signal = result.get('signal', 'HOLD')
            rationale = result.get('rationale', '-') or '-'
            invalidation = result.get('invalidation', '-') or '-'
            html += f"""
                        <tr data-date="{date}" data-signal="{signal}" data-prompt="{result.get('prompt_name', '-')}">
                            <td><strong>{result.get('symbol', '-')}</strong></td>
                            <td>{result.get('prompt_name', '-')}</td>
                            <td><span class="signal-badge signal-{signal}">{signal}</span></td>
                            <td>{result.get('confidence', 0):.2f}</td>
                            <td>{result.get('entry') or '-'}</td>
                            <td>{result.get('stop') or '-'}</td>
                            <td>{', '.join([str(t) for t in result.get('targets', [])]) or '-'}</td>
                            <td class="text-cell">{rationale}</td>
                            <td class="text-cell">{invalidation}</td>
                            <td style="font-size: 0.85em; color: #6c757d;">{result.get('timestamp_utc', '-')}</td>
                        </tr>
"""
        html += """
                    </tbody>
                </table>
            </div>
        </div>
"""
    
    html += """
    </div>
    <script>
        const promptFilter = document.getElementById('promptFilter');
        const signalFilter = document.getElementById('signalFilter');
        const dateFilter = document.getElementById('dateFilter');
        const applyButton = document.getElementById('applyFilters');
        const resetButton = document.getElementById('resetFilters');

        const kpiTotal = document.getElementById('kpiTotal');
        const kpiBuy = document.getElementById('kpiBuy');
        const kpiSell = document.getElementById('kpiSell');
        const kpiHold = document.getElementById('kpiHold');
        const kpiDays = document.getElementById('kpiDays');
        const kpiRatio = document.getElementById('kpiRatio');

        function applyFilters() {
            const promptValue = promptFilter.value;
            const signalValue = signalFilter.value;
            const dateValue = dateFilter.value;

            let visibleCount = 0;
            const signalCounts = { BUY: 0, SELL: 0, HOLD: 0 };
            const visibleDates = new Set();

            document.querySelectorAll('tbody tr').forEach((row) => {
                const rowPrompt = row.dataset.prompt;
                const rowSignal = row.dataset.signal;
                const rowDate = row.dataset.date;

                const matchPrompt = promptValue === 'ALL' || rowPrompt === promptValue;
                const matchSignal = signalValue === 'ALL' || rowSignal === signalValue;
                const matchDate = dateValue === 'ALL' || rowDate === dateValue;

                if (matchPrompt && matchSignal && matchDate) {
                    row.style.display = '';
                    visibleCount += 1;
                    if (signalCounts[rowSignal] !== undefined) {
                        signalCounts[rowSignal] += 1;
                    }
                    visibleDates.add(rowDate);
                } else {
                    row.style.display = 'none';
                }
            });

            document.querySelectorAll('.date-section').forEach((section) => {
                const rows = section.querySelectorAll('tbody tr');
                let anyVisible = false;
                rows.forEach((row) => {
                    if (row.style.display !== 'none') {
                        anyVisible = true;
                    }
                });
                section.style.display = anyVisible ? '' : 'none';
            });

            kpiTotal.textContent = visibleCount;
            kpiBuy.textContent = signalCounts.BUY;
            kpiSell.textContent = signalCounts.SELL;
            kpiHold.textContent = signalCounts.HOLD;
            kpiDays.textContent = visibleDates.size;
            if (signalCounts.SELL > 0) {
                kpiRatio.textContent = (signalCounts.BUY / signalCounts.SELL).toFixed(2);
            } else if (signalCounts.BUY > 0) {
                kpiRatio.textContent = '∞';
            } else {
                kpiRatio.textContent = '0';
            }
        }

        applyButton.addEventListener('click', applyFilters);
        resetButton.addEventListener('click', () => {
            promptFilter.value = 'ALL';
            signalFilter.value = 'ALL';
            dateFilter.value = 'ALL';
            applyFilters();
        });

        applyFilters();
    </script>
</body>
</html>"""
    
    output_path.write_text(html, encoding="utf-8")


def main():
    """Main function."""
    print("Loading all results...")
    all_results = load_all_results()
    
    if not all_results:
        print("No results found!")
        return
    
    print(f"Found {len(all_results)} results")
    
    output_path = BASE_DIR / "runs" / "all_runs_overview.html"
    generate_overview_html(all_results, output_path)
    
    print(f"✓ Overview generated: {output_path}")


if __name__ == "__main__":
    main()
