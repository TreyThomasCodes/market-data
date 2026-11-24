"""Write tests/fixtures/thetadata_calendar.json: ThetaData's calendar_year for 2012-2027, through the service's own
fetch code. Production; logs in once. Usage: calendar_fixture.py"""
import json, os
from market_data.config import load_settings
from market_data.thetadata import fetch_calendar_years

ROOT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..")
got = fetch_calendar_years(load_settings(os.path.join(ROOT, ".env")).thetadata_api_key, range(2012, 2028))
out = {str(y): [{"date": d.date.isoformat(), "status": d.status, "open": d.open and d.open.isoformat(),
                 "close": d.close and d.close.isoformat()} for d in rows] for y, rows in got.items()}
assert all(out.values()), [y for y, v in out.items() if not v]
path = os.path.join(ROOT, "tests", "fixtures", "thetadata_calendar.json")
os.makedirs(os.path.dirname(path), exist_ok=True)
with open(path, "w") as f:
    json.dump(out, f, indent=1)
print(path, sum(len(v) for v in out.values()), "rows")
