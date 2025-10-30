venv: ; python3 -m venv .venv && . .venv/bin/activate && pip install -r requirements.txt
etl:  ; . .venv/bin/activate && python scripts/etl_yellow_taxi.py
agg:  ; . .venv/bin/activate && python scripts/agg_daily_tips.py
zones:; . .venv/bin/activate && python scripts/join_zones.py
dq:   ; . .venv/bin/activate && python scripts/dq_checks.py
plot: ; . .venv/bin/activate && python scripts/plot_daily.py
all: venv etl agg zones dq plot
