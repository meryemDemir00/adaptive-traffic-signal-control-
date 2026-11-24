"""Grafana dashboard'unu config/approaches.yaml icindeki kavsaklardan uretir.

Kullanim (repo kokunden):
    python grafana/make_dashboard.py

Farkli bir yol agi kullanildiginda approaches.yaml guncellenir ve bu betik
tekrar calistirilir; dashboard yeni kavsak kimlikleri ve adlariyla yeniden uretilir.
Cikti: grafana/provisioning/dashboards/traffic-dashboard.json
"""
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent.parent
OUT = Path(sys.argv[1]) if len(sys.argv) > 1 else (
    ROOT / "grafana" / "provisioning" / "dashboards" / "traffic-dashboard.json")

with open(ROOT / "config" / "approaches.yaml", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

TLS = [(tid, str(v.get("name", tid)).replace("`", "").replace("'", ""))
       for tid, v in cfg["intersections"].items()]

DS = {"type": "grafana-clickhouse-datasource", "uid": "clickhouse"}
TIME_M = "toDateTime(toUInt32(sim_time_end), 'UTC')"
TIME_C = "toDateTime(toUInt32(applied_sim_time), 'UTC')"


def name_expr(col):
    ids = ", ".join("'%s'" % t for t, _ in TLS)
    names = ", ".join("'%s'" % n for _, n in TLS)
    return "transform(%s, [%s], [%s], %s)" % (col, ids, names, col)


def target(sql, fmt, ref="A"):
    return {
        "datasource": DS,
        "editorType": "sql",
        "format": 0 if fmt == "table" else 2,
        "queryType": fmt,
        "rawSql": sql,
        "refId": ref,
    }


def pivot_metrics(col):
    cols = ", ".join("maxIf(%s, tls_id='%s') AS `%s`" % (col, t, n) for t, n in TLS)
    return ("SELECT %s AS time, %s FROM traffic.metrics "
            "WHERE run_id='${run_id}' GROUP BY time ORDER BY time") % (TIME_M, cols)


def pivot_plan():
    cols = ", ".join("maxIf(toInt32OrNull(program_id), tls_id='%s') AS `%s`" % (t, n)
                     for t, n in TLS)
    return ("SELECT %s AS time, %s FROM traffic.control_events "
            "WHERE run_id='${run_id}' GROUP BY time ORDER BY time") % (TIME_C, cols)


def ts_panel(pid, title, x, y, targets, custom=None, extra_defaults=None, w=12, h=8):
    c = {"lineWidth": 2, "showPoints": "never", "spanNulls": False}
    if custom:
        c.update(custom)
    d = {"custom": c}
    if extra_defaults:
        d.update(extra_defaults)
    return {
        "id": pid, "type": "timeseries", "title": title, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": d, "overrides": []},
        "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                    "tooltip": {"mode": "multi", "sort": "none"}},
        "targets": targets,
    }


def table_panel(pid, title, x, y, sql, w=12, h=8):
    return {
        "id": pid, "type": "table", "title": title, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h},
        "fieldConfig": {"defaults": {}, "overrides": []},
        "options": {"showHeader": True},
        "targets": [target(sql, "table")],
    }


state_sql = (
    "SELECT %s AS Kavsak, round(m.avg_queue, 1) AS Kuyruk, "
    "round(m.avg_wait, 1) AS Bekleme_s, round(m.avg_speed, 1) AS Hiz_ms, "
    "ifNull(c.program_id, '0') AS Plan "
    "FROM (SELECT tls_id, argMax(avg_queue, window_end_ms) AS avg_queue, "
    "argMax(avg_wait, window_end_ms) AS avg_wait, "
    "argMax(avg_speed, window_end_ms) AS avg_speed "
    "FROM traffic.metrics WHERE run_id='${run_id}' GROUP BY tls_id) AS m "
    "LEFT JOIN (SELECT tls_id, argMax(program_id, decided_at_ms) AS program_id "
    "FROM traffic.control_events WHERE run_id='${run_id}' GROUP BY tls_id) AS c "
    "ON m.tls_id = c.tls_id ORDER BY Kavsak"
) % name_expr("m.tls_id")

spark_lat = ("SELECT %s AS time, avg(spark_out_at_ms - last_produced_at_ms) AS spark_gecikme_ms "
             "FROM traffic.metrics WHERE run_id='${run_id}' GROUP BY time ORDER BY time") % TIME_M
e2e_lat = ("SELECT %s AS time, max(applied_at_ms - source_produced_at_ms) AS uctan_uca_gecikme_ms "
           "FROM traffic.control_events WHERE run_id='${run_id}' GROUP BY time ORDER BY time") % TIME_C

events_sql = (
    "SELECT toUInt32(applied_sim_time) AS Sim_zamani_s, %s AS Kavsak, "
    "program_id AS Plan, reason AS Neden, "
    "applied_at_ms - source_produced_at_ms AS Gecikme_ms "
    "FROM traffic.control_events WHERE run_id='${run_id}' "
    "ORDER BY applied_sim_time, tls_id"
) % name_expr("tls_id")

plan_mappings = [{"type": "value", "options": {
    "0": {"text": "0 varsayilan", "index": 0},
    "1": {"text": "1 ana yon", "index": 1},
    "2": {"text": "2 yan yon", "index": 2}}}]

sys_cols = ("mode AS Mod, scenario AS Senaryo, records_total AS Kayit, windows AS Pencere, "
            "wall_seconds AS Sure_s, records_per_sec AS Kayit_sn, completeness_pct AS Surerlilik_yuzde, "
            "max_gap_ms AS Maks_bosluk_ms, decisions AS Karar, e2e_avg_ms AS Gecikme_ort_ms, "
            "e2e_max_ms AS Gecikme_maks_ms")

panels = [
    table_panel(1, "Anlik durum (son pencere ve secili plan)", 0, 0, state_sql),
    ts_panel(2, "Kuyruk uzunlugu (duran arac, 10 sn pencere ortalamasi)", 12, 0,
             [target(pivot_metrics("avg_queue"), "timeseries")]),
    ts_panel(3, "Ortalama bekleme suresi (s)", 0, 8,
             [target(pivot_metrics("avg_wait"), "timeseries")],
             extra_defaults={"unit": "s"}),
    ts_panel(4, "Secili sinyal plani (kontrol degisiklikleri)", 12, 8,
             [target(pivot_plan(), "timeseries")],
             custom={"lineInterpolation": "stepAfter", "showPoints": "always", "spanNulls": True},
             extra_defaults={"min": 0, "max": 2, "mappings": plan_mappings, "decimals": 0}),
    ts_panel(5, "Gecikme (ms): Spark asamasi ve uctan uca", 0, 16,
             [target(spark_lat, "timeseries", "A"), target(e2e_lat, "timeseries", "B")],
             custom={"showPoints": "auto"}, extra_defaults={"unit": "ms"}),
    table_panel(6, "Kontrol degisiklikleri zaman cizelgesi", 12, 16, events_sql),
    table_panel(7, "Sistem metrikleri (secili kosu)", 0, 24,
                "SELECT %s FROM traffic.run_system_metrics WHERE run_id='${run_id}'" % sys_cols,
                w=24, h=4),
    table_panel(8, "Tum kosular (karsilastirma)", 0, 28,
                "SELECT run_id AS Kosu, %s FROM traffic.run_system_metrics ORDER BY run_id" % sys_cols,
                w=24, h=8),
]

dashboard = {
    "uid": "traffic-adaptive",
    "title": "Adaptif Trafik Sinyal Kontrolu",
    "tags": ["traffic", "sumo"],
    "timezone": "utc",
    "editable": True,
    "schemaVersion": 39,
    "version": 3,
    "refresh": "5s",
    "time": {"from": "1970-01-01T00:00:00.000Z", "to": "1970-01-01T01:00:00.000Z"},
    "templating": {"list": [{
        "name": "run_id",
        "label": "Kosu (run_id)",
        "type": "query",
        "datasource": DS,
        "query": "SELECT run_id FROM traffic.metrics GROUP BY run_id ORDER BY max(window_end_ms) DESC",
        "refresh": 1,
        "sort": 0,
        "multi": False,
        "includeAll": False,
        "current": {},
    }]},
    "annotations": {"list": []},
    "panels": panels,
}

OUT.parent.mkdir(parents=True, exist_ok=True)
with open(OUT, "w", encoding="utf-8") as f:
    json.dump(dashboard, f, ensure_ascii=False, indent=2)
print("Dashboard uretildi: %s (%d kavsak)" % (OUT, len(TLS)))
