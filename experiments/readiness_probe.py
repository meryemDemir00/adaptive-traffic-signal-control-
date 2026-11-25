"""Uctan uca hazir olma sondasi.

Sahte bir 'probe_*' kosusu gonderir ve sunu bekler:
  1) Spark -> ClickHouse: bu run_id icin traffic.metrics satiri
  2) Controller: traffic.control'de bu run_id icin bir karar
Ikisi de gorulurse Kafka -> Spark -> ClickHouse ve Controller hatti canlidir.
Sonda ayrica onceki kosunun son pencerelerini kapatir (daha gec event-time gonderdigi icin).

Kullanim (repo kokunden):  python experiments/readiness_probe.py [--attempts 12] [--wait 20]
Cikis kodu: 0 = hazir, 1 = hazir degil.
"""
import argparse
import json
import os
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer, TopicPartition

sys.path.insert(0, str(Path(__file__).resolve().parent))
from run_clock import next_run_start_ms  # noqa: E402

load_dotenv(override=False)

# Host'tan baglanirken .env'deki kafka:9092 gecerli degildir
HOST_BOOTSTRAP = os.getenv("HOST_KAFKA_BOOTSTRAP", "localhost:29092")
TOPIC_RAW = os.getenv("KAFKA_TOPIC_RAW", "traffic.raw")
TOPIC_CONTROL = os.getenv("KAFKA_TOPIC_CONTROL", "traffic.control")
CH_URL = f"http://localhost:{os.getenv('CLICKHOUSE_PORT', '8123')}"
CH_DB = os.getenv("CLICKHOUSE_DB", "traffic")
CH_USER = os.getenv("CLICKHOUSE_USER", "")
CH_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")

WINDOW_S = int(os.getenv("SPARK_WINDOW_SECONDS", "10"))
WATERMARK_S = int(os.getenv("SPARK_WATERMARK_SECONDS", "2"))
CONSECUTIVE = int(os.getenv("CONTROL_CONSECUTIVE_WINDOWS", "3"))
# CONSECUTIVE pencerenin kapanmasi icin gereken en gec sim_time
T_MAX = (CONSECUTIVE + 1) * WINDOW_S + WATERMARK_S + 2


def ch_count(run_id: str) -> int:
    sql = f"SELECT count() FROM {CH_DB}.metrics WHERE run_id = '{run_id}'"
    url = f"{CH_URL}/?query={urllib.parse.quote(sql)}"
    if CH_USER:
        url += f"&user={urllib.parse.quote(CH_USER)}"
    if CH_PASSWORD:
        url += f"&password={urllib.parse.quote(CH_PASSWORD)}"
    try:
        with urllib.request.urlopen(url, timeout=3) as resp:
            return int(resp.read().decode().strip() or 0)
    except Exception:
        return 0


def load_probe_target():
    with open("config/thresholds.yaml", encoding="utf-8") as f:
        cfg = yaml.safe_load(f)
    tls_id, th = next(iter(cfg["intersections"].items()))
    return tls_id, float(th["high"])


def try_once(producer, consumer, tls_id, high, idx, wait_s):
    probe_id = f"probe_{int(time.time())}_{idx}"
    base = next_run_start_ms()
    q = int(high * 3) + 5  # esigin cok ustunde: yuksek yogunluk, tamami ana yonde
    for t in range(0, T_MAX + 1, 2):
        msg = {
            "run_id": probe_id, "scenario": "probe", "mode": "adaptive", "seed": 0,
            "sim_time": float(t), "event_time_ms": base + t * 1000,
            "produced_at_ms": int(time.time() * 1000), "tls_id": tls_id,
            "total_halting": q, "queue_main": q, "queue_side": 0,
            "avg_waiting_time": 10.0, "avg_speed": 1.0, "avg_occupancy": 50.0,
            "vehicle_count": q,
        }
        producer.send(TOPIC_RAW, key=f"{probe_id}:{tls_id}", value=msg)
    producer.flush()

    got_metrics = got_control = False
    deadline = time.time() + wait_s
    while time.time() < deadline and not (got_metrics and got_control):
        if not got_control:
            for _tp, msgs in consumer.poll(timeout_ms=1000).items():
                for m in msgs:
                    if isinstance(m.value, dict) and m.value.get("run_id") == probe_id:
                        got_control = True
        if not got_metrics:
            got_metrics = ch_count(probe_id) > 0
        time.sleep(0.5)
    return probe_id, got_metrics, got_control


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--attempts", type=int, default=12)
    ap.add_argument("--wait", type=int, default=20, help="her deneme icin bekleme (sn)")
    args = ap.parse_args()

    tls_id, high = load_probe_target()
    print(f"[PROBE] hedef kavsak={tls_id}, kafka={HOST_BOOTSTRAP}")

    producer = consumer = None
    for idx in range(1, args.attempts + 1):
        try:
            if producer is None:
                producer = KafkaProducer(
                    bootstrap_servers=HOST_BOOTSTRAP.split(","),
                    key_serializer=lambda k: k.encode("utf-8"),
                    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                )
            if consumer is None:
                consumer = KafkaConsumer(
                    bootstrap_servers=HOST_BOOTSTRAP.split(","),
                    enable_auto_commit=False,
                    value_deserializer=lambda b: json.loads(b.decode("utf-8")),
                )
                tp = TopicPartition(TOPIC_CONTROL, 0)
                consumer.assign([tp])
                consumer.seek_to_end(tp)
                consumer.position(tp)
        except Exception as exc:
            print(f"[PROBE] deneme {idx}: Kafka'ya baglanilamadi ({exc})")
            time.sleep(5)
            continue

        probe_id, ok_m, ok_c = try_once(producer, consumer, tls_id, high, idx, args.wait)
        print(f"[PROBE] deneme {idx}: metrics={'OK' if ok_m else 'YOK'}, controller={'OK' if ok_c else 'YOK'} ({probe_id})")
        if ok_m and ok_c:
            print("[PROBE] hat hazir.")
            return 0

    print("[PROBE] HAZIR DEGIL: docker compose logs spark controller ile bakin.", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
