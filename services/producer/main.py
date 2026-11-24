import xml.etree.ElementTree as ET
import argparse
import json
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from pathlib import Path

import yaml
from dotenv import load_dotenv

# .env dosyasini yukle (ortam degiskenleri zaten tanimliysa ezmez)
load_dotenv(override=False)

# SUMO_HOME/tools yolunu Python path'e ekle
if "SUMO_HOME" in os.environ:
    tools = os.path.join(os.environ["SUMO_HOME"], "tools")
    if tools not in sys.path:
        sys.path.append(tools)
else:
    sys.exit("HATA: SUMO_HOME ortam degiskeni tanimli degil.")

import traci  # noqa: E402


def now_ms() -> int:
    return int(time.time() * 1000)


def is_running_in_docker() -> bool:
    flag = os.getenv("IN_DOCKER", "").strip().lower()
    if flag in {"1", "true", "yes"}:
        return True
    if flag in {"0", "false", "no"}:
        return False
    return os.path.exists("/.dockerenv")


def resolve_bootstrap_servers(raw_value: str) -> str:
    val = (raw_value or "").strip()
    # KAFKA_BOOTSTRAP_SERVERS acikca verildiyse tahmine gerek kalmadan dogrudan kullan
    if val:
        return val
    return "kafka:9092" if is_running_in_docker() else "localhost:29092"


def resolve_clickhouse_url() -> str:
    host = os.getenv("CLICKHOUSE_HOST", "clickhouse" if is_running_in_docker() else "localhost").strip()
    if not is_running_in_docker() and host == "clickhouse":
        host = "localhost"
    port = os.getenv("CLICKHOUSE_PORT", "8123").strip()
    return f"http://{host}:{port}"


def insert_control_event_clickhouse(
    ch_url: str,
    ch_db: str,
    ch_user: str,
    ch_password: str,
    row: dict,
) -> None:
    query = urllib.parse.quote(f"INSERT INTO {ch_db}.control_events FORMAT JSONEachRow")
    url = f"{ch_url}/?query={query}"
    if ch_user:
        url += f"&user={urllib.parse.quote(ch_user)}"
    if ch_password:
        url += f"&password={urllib.parse.quote(ch_password)}"

    body = (json.dumps(row, ensure_ascii=False) + "\n").encode("utf-8")
    req = urllib.request.Request(url, data=body, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=3) as resp:
            resp.read()
    except Exception as exc:
        print(f"[UYARI] ClickHouse control_events yazma hatasi: {exc}", file=sys.stderr)


def load_approaches(config_path: str) -> dict:
    with open(config_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f)
    return data["intersections"]


def collect_intersection_metrics(
    tls_id: str,
    lanes_cfg: dict,
    run_id: str,
    scenario: str,
    mode: str,
    seed: int,
    sim_time: float,
    run_start_ms: int,
) -> dict:
    main_lanes = lanes_cfg.get("main", [])
    side_lanes = lanes_cfg.get("side", [])
    all_lanes = main_lanes + side_lanes

    queue_main = sum(traci.lane.getLastStepHaltingNumber(l) for l in main_lanes)
    queue_side = sum(traci.lane.getLastStepHaltingNumber(l) for l in side_lanes)
    total_halting = queue_main + queue_side

    vehicle_count = sum(traci.lane.getLastStepVehicleNumber(l) for l in all_lanes)

    if all_lanes:
        avg_speed = sum(traci.lane.getLastStepMeanSpeed(l) for l in all_lanes) / len(all_lanes)
        avg_occupancy = sum(traci.lane.getLastStepOccupancy(l) for l in all_lanes) / len(all_lanes)
        total_wait = sum(traci.lane.getWaitingTime(l) for l in all_lanes)
        avg_waiting_time = (total_wait / vehicle_count) if vehicle_count > 0 else 0.0
    else:
        avg_speed = 0.0
        avg_occupancy = 0.0
        avg_waiting_time = 0.0

    event_time_ms = int(run_start_ms + sim_time * 1000)

    return {
        "run_id": run_id,
        "scenario": scenario,
        "mode": mode,
        "seed": seed,
        "sim_time": round(sim_time, 2),
        "event_time_ms": event_time_ms,
        "produced_at_ms": now_ms(),
        "tls_id": tls_id,
        "total_halting": int(total_halting),
        "queue_main": int(queue_main),
        "queue_side": int(queue_side),
        "avg_waiting_time": round(avg_waiting_time, 2),
        "avg_speed": round(avg_speed, 2),
        "avg_occupancy": round(avg_occupancy, 2),
        "vehicle_count": int(vehicle_count),
    }


def start_control_listener(
    bootstrap_servers: str,
    control_topic: str,
    run_id: str,
    pending_commands: dict,
    lock: threading.Lock,
    stop_event: threading.Event,
):
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        control_topic,
        bootstrap_servers=bootstrap_servers.split(","),
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"producer-control-{run_id}",
        consumer_timeout_ms=1000,
    )

    try:
        while not stop_event.is_set():
            records = consumer.poll(timeout_ms=250)
            for _tp, messages in records.items():
                for msg in messages:
                    payload = msg.value
                    if payload.get("run_id") != run_id:
                        continue
                    tls_id = payload.get("tls_id")
                    program_id = str(payload.get("program_id"))
                    if tls_id and program_id in {"0", "1", "2"}:
                        with lock:
                            pending_commands[tls_id] = payload
    finally:
        try:
            consumer.close()
        except Exception:
            pass


def main():
    default_bootstrap = resolve_bootstrap_servers(os.getenv("KAFKA_BOOTSTRAP_SERVERS", ""))
    default_ch_url = resolve_clickhouse_url()

    parser = argparse.ArgumentParser(description="SUMO TraCI -> Kafka Producer")
    parser.add_argument("--run-id", default=f"run_{int(time.time())}")
    parser.add_argument("--scenario", default="normal", choices=["normal", "demand_surge", "lane_closure"])
    parser.add_argument("--mode", default="fixed", choices=["fixed", "adaptive"])
    parser.add_argument("--seed", type=int, default=int(os.getenv("SIM_SEED", "42")))
    parser.add_argument("--gui", action="store_true", help="SUMO GUI ile calistir")
    parser.add_argument("--dry-run", action="store_true", help="Kafka yerine konsola JSON bas")
    parser.add_argument("--bootstrap-servers", default=default_bootstrap)
    parser.add_argument("--raw-topic", default=os.getenv("KAFKA_TOPIC_RAW", "traffic.raw"))
    parser.add_argument("--control-topic", default=os.getenv("KAFKA_TOPIC_CONTROL", "traffic.control"))
    parser.add_argument("--clickhouse-url", default=default_ch_url)
    parser.add_argument("--approaches", default="config/approaches.yaml")
    parser.add_argument("--output-dir", default="experiments/results", help="SUMO tripinfo/summary cikti klasoru")
    parser.add_argument("--step-delay", type=float, default=0.0, help="Her sim adimi sonrasi bekleme (sn)")
    args = parser.parse_args()

    ch_db = os.getenv("CLICKHOUSE_DB", "traffic").strip()
    ch_user = os.getenv("CLICKHOUSE_USER", "").strip()
    ch_password = os.getenv("CLICKHOUSE_PASSWORD", "").strip()

    intersections = load_approaches(args.approaches)
    sumocfg_path = Path("sumo/scenarios") / f"{args.scenario}.sumocfg"
    if not sumocfg_path.exists():
        sys.exit(f"HATA: Senaryo dosyasi bulunamadi: {sumocfg_path}")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    tripinfo_path = output_dir / f"{args.run_id}_tripinfo.xml"
    summary_path = output_dir / f"{args.run_id}_summary.xml"

    kafka_producer = None
    if not args.dry_run:
        from kafka import KafkaProducer
        kafka_producer = KafkaProducer(
            bootstrap_servers=args.bootstrap_servers.split(","),
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
        )

    pending_commands: dict = {}
    cmd_lock = threading.Lock()
    stop_event = threading.Event()
    listener_thread = None

    if args.mode == "adaptive" and not args.dry_run:
        listener_thread = threading.Thread(
            target=start_control_listener,
            args=(args.bootstrap_servers, args.control_topic, args.run_id, pending_commands, cmd_lock, stop_event),
            daemon=True,
        )
        listener_thread.start()

    sumo_binary = "sumo-gui" if args.gui else "sumo"
    sumo_cmd = [
        sumo_binary,
        "-c", str(sumocfg_path),
        "--seed", str(args.seed),
        "--no-step-log",
        "--tripinfo-output", str(tripinfo_path),
        "--summary-output", str(summary_path),
    ]

    cfg_tree = ET.parse(sumocfg_path)
    end_elem = cfg_tree.getroot().find("./time/end")
    sim_end_time = float(end_elem.attrib["value"]) if end_elem is not None else 2400.0

    traci.start(sumo_cmd)
    run_start_ms = now_ms()
    last_sample_sim_time = -2.0
    sent_count = 0

    try:
        while traci.simulation.getTime() < sim_end_time:
            traci.simulationStep()
            sim_time = traci.simulation.getTime()

            # Adaptive modda bekleyen kontrol komutlarini uygula ve ClickHouse control_events'e yaz
            if args.mode == "adaptive":
                with cmd_lock:
                    to_apply = list(pending_commands.items())
                    pending_commands.clear()

                for tls_id, cmd in to_apply:
                    prog_id = str(cmd["program_id"])
                    traci.trafficlight.setProgram(tls_id, prog_id)
                    applied_at_ms = now_ms()
                    applied_sim_time = round(sim_time, 2)

                    event_row = {
                        "run_id": cmd.get("run_id", args.run_id),
                        "tls_id": tls_id,
                        "program_id": prog_id,
                        "decided_at_ms": int(cmd.get("decided_at_ms", applied_at_ms)),
                        "source_produced_at_ms": int(cmd.get("source_produced_at_ms", applied_at_ms)),
                        "sim_time": float(cmd.get("sim_time", applied_sim_time)),
                        "reason": str(cmd.get("reason", "adaptive_control")),
                        "applied_at_ms": applied_at_ms,
                        "applied_sim_time": applied_sim_time,
                    }
                    if not args.dry_run:
                        insert_control_event_clickhouse(
                            ch_url=args.clickhouse_url,
                            ch_db=ch_db,
                            ch_user=ch_user,
                            ch_password=ch_password,
                            row=event_row,
                        )
                    print(
                        f"[CONTROL APPLIED] run={args.run_id} tls={tls_id} "
                        f"program={prog_id} sim_time={applied_sim_time} "
                        f"latency_ms={applied_at_ms - event_row['source_produced_at_ms']}"
                    )

            # Her 2 simulasyon saniyesinde bir olcum uret
            if sim_time - last_sample_sim_time >= 2.0 - 1e-6:
                last_sample_sim_time = sim_time
                for tls_id, lanes_cfg in intersections.items():
                    msg = collect_intersection_metrics(
                        tls_id=tls_id,
                        lanes_cfg=lanes_cfg,
                        run_id=args.run_id,
                        scenario=args.scenario,
                        mode=args.mode,
                        seed=args.seed,
                        sim_time=sim_time,
                        run_start_ms=run_start_ms,
                    )
                    key = f"{args.run_id}:{tls_id}"
                    if args.dry_run:
                        if sent_count < 6:
                            print(f"[{key}] {json.dumps(msg)}")
                    else:
                        kafka_producer.send(args.raw_topic, key=key, value=msg)
                    sent_count += 1

            if args.step_delay > 0:
                time.sleep(args.step_delay)

        if kafka_producer:
            kafka_producer.flush()
        print(
            f"Tamamlandi! Toplam {sent_count} mesaj uretildi "
            f"(run_id={args.run_id}, bootstrap={args.bootstrap_servers}, tripinfo={tripinfo_path})."
        )
    finally:
        stop_event.set()
        if listener_thread is not None:
            listener_thread.join(timeout=3.0)
        traci.close()
        if kafka_producer:
            kafka_producer.close()


if __name__ == "__main__":
    main()