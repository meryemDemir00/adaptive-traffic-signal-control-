import json
import os
import time
from collections import defaultdict

import yaml
from kafka import KafkaConsumer, KafkaProducer

BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
TOPIC_METRICS = os.environ["KAFKA_TOPIC_METRICS"]
TOPIC_CONTROL = os.environ["KAFKA_TOPIC_CONTROL"]
MIN_INTERVAL = float(os.environ.get("CONTROL_MIN_INTERVAL_SECONDS", "30"))
CONSECUTIVE = int(os.environ.get("CONTROL_CONSECUTIVE_WINDOWS", "3"))
THRESHOLDS_FILE = os.environ.get("THRESHOLDS_FILE", "/app/config/thresholds.yaml")

with open(THRESHOLDS_FILE, encoding="utf-8") as f:
    CFG = yaml.safe_load(f)

EXIT_RATIO = float(CFG.get("exit_ratio", 0.8))
DOMINANCE = float(CFG.get("dominance_ratio", 1.5))
PROGRAMS = CFG["programs"]
INTERSECTIONS = CFG["intersections"]


class State:
    def __init__(self):
        self.high = False
        self.high_windows = 0
        self.calm_windows = 0
        self.program = PROGRAMS["default"]
        self.last_change_sim = None


def update_level(state, q, th):
    if state.high:
        if q < th["high"] * EXIT_RATIO:
            state.high = False
    elif q > th["high"]:
        state.high = True
    if state.high:
        state.high_windows += 1
        state.calm_windows = 0
    else:
        state.high_windows = 0
        state.calm_windows += 1


def decide(state, m, th):
    update_level(state, m["avg_queue"], th)
    sim = m["sim_time_end"]
    if state.last_change_sim is not None and sim - state.last_change_sim < MIN_INTERVAL:
        return None
    target = None
    reason = None
    if state.high and state.high_windows >= CONSECUTIVE:
        main = m["avg_queue_main"]
        side = m["avg_queue_side"]
        if main > DOMINANCE * side:
            target, reason = PROGRAMS["main_priority"], "high_queue_main"
        elif side > DOMINANCE * main:
            target, reason = PROGRAMS["side_priority"], "high_queue_side"
    elif (not state.high and state.calm_windows >= CONSECUTIVE
          and state.program != PROGRAMS["default"]):
        target, reason = PROGRAMS["default"], "return_to_default"
    if target is None or target == state.program:
        return None
    state.program = target
    state.last_change_sim = sim
    return target, reason


def main():
    consumer = KafkaConsumer(
        TOPIC_METRICS,
        bootstrap_servers=BOOTSTRAP,
        group_id="controller",
        auto_offset_reset="latest",
        enable_auto_commit=True,
    )
    producer = KafkaProducer(bootstrap_servers=BOOTSTRAP)
    states = defaultdict(State)
    print("controller started, listening on %s" % TOPIC_METRICS, flush=True)

    for msg in consumer:
        m = json.loads(msg.value.decode("utf-8"))
        if m.get("mode") != "adaptive":
            continue
        th = INTERSECTIONS.get(m["tls_id"])
        if th is None:
            continue
        state = states[(m["run_id"], m["tls_id"])]
        result = decide(state, m, th)
        if result is None:
            continue
        program, reason = result
        out = {
            "run_id": m["run_id"],
            "tls_id": m["tls_id"],
            "program_id": program,
            "decided_at_ms": int(time.time() * 1000),
            "source_produced_at_ms": m["last_produced_at_ms"],
            "sim_time": m["sim_time_end"],
            "reason": reason,
        }
        key = ("%s:%s" % (m["run_id"], m["tls_id"])).encode("utf-8")
        producer.send(TOPIC_CONTROL, key=key, value=json.dumps(out).encode("utf-8"))
        producer.flush()
        print("decision: %s" % json.dumps(out), flush=True)


if __name__ == "__main__":
    main()