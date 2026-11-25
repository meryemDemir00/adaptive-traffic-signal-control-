"""Monoton artan kosu baslangici (event-time tabani).

Her kosu ve her sonda bir sonraki 1 saatlik dilimi kullanir, boylece event_time_ms
hicbir zaman geri gitmez ve Spark watermark'i yeni kosularin verisini 'gec veri'
diye atmaz. Spark'i yeniden baslatmaya gerek kalmaz.

Kullanim:  python experiments/run_clock.py   (bir sonraki degeri yazdirir ve sayaci ilerletir)
"""
from pathlib import Path

BASE_MS = 1_900_000_000_000      # sabit, sentetik taban (10 sn'ye bolunur: pencerelerle hizali)
STEP_MS = 3_600_000              # 2400 sn'lik kosudan buyuk, 10 sn'ye bolunur
COUNTER_FILE = Path(__file__).resolve().parent / ".run_counter"


def next_run_start_ms() -> int:
    try:
        n = int(COUNTER_FILE.read_text().strip())
    except (FileNotFoundError, ValueError):
        n = 0
    COUNTER_FILE.write_text(str(n + 1))
    return BASE_MS + n * STEP_MS


if __name__ == "__main__":
    print(next_run_start_ms())
