"""Monte Carlo pi estimator. Reads config JSON from argv[1], writes result.json."""
import json
import pathlib
import random
import sys

cfg = json.loads(pathlib.Path(sys.argv[1]).read_text()) if len(sys.argv) > 1 else {}
n = cfg.get("n_samples", 100_000)
seed = cfg.get("seed", 0)
label = cfg.get("label", "run")

rng = random.Random(seed)
inside = sum(1 for _ in range(n) if rng.random() ** 2 + rng.random() ** 2 <= 1.0)
pi_est = 4 * inside / n

pathlib.Path("result.json").write_text(
    json.dumps({"label": label, "pi_estimate": pi_est, "n_samples": n, "seed": seed})
)
print(f"{label}: pi ≈ {pi_est:.6f}")
