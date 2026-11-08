#!/usr/bin/env python3
"""plot.py - Genera las gráficas de rendimiento a partir del CSV de bench.

Uso:
    python3 plot.py [benchmark.csv] [-o carpeta_de_salida]

Requiere matplotlib (pip install matplotlib).

Por cada experimento crea una imagen con tres paneles (tiempo, speedup y
eficiencia) y además un resumen.png con el speedup de todos los experimentos.
También imprime una tabla con los números para pegarla en el informe.
"""

import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")  # no necesita pantalla
import matplotlib.pyplot as plt

# experimento -> (título, columna que varía, etiqueta del eje X)
EXPERIMENTS = {
    "agents": ("Cpu@1: trabajos independientes", "agents", "Agentes"),
    "sleep": ("Sleep@1: trabajos independientes", "agents", "Agentes"),
    "threads": ("Cpu@1: un solo trabajo", "threads", "Hilos por tarea"),
    "wordcount": ("WordCount@1", "threads", "Hilos por tarea"),
    "histogram": ("Histogram@1", "threads", "Hilos por tarea"),
}


def mean(values):
    return statistics.mean(values) if values else 0.0


def std(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def load(path):
    """Devuelve {experimento: {x: {"wall": [...], "server": [...], "step": [...]}}}."""
    data = defaultdict(lambda: defaultdict(lambda: {"wall": [], "server": [], "step": []}))
    skipped = 0

    with open(path, newline="") as f:
        for row in csv.DictReader(f):
            if row["state"] != "succeeded":
                skipped += 1
                continue

            exp = row["experiment"]
            _, param, _ = EXPERIMENTS.get(exp, (exp, "threads", "Hilos por tarea"))
            x = int(row[param])
            cell = data[exp][x]

            cell["wall"].append(float(row["wall_s"]))
            cell["server"].append(float(row["server_s"]))

            step = float(row["step_s"])
            if step >= 0:
                cell["step"].append(step)

    if skipped:
        print(f"aviso: se ignoraron {skipped} corridas que no terminaron en 'succeeded'",
              file=sys.stderr)

    return data


def analyze(points):
    """Calcula medias, desviaciones, speedup y eficiencia de un experimento."""
    xs = sorted(points)
    wall = [mean(points[x]["wall"]) for x in xs]
    wall_sd = [std(points[x]["wall"]) for x in xs]

    # Si hay tiempo del paso (solo la tarea) se usa ese; si no, el total
    has_step = all(points[x]["step"] and mean(points[x]["step"]) > 0 for x in xs)

    if has_step:
        step = [mean(points[x]["step"]) for x in xs]
        step_sd = [std(points[x]["step"]) for x in xs]
        metric = step
    else:
        step = step_sd = None
        metric = wall

    speedup = [metric[0] / m if m > 0 else 0.0 for m in metric]
    efficiency = [s / (x / xs[0]) for s, x in zip(speedup, xs)]

    return {
        "xs": xs, "wall": wall, "wall_sd": wall_sd,
        "step": step, "step_sd": step_sd,
        "speedup": speedup, "efficiency": efficiency,
        "metric_name": "tiempo de la tarea" if has_step else "tiempo total",
    }


def plot_experiment(exp, points, outdir):
    title, _, xlabel = EXPERIMENTS.get(exp, (exp, "threads", "Hilos por tarea"))
    r = analyze(points)
    xs = r["xs"]

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(15, 4.3))
    fig.suptitle(title, fontsize=13)

    # Panel 1: tiempo
    ax1.errorbar(xs, r["wall"], yerr=r["wall_sd"], marker="o", capsize=4,
                 label="Total (medido por el cliente)")
    if r["step"] is not None:
        ax1.errorbar(xs, r["step"], yerr=r["step_sd"], marker="s", capsize=4,
                     linestyle="--", label="Solo la tarea")
    ax1.set_xlabel(xlabel)
    ax1.set_ylabel("Tiempo (s)")
    ax1.set_title("Tiempo de ejecución")
    ax1.set_ylim(bottom=0)
    ax1.legend()

    # Panel 2: speedup contra el ideal
    ideal = [x / xs[0] for x in xs]
    ax2.plot(xs, r["speedup"], marker="o", label=f"Real ({r['metric_name']})")
    ax2.plot(xs, ideal, "k--", label="Ideal")
    ax2.set_xlabel(xlabel)
    ax2.set_ylabel("Speedup")
    ax2.set_title("Speedup")
    ax2.legend()

    # Panel 3: eficiencia
    ax3.plot(xs, [e * 100 for e in r["efficiency"]], marker="o", color="tab:green")
    ax3.axhline(100, color="k", linestyle="--", linewidth=1)
    ax3.set_xlabel(xlabel)
    ax3.set_ylabel("Eficiencia (%)")
    ax3.set_title("Eficiencia")
    ax3.set_ylim(bottom=0)

    for ax in (ax1, ax2, ax3):
        ax.set_xticks(xs)
        ax.grid(alpha=0.3)

    fig.tight_layout()
    path = os.path.join(outdir, f"{exp}.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return r, path


def print_table(exp, r):
    title = EXPERIMENTS.get(exp, (exp,))[0]
    xlabel = EXPERIMENTS.get(exp, (exp, "", "Hilos"))[2]

    print(f"\n{exp}  ({title})")
    print(f"{xlabel:>16} {'total (s)':>12} {'desv.':>8} {'tarea (s)':>11} "
          f"{'speedup':>9} {'efic. %':>9}")

    for i, x in enumerate(r["xs"]):
        step = f"{r['step'][i]:11.3f}" if r["step"] is not None else f"{'-':>11}"
        print(f"{x:>16} {r['wall'][i]:12.3f} {r['wall_sd'][i]:8.3f} {step} "
              f"{r['speedup'][i]:9.2f} {r['efficiency'][i] * 100:9.1f}")


def plot_summary(results, outdir):
    fig, ax = plt.subplots(figsize=(7.5, 5.2))
    max_x = 1

    for exp, r in results.items():
        title, _, xlabel = EXPERIMENTS.get(exp, (exp, "", "Hilos"))
        ax.plot(r["xs"], r["speedup"], marker="o", label=f"{title} ({xlabel.lower()})")
        max_x = max(max_x, max(r["xs"]))

    ax.plot([1, max_x], [1, max_x], "k--", label="Ideal")
    ax.set_xlabel("Agentes / hilos por tarea")
    ax.set_ylabel("Speedup")
    ax.set_title("Speedup de todos los experimentos")
    ax.grid(alpha=0.3)
    ax.legend(fontsize=8)

    fig.tight_layout()
    path = os.path.join(outdir, "resumen.png")
    fig.savefig(path, dpi=150)
    plt.close(fig)

    return path


def main():
    parser = argparse.ArgumentParser(description="Gráficas del benchmark del servidor de pipelines")
    parser.add_argument("csv", nargs="?", default="benchmark.csv", help="CSV generado por bench")
    parser.add_argument("-o", "--outdir", default="graficas", help="carpeta de salida")
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        sys.exit(f"no existe {args.csv}; corre primero ./bench")

    data = load(args.csv)

    if not data:
        sys.exit("el CSV no tiene corridas exitosas para graficar")

    os.makedirs(args.outdir, exist_ok=True)

    results = {}
    order = [e for e in EXPERIMENTS if e in data] + [e for e in data if e not in EXPERIMENTS]

    for exp in order:
        r, path = plot_experiment(exp, data[exp], args.outdir)
        results[exp] = r
        print_table(exp, r)
        print(f"-> {path}")

    print(f"-> {plot_summary(results, args.outdir)}")


if __name__ == "__main__":
    main()
