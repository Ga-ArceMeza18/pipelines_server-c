
#!/usr/bin/env python3
"""plot.py - Analisis de rendimiento del servidor de pipelines.

Lee el CSV generado por bench.c y calcula:
- Mediana de los tiempos de ejecucion.
- Desviacion estandar.
- Speedup y eficiencia.
- Fraccion paralelizable estimada mediante la ley de Amdahl.
- Speedup predicho por Amdahl.
- Graficas individuales y resumen general.

Uso:
    python3 tools/plot.py benchmark.csv
"""

import argparse
import csv
import os
import statistics
import sys
from collections import defaultdict

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt


EXPERIMENTS = {
    "wide": ("Pipeline ancha: trabajos independientes", "agents", "Agentes"),
    "deep": ("Pipeline profunda: cadena de dependencias", "agents", "Agentes"),
    "combined": ("Pipelines simultaneas", "agents", "Agentes"),
    "threads": ("Cpu@1: hilos por tarea", "threads", "Hilos"),
    "wordcount": ("WordCount@1: paralelismo OpenMP", "threads", "Hilos"),
    "histogram": ("Histogram@1: paralelismo OpenMP", "threads", "Hilos"),
}


def median(values):
    return statistics.median(values) if values else 0.0


def std(values):
    return statistics.stdev(values) if len(values) > 1 else 0.0


def load(path):
    """Agrupa los resultados exitosos por entorno, experimento y parametro."""

    data = defaultdict(
        lambda: defaultdict(
            lambda: defaultdict(
                lambda: {"wall": [], "server": [], "step": []}
            )
        )
    )

    skipped = 0

    with open(path, newline="", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        required = {
            "environment", "experiment", "agents", "threads",
            "state", "wall_s", "server_s", "step_s"
        }

        if not required.issubset(set(reader.fieldnames or [])):
            sys.exit("Error: el CSV no tiene las columnas esperadas de bench.c")

        for row in reader:
            if row["state"] != "succeeded":
                skipped += 1
                continue

            environment = row["environment"]
            experiment = row["experiment"]

            if experiment not in EXPERIMENTS:
                continue

            _, parameter, _ = EXPERIMENTS[experiment]
            x = int(row[parameter])

            cell = data[environment][experiment][x]

            wall = float(row["wall_s"])
            server = float(row["server_s"])
            step = float(row["step_s"])

            if wall > 0:
                cell["wall"].append(wall)

            if server > 0:
                cell["server"].append(server)

            if step > 0:
                cell["step"].append(step)

    if skipped:
        print(
            f"Aviso: se ignoraron {skipped} ejecuciones no exitosas.",
            file=sys.stderr
        )

    return data


def estimate_fraction(speedup, p):
    """Estima f despejando la ley de Amdahl."""

    if p <= 1 or speedup <= 0:
        return None

    denominator = 1.0 - (1.0 / p)

    if denominator <= 0:
        return None

    fraction = (1.0 - (1.0 / speedup)) / denominator

    return fraction


def amdahl_speedup(fraction, p):
    """Calcula el speedup predicho por la ley de Amdahl."""

    return 1.0 / ((1.0 - fraction) + fraction / p)


def analyze(points, experiment):
    """Calcula las metricas usando medianas."""

    xs = sorted(points)

    wall = [median(points[x]["wall"]) for x in xs]
    wall_sd = [std(points[x]["wall"]) for x in xs]

    # Para WordCount e Histogram se analiza el tiempo del paso de procesamiento.
    has_step = experiment in ("wordcount", "histogram") and all(
        points[x]["step"] for x in xs
    )

    if has_step:
        metric = [median(points[x]["step"]) for x in xs]
        metric_sd = [std(points[x]["step"]) for x in xs]
        metric_name = "Tiempo de la tarea"
    else:
        metric = wall
        metric_sd = wall_sd
        metric_name = "Tiempo total"

    # El baseline debe ser p=1.
    if 1 not in xs:
        print(
            f"Aviso: {experiment} no tiene configuracion p=1; "
            "no se calculara speedup ni eficiencia.",
            file=sys.stderr
        )

        speedup = [0.0] * len(xs)
        efficiency = [0.0] * len(xs)
        fraction = None
        prediction = [0.0] * len(xs)

    else:
        baseline = metric[xs.index(1)]

        speedup = [
            baseline / value if value > 0 else 0.0
            for value in metric
        ]

        efficiency = [
            s / p if p > 0 else 0.0
            for s, p in zip(speedup, xs)
        ]

        # Se estima f para cada punto p > 1.
        fractions = []

        for p, s in zip(xs, speedup):
            f = estimate_fraction(s, p)

            if f is not None:
                fractions.append(f)

        # La mediana reduce la influencia de una medicion anomala.
        fraction = median(fractions) if fractions else None

        # La fraccion estimada se limita al intervalo fisico [0, 1].
        if fraction is not None:
            fraction = max(0.0, min(1.0, fraction))

        prediction = [
            amdahl_speedup(fraction, p) if fraction is not None else 0.0
            for p in xs
        ]

    return {
        "xs": xs,
        "wall": wall,
        "wall_sd": wall_sd,
        "metric": metric,
        "metric_sd": metric_sd,
        "metric_name": metric_name,
        "speedup": speedup,
        "efficiency": efficiency,
        "fraction": fraction,
        "prediction": prediction,
    }


def plot_experiment(experiment, points, outdir, environment):
    title, _, xlabel = EXPERIMENTS[experiment]

    result = analyze(points, experiment)
    xs = result["xs"]

    fig, (ax1, ax2, ax3) = plt.subplots(1, 3, figsize=(16, 4.8))

    fig.suptitle(
        f"{title}\nEntorno: {environment}",
        fontsize=12
    )

    # Tiempo de ejecucion.
    ax1.errorbar(
        xs,
        result["wall"],
        yerr=result["wall_sd"],
        marker="o",
        capsize=4,
        label="Tiempo total"
    )

    if result["metric_name"] == "Tiempo de la tarea":
        ax1.errorbar(
            xs,
            result["metric"],
            yerr=result["metric_sd"],
            marker="s",
            linestyle="--",
            capsize=4,
            label="Tiempo de procesamiento"
        )

    ax1.set_xlabel(xlabel)
    ax1.set_ylabel("Tiempo (s)")
    ax1.set_title("Tiempo de ejecucion")
    ax1.set_ylim(bottom=0)
    ax1.set_xticks(xs)
    ax1.grid(alpha=0.3)
    ax1.legend()

    # Speedup real y prediccion de Amdahl.
    ax2.plot(
        xs,
        result["speedup"],
        marker="o",
        label="Speedup medido"
    )

    ax2.plot(
        xs,
        result["prediction"],
        marker="s",
        linestyle="--",
        label="Prediccion de Amdahl"
    )

    ax2.plot(
        xs,
        xs,
        color="black",
        linestyle=":",
        label="Speedup ideal"
    )

    ax2.set_xlabel(xlabel)
    ax2.set_ylabel("Speedup")
    ax2.set_title("Speedup")
    ax2.set_xticks(xs)
    ax2.set_ylim(bottom=0)
    ax2.grid(alpha=0.3)
    ax2.legend(fontsize=8)

    # Eficiencia.
    ax3.plot(
        xs,
        [value * 100 for value in result["efficiency"]],
        marker="o",
        label="Eficiencia"
    )

    ax3.axhline(
        100,
        color="black",
        linestyle="--",
        linewidth=1
    )

    ax3.set_xlabel(xlabel)
    ax3.set_ylabel("Eficiencia (%)")
    ax3.set_title("Eficiencia")
    ax3.set_xticks(xs)
    ax3.set_ylim(bottom=0)
    ax3.grid(alpha=0.3)

    fig.tight_layout()

    filename = f"{environment}_{experiment}.png"
    path = os.path.join(outdir, filename)

    fig.savefig(path, dpi=150)
    plt.close(fig)

    return result, path


def print_table(experiment, result, environment):
    title, _, xlabel = EXPERIMENTS[experiment]

    print(f"\n{'=' * 100}")
    print(f"Entorno: {environment}")
    print(f"Experimento: {title}")
    print(f"{'=' * 100}")

    print(
        f"{xlabel:>10} "
        f"{'Mediana(s)':>12} "
        f"{'Desv.(s)':>11} "
        f"{'Speedup':>10} "
        f"{'Efic.(%)':>11} "
        f"{'Amdahl':>10}"
    )

    for i, p in enumerate(result["xs"]):
        print(
            f"{p:>10} "
            f"{result['metric'][i]:12.4f} "
            f"{result['metric_sd'][i]:11.4f} "
            f"{result['speedup'][i]:10.3f} "
            f"{result['efficiency'][i] * 100:11.2f} "
            f"{result['prediction'][i]:10.3f}"
        )

    if result["fraction"] is not None:
        print(
            "\nFraccion paralelizable estimada: "
            f"f = {result['fraction']:.4f} "
            f"({result['fraction'] * 100:.2f}%)"
        )

        print(
            "Fraccion serial estimada: "
            f"{1 - result['fraction']:.4f} "
            f"({(1 - result['fraction']) * 100:.2f}%)"
        )
    else:
        print("\nNo fue posible estimar la fraccion paralelizable.")

    print(f"Metrica utilizada: {result['metric_name']}")


def plot_summary(results, outdir):
    """Grafica el speedup medido y Amdahl de todos los experimentos."""

    for environment, experiments in results.items():
        fig, ax = plt.subplots(figsize=(9, 5.5))

        max_x = 1

        for experiment, result in experiments.items():
            title = EXPERIMENTS[experiment][0]

            ax.plot(
                result["xs"],
                result["speedup"],
                marker="o",
                label=title
            )

            max_x = max(max_x, max(result["xs"]))

        ax.plot(
            [1, max_x],
            [1, max_x],
            color="black",
            linestyle="--",
            label="Ideal"
        )

        ax.set_xlabel("Agentes / hilos")
        ax.set_ylabel("Speedup")
        ax.set_title(f"Resumen de speedup - {environment}")
        ax.set_ylim(bottom=0)
        ax.grid(alpha=0.3)
        ax.legend(fontsize=8)

        fig.tight_layout()

        path = os.path.join(outdir, f"{environment}_resumen.png")
        fig.savefig(path, dpi=150)
        plt.close(fig)

        print(f"-> {path}")


def main():
    parser = argparse.ArgumentParser(
        description="Analisis de rendimiento del servidor de pipelines"
    )

    parser.add_argument(
        "csv",
        nargs="?",
        default="benchmark.csv",
        help="CSV generado por bench.c"
    )

    parser.add_argument(
        "-o",
        "--outdir",
        default="graficas",
        help="Carpeta donde guardar las graficas"
    )

    args = parser.parse_args()

    if not os.path.exists(args.csv):
        sys.exit(f"Error: no existe {args.csv}")

    data = load(args.csv)

    if not data:
        sys.exit("Error: no hay ejecuciones exitosas para analizar.")

    os.makedirs(args.outdir, exist_ok=True)

    all_results = {}

    for environment, experiments in data.items():
        all_results[environment] = {}

        for experiment in EXPERIMENTS:
            if experiment not in experiments:
                continue

            result, path = plot_experiment(
                experiment,
                experiments[experiment],
                args.outdir,
                environment
            )

            all_results[environment][experiment] = result

            print_table(experiment, result, environment)
            print(f"Grafica: {path}")

    plot_summary(all_results, args.outdir)

    print("\nAnalisis terminado.")


if __name__ == "__main__":
    main()
