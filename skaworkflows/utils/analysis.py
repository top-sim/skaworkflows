import numpy as np
import matplotlib.pyplot as plt
import pandas as pd

from matplotlib import rcParams

from skaworkflows.common import SKALow, LOW_TOTAL_SIZING

rcParams["text.usetex"] = False
rcParams["font.family"] = "serif"
rcParams["font.size"] = 9.0


def heatmap_computing_permutations():
    channels = 16384 / 2
    df = pd.read_csv(LOW_TOTAL_SIZING)
    df = df[["HPSO", "Baseline", "Stations", "Total Batch [Pflop/s]", "Channels"]]
    matrices = []
    fig, axes = plt.subplots(
        figsize=(10 / 3, 2.5), dpi=300, nrows=1, ncols=1
    )

    i = 0
    for hpso in df["HPSO"].unique():
        if hpso == "hpso04a" or hpso == "hpso05a":
            continue

        df_hpso = df[(df["HPSO"] == hpso) & (df["Channels"] == channels)]
        matrix = df_hpso.pivot_table(
            index="Baseline",
            columns="Stations",
            values="Total Batch [Pflop/s]",
            aggfunc="sum",
        )
        matrices.append(matrix.to_numpy())

    mean_matrix = np.mean(np.stack(matrices, axis=0), axis=0)
    im = axes.imshow(mean_matrix, cmap="viridis", origin="lower")

    axes.set_xticks(range(len(matrix.columns)), matrix.columns)
    axes.set_xticklabels(matrix.columns)
    axes.set_yticks(range(len(matrix.index)), matrix.index)
    axes.set_yticklabels(matrix.index)
    axes.set_title(f"Mean PFLOPs, {channels} channels")

    def draw_edges(cells: list):
        edges = set()
        for r, c in cells:
            corners = [
                (c - 0.5, r - 0.5),
                (c + 0.5, r - 0.5),
                (c + 0.5, r + 0.5),
                (c - 0.5, r + 0.5),
            ]
            square_edges = [
                (corners[0], corners[1]),
                (corners[1], corners[2]),
                (corners[2], corners[3]),
                (corners[3], corners[0]),
            ]
            for e in square_edges:
                e_norm = tuple(sorted(e))
                if e_norm in edges:
                    edges.remove(e_norm)
                else:
                    edges.add(e_norm)
        return edges

    cells = [(4, 1), (4, 2), (3, 3), (2, 4), (3, 2), (2, 3), (1, 4)]
    edges = draw_edges(cells)
    for (x1, y1), (x2, y2) in edges:
        axes.plot([x1, x2], [y1, y2], color="yellow", linewidth=2)

    cells = [(4, 4), (4, 3), (3, 4)]
    edges = draw_edges(cells)
    for (x1, y1), (x2, y2) in edges:
        axes.plot([x1, x2], [y1, y2], color="red", linewidth=2)

    axes.spines["right"].set_visible(False)
    axes.spines["top"].set_visible(False)

    axes.set_xlabel("# Stations")
    axes.set_ylabel("Baseline (km)")
    axes.tick_params(axis="x", labelrotation=45)
    plt.subplots_adjust(left=0.3, bottom=0.1, right=0.85, top=1)
    fig.colorbar(
        im, ax=axes, orientation="vertical", pad=0.1, label="PFLOPs", shrink=0.3
    )
    im.set_clim(vmin=0, vmax=15)

    plt.savefig("HeatmapPermutations_even_less.png", dpi=fig.dpi)


def plan_weighting_demonstration(seed=None, n_events=40, target_hours=168):
    rng = np.random.default_rng(seed)

    colors = {1: "#c6dbef", 4: "#fdd0a2", 9: "#de2d26"}

    def generate_events():
        sizes = rng.choice([1, 4, 9], size=n_events, p=[0.7, 0.25, 0.05])

        durations = []
        for s in sizes:
            if s == 9:
                durations.append(rng.integers(4, 7))
            else:
                durations.append(rng.integers(1, 6))

        durations = np.array(durations)

        durations = np.round(
            durations * target_hours / durations.sum()
        ).astype(int)

        durations = np.maximum(durations, 1)

        while durations.sum() < target_hours:
            durations[rng.integers(len(durations))] += 1

        while durations.sum() > target_hours:
            i = rng.choice(np.where(durations > 1)[0])
            durations[i] -= 1

        return [{"duration": d, "size": s} for d, s in zip(durations, sizes)]

    def build_clustered(events, rng):
        large = [e for e in events if e["size"] == 9]
        small = [e for e in events if e["size"] != 9]

        rng.shuffle(small)

        split = len(small) // 2

        cluster = small[:split]

        large = sorted(large, key=lambda _: rng.normal())

        return cluster + large + small[split:]

    def build_equispaced(events, rng):
        large = [e for e in events if e["size"] == 9]
        small = [e for e in events if e["size"] != 9]

        rng.shuffle(small)

        large = sorted(large, key=lambda _: rng.random())

        plan = []

        chunks = np.array_split(small, len(large) + 1)

        for i, chunk in enumerate(chunks):
            plan.extend(chunk.tolist())

            if i < len(large):
                plan.append(large[i])

        return plan

    def build_timeline(plan):
        t = 0
        tl = []
        for e in plan:
            tl.append(
                {
                    "start": t,
                    "end": t + e["duration"],
                    "weight": e["size"],
                }
            )
            t += e["duration"]
        return tl

    def max_window(tl, window=24):
        end_time = tl[-1]["end"]
        starts = np.arange(0, end_time - window + 1, 1)

        scores = []

        for s in starts:
            e = s + window

            total_w = 0
            total_t = 0

            for obs in tl:
                overlap = min(obs["end"], e) - max(obs["start"], s)

                if overlap > 0:
                    total_w += overlap * obs["weight"]
                    total_t += overlap

            scores.append(total_w / total_t if total_t > 0 else 0.0)

        scores = np.array(scores)

        return starts, scores, float(np.max(scores))

    def draw_bracket(ax, tl, window_start, window=24, y=9.5, label=None):
        window_end = window_start + window

        intersecting = [
            obs
            for obs in tl
            if obs["start"] < window_end and obs["end"] > window_start
        ]

        adjusted_end = max(obs["end"] for obs in intersecting)

        ax.plot(
            [window_start, window_start], [y - 0.6, y], color="black", linewidth=1
        )
        ax.plot(
            [adjusted_end, adjusted_end],
            [y - 0.6, y],
            color="black",
            linewidth=1,
        )
        ax.plot(
            [window_start, adjusted_end], [y, y], color="black", linewidth=1
        )

        if label:
            ax.text(
                (window_start + adjusted_end) / 2,
                y + 0.2,
                label,
                ha="center",
                fontsize=9,
            )

    def plot(ax, plan, tl):
        for obs in tl:
            duration = obs["end"] - obs["start"]

            ax.bar(
                obs["start"] + duration / 2,
                obs["weight"],
                width=duration,
                color=colors[obs["weight"]],
                edgecolor="white",
                linewidth=0.5,
            )

        ax.set_ylim(0, 11.5)
        ax.set_yticks([1, 4, 9])
        ax.set_yticklabels(["S", "M", "L"])
        ax.grid(axis="y", alpha=0.25)

        starts, scores, max_score = max_window(tl)
        worst_start = starts[np.argmax(scores)]

        draw_bracket(ax, tl, worst_start, label=f"max={max_score:.2f}")

        return max_score

    events = generate_events()

    clustered = build_clustered(events, rng)
    equispaced = build_equispaced(events, rng)

    c_tl = build_timeline(clustered)
    e_tl = build_timeline(equispaced)

    fig, axes = plt.subplots(
        2, 1, figsize=(10 / 3, 3), dpi=300, sharex=True
    )

    c_max = plot(axes[0], clustered, c_tl)
    e_max = plot(axes[1], equispaced, e_tl)

    axes[1].set_xlim(0, target_hours)
    axes[1].set_xlabel("Hour of Week")
    fig.supylabel("Observation size classes")
    plt.tight_layout()
    plt.savefig("plan-weight-demonstration.png", dpi=fig.dpi)

    return {
        "clustered_max": c_max,
        "equispaced_max": e_max,
    }
