"""
Utility methods to evaluate SKAWorkflows data
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
import pandas as pd

from scipy.spatial import ConvexHull
from matplotlib.colors import LogNorm

from skaworkflows.common import SKALow, LOW_TOTAL_SIZING

from matplotlib import rcParams
rcParams["text.usetex"] = False
rcParams["font.family"] = "serif"
rcParams["font.size"] = 9.0


def heatmap_computing_permutations():
    """
    Produce a heatmap of the different combinations of stations and
    baselines for SKA Low HPSOs
    """
    channels = 16384*2 #/2
    df = pd.read_csv(LOW_TOTAL_SIZING)
    df = df[['HPSO', 'Baseline', 'Stations', 'Total Batch [Pflop/s]', 'Channels']]
    matrices = []
    fig, axes = plt.subplots(figsize=(10/3, 2.5), dpi=300,nrows=1, ncols=1)

    i = 0
    for hpso in df['HPSO'].unique():
        if hpso == 'hpso04a' or hpso=='hpso05a':
            continue

        df_hpso = df[(df['HPSO'] == hpso) & (df['Channels'] == channels)]
        matrix=df_hpso.pivot_table(index='Baseline', columns='Stations', values='Total Batch [Pflop/s]',
                            aggfunc='sum')
        matrices.append(matrix.to_numpy())

        # ratio_df = pd.read_csv("/home/rwb/github/experiments/simulation_scheduling_experiments/hpso_permutation_ratios-4_64-0.5_128-0.3_256-0.2_alpha-0.20_pivot.csv",
        #                         index_col=False)
        # ratio_hpso_df = ratio_df[ratio_df['hpso']==hpso].drop(['hpso'], axis=1).replace(np.nan, 0)
        # ratio_matrix = ratio_hpso_df.to_numpy()

    mean_matrix = np.mean(np.stack(matrices, axis=0), axis=0)
    im = axes.imshow(mean_matrix, cmap='viridis', origin='lower')#, norm=LogNorm())

    axes.set_xticks(range(len(matrix.columns)), matrix.columns)
    axes.set_xticklabels(matrix.columns)
    axes.set_yticks(range(len(matrix.index)), matrix.index)
    axes.set_yticklabels(matrix.index)
    axes.set_title(f"Mean PFLOPs")
    # Cells we want to highlight (row, col)

    # For each cell, work out its square corners
    # imshow aligns cells so (col-0.5, row-0.5) to (col+0.5, row+0.5)
    def draw_edges(cells: list):
        edges = set()
        for r, c in cells:
            # each edge is ((x1, y1), (x2, y2))
            corners = [
                (c - 0.5, r - 0.5), (c + 0.5, r - 0.5),
                (c + 0.5, r + 0.5), (c - 0.5, r + 0.5)
            ]
            square_edges = [
                (corners[0], corners[1]),  # bottom
                (corners[1], corners[2]),  # right
                (corners[2], corners[3]),  # top
                (corners[3], corners[0])  # left
            ]
            for e in square_edges:
                # store edges in a normalized order so shared edges cancel
                e_norm = tuple(sorted(e))
                if e_norm in edges:
                    edges.remove(e_norm)  # internal edge → remove
                else:
                    edges.add(e_norm)  # boundary edge → keep for now

        return edges

    cells = [(4, 1),(4,2), (3,3), (2,4), (3,2), (2, 3), (1, 4)]
    edges = draw_edges(cells)
    for (x1, y1), (x2, y2) in edges:
        axes.plot([x1, x2], [y1, y2], color="yellow", linewidth=2)

    cells = [(4, 4), (4, 3), (3, 4)]
    edges = draw_edges(cells)
    for (x1, y1), (x2, y2) in edges:
        axes.plot([x1, x2], [y1, y2], color="red", linewidth=2)

    axes.spines['right'].set_visible(False)
    axes.spines['top'].set_visible(False)

    axes.set_xlabel("# Stations")
    axes.set_ylabel("Baseline (km)")
    axes.tick_params(axis='x', labelrotation=45)
    plt.subplots_adjust(left=0.3, bottom=0.1, right=0.85, top=1)
    # from mpl_toolkits.axes_grid1 import make_axes_locatable
    # divider = make_axes_locatable(axes)
    # cax = divider.append_axes("right", size="5%", pad=0.05)  # same height as ax
    fig.colorbar(im, ax=axes, orientation='vertical', pad=0.1, label='PFLOPs', shrink=0.3)

    plt.savefig("HeatmapPermutations.png",dpi=fig.dpi)
    # print(arr)


if __name__ == '__main__':
    heatmap_computing_permutations()

