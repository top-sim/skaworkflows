import argparse
import sys
from pathlib import Path

from skaworkflows.config_generator import create_config, config_to_shadow
from skaworkflows.utils.analysis import (
    heatmap_computing_permutations,
    plan_weighting_demonstration,
)


def add_config_parser(subparsers):
    parser = subparsers.add_parser(
        "config", help="Generate skaworkflow configuration"
    )
    parser.add_argument(
        "--days", type=int, default=7, help="Number of days to plan"
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path.cwd(),
        help="Output directory for config files",
    )
    parser.add_argument(
        "--imaging-graph-base",
        default="prototype",
        choices=["prototype", "scatter"],
        help="Base workflow graph to use",
    )
    parser.add_argument(
        "--timestep",
        default="seconds",
        help="Simulation timestep",
    )
    parser.add_argument(
        "--num-plans",
        type=int,
        default=40,
        dest="num_of_plans",
        help="Number of observation permutations to generate",
    )
    parser.add_argument(
        "--num-shuffled-plans",
        type=int,
        default=5,
        dest="num_shuffled_plans",
        help="Shuffled plan variations per permutation",
    )
    parser.add_argument(
        "--concurrent-demand",
        type=int,
        default=0,
        dest="concurrent_demand",
        help="Concurrent station demand",
    )
    parser.add_argument(
        "--telescope",
        default="low",
        choices=["low", "mid"],
        help="Telescope to plan for",
    )
    parser.add_argument(
        "--infrastructure",
        default="parametric",
        choices=["parametric", "cdr"],
        help="HPC infrastructure model",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite existing config"
    )
    parser.set_defaults(func=_run_config)


def _run_config(args):
    paths = create_config(
        days=args.days,
        telescope=args.telescope,
        infrastructure=args.infrastructure,
        output_dir=args.output_dir,
        imaging_graph_base=args.imaging_graph_base,
        timestep=args.timestep,
        overwrite=args.overwrite,
        num_of_plans=args.num_of_plans,
        num_shuffled_plans=args.num_shuffled_plans,
        concurrent_demand=args.concurrent_demand,
    )
    for p in paths:
        print(p)


def add_shadow_parser(subparsers):
    parser = subparsers.add_parser(
        "shadow",
        help="Translate skaworkflow config to SHADOW-compatible format",
    )
    parser.add_argument(
        "cfg_path", type=Path, help="Path to the skaworkflow config JSON"
    )
    parser.set_defaults(func=_run_shadow)


def _run_shadow(args):
    import json

    result = config_to_shadow(args.cfg_path)
    json.dump(result, sys.stdout, indent=2)
    print()


def add_analysis_parser(subparsers):
    parser = subparsers.add_parser(
        "analysis", help="Data analysis and visualisation tools"
    )
    sub = parser.add_subparsers(dest="analysis_command", required=True)

    heatmap_parser = sub.add_parser(
        "heatmap", help="Generate HPSO computing permutations heatmap"
    )
    heatmap_parser.add_argument(
        "--output",
        default="HeatmapPermutations.png",
        help="Output filename for the heatmap",
    )
    heatmap_parser.set_defaults(func=_run_heatmap)

    weighting_parser = sub.add_parser(
        "weighting",
        help="Demonstrate observation plan weighting strategies",
    )
    weighting_parser.add_argument(
        "--seed", type=int, default=None, help="Random seed"
    )
    weighting_parser.add_argument(
        "--n-events",
        type=int,
        default=40,
        dest="n_events",
        help="Number of events to generate",
    )
    weighting_parser.add_argument(
        "--target-hours",
        type=int,
        default=168,
        dest="target_hours",
        help="Target scheduling window in hours",
    )
    weighting_parser.add_argument(
        "--output",
        default="plan-weight-demonstration.png",
        help="Output filename for the plot",
    )
    weighting_parser.set_defaults(func=_run_weighting)


def _run_heatmap(args):
    import matplotlib.pyplot as plt

    heatmap_computing_permutations()
    import shutil

    shutil.move("HeatmapPermutations_even_less.png", args.output)
    print(f"Heatmap saved to {args.output}")


def _run_weighting(args):
    import matplotlib

    matplotlib.use("Agg")
    result = plan_weighting_demonstration(
        seed=args.seed,
        n_events=args.n_events,
        target_hours=args.target_hours,
    )
    print(f"Weighting demonstration saved to plan-weight-demonstration.png")
    print(f"  Clustered max score:  {result['clustered_max']:.2f}")
    print(f"  Equispaced max score: {result['equispaced_max']:.2f}")


def main():
    parser = argparse.ArgumentParser(
        description="SKA Workflows tools and utilities"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    add_config_parser(subparsers)
    add_shadow_parser(subparsers)
    add_analysis_parser(subparsers)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
