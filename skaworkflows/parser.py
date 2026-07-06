import argparse

from skaworkflows.config_generator import create_observing_plans, create_config

from skaworkflows.common import Telescope
from skaworkflows.observation import parameters


parameters = parameters.load_observation_defaults("skalow")


parser = argparse.ArgumentParser()
parser.add_argument(
    "--create-base", help="Create a base config file from the default"
)


def run():
    return create_config(
        days=1,
        num_of_plans=5,
        num_shuffled_plans=3,
        concurrent_demand=64,
    )


def create_maximal_plans():
    pass


def parse_args():
    parser = argparse.ArgumentParser(
        description="Tool to create plans using either a custom config or an experiment configuration."
    )

    parser.add_argument(
        "telescope",
        choices=["SKALow", "SKAMid"],
        help="Which instrument for which we are generating an observing plan.",
    )

    parser.add_argument(
        "--timestep",
        type=int,
        default=1,
        help="Global timestep parameter for plan execution.",
    )

    parser.add_argument(
        "--output-dir",
        type=str,
        default=".",
        help="Directory to write outputs or generated config files.",
    )

    mode_group = parser.add_argument_group("Custom observation plan")

    mode_group.add_argument(
        "--custom-config",
        type=str,
        help="Path to a custom TOML configuration file.",
    )

    parser.add_argument(
        "--generate-template",
        action="store_true",
        help="Generate a base configuration file template for custom mode.",
    )

    mode_group.add_argument(
        "--experiment",
        action="store_true",
        help="Use experiment mode to generate config internally.",
    )

    experiment_group = parser.add_argument_group("Experiment Configuration")
    experiment_group.add_argument(
        "--max_demand",
        type=int,
        help="Demand range for the experiment (# Stations used).",
    )
    experiment_group.add_argument(
        "--max_channels",
        type=int,
        nargs="+",
        help="List of channels to include in the experiment.",
    )
    experiment_group.add_argument(
        "--max_baselines",
        type=int,
        nargs="+",
        help="List of baseline configurations to use in the experiment.",
    )

    args = parser.parse_args()

    if args.custom_config and (args.demand or args.channels or args.baselines):
        parser.error("Cannot use experiment parameters with --custom-config.")

    if args.experiment and (
        not args.demand or not args.channels or not args.baselines
    ):
        parser.print_help()

    parser.print_help()
    return args


if __name__ == "__main__":
    from skaworkflows.cli import main

    main()
