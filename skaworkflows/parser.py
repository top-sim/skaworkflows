import argparse


"""
Run time directive for generating observing plans


What is necessary for a complete plan (in order of decisions)? 
1. Global parameters: 
    a. Pick telescope 
    b. Pick HPC infrastructure (CDR, parameteric model) 
    c. Pick number of nodes 
    
2. Observation parameters: 
    a. HPSOs 
    b. which workflows 

3. Workflow parameters
    a. Which graph structure corresponds to the parametric workflow (base graph paths)
    b. do we use data on edges
    c. do we use data calculation during tasks. 

3. Simulation parameters
    a. timestep


There are two ways that we can require input for this information: 

a. Users create their own plans using the TOML approach
b. Users use an 'experiment' create that provides defaults as well as support for ranges of values.  

"""

from skaworkflows.observation import parameters

parameters = parameters.load_observation_defaults("skalow")


parser = argparse.ArgumentParser()
parser.add_argument("--create-base",
                    help="Create a base config file from the default")


import argparse

def parse_args():
    parser = argparse.ArgumentParser(
        description="Tool to create plans using either a custom config or an experiment configuration."
    )

    parser.add_argument("telescope", choices=["SKALow", "SKAMid"],
                        help="Which instrument for which we are generating an observing plan.")

    # Global arguments (apply to both modes)
    parser.add_argument(
        "--timestep", type=int, default=1,
        help="Global timestep parameter for plan execution."
    )

    parser.add_argument(
        "--output-dir", type=str, default=".",
        help="Directory to write outputs or generated config files."
    )

    # Mutually exclusive main config groups: custom or experiment
    mode_group = parser.add_argument_group("Custom observation plan")

    # Option 1: Custom config
    mode_group.add_argument(
        "--custom-config", type=str,
        help="Path to a custom TOML configuration file."
    )

    # Option to generate a base template config
    parser.add_argument(
        "--generate-template", action="store_true",
        help="Generate a base configuration file template for custom mode."
    )

    # Option 2: Experiment parameters
    mode_group.add_argument(
        "--experiment", action="store_true",
        help="Use experiment mode to generate config internally."
    )

    # Experiment-specific arguments
    experiment_group = parser.add_argument_group("Experiment Configuration")
    experiment_group.add_argument(
        "--max_demand", type=int, help="Demand range for the experiment (# Stations used)."
    )
    experiment_group.add_argument(
        "--max_channels", type=int, nargs="+",
        help="List of channels to include in the experiment."
    )
    experiment_group.add_argument(
        "--max_baselines", type=int, nargs="+",
        help="List of baseline configurations to use in the experiment."
    )

    args = parser.parse_args()

    # if not parser
    # Validations
    if args.custom_config and (args.demand or args.channels or args.baselines):
        parser.error("Cannot use experiment parameters with --custom-config.")

    if args.experiment and (not args.demand or not args.channels or not args.baselines):
        # parser.error("--experiment mode requires --demand, --channels, and --baselines.")
        parser.print_help()

    parser.print_help()
    return args

if __name__ == "__main__":
    args = parse_args()
    # print("Parsed arguments:", vars(args))

