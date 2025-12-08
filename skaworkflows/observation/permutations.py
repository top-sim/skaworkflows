# copyright (c) 2024 rw bunney

# this program is free software: you can redistribute it and/or modify
# it under the terms of the gnu general public license as published by
# the free software foundation, either version 3 of the license, or
# (at your option) any later version.

# this program is distributed in the hope that it will be useful,
# but without any warranty; without even the implied warranty of
# merchantability or fitness for a particular purpose.  see the
# gnu general public license for more details.

# you should have received a copy of the gnu general public license
# along with this program.  if not, see <https://www.gnu.org/licenses/>.

import json
import random
import sys
import logging

import numpy as np
import pandas as pd

from collections import Counter
from pathlib import Path

from sympy.simplify.simplify import sum_add

from skaworkflows.common import SKALow
from skaworkflows.common import (SKALOW_SMALL_PAIRS, SKALOW_MED_PAIRS,
                                 SKALOW_LARGE_PAIRS)

from skaworkflows.observation.observation import HPSOParameter, ObservationPlan
from skaworkflows.config_generator import create_config
from skaworkflows import common
from skaworkflows.observation.parameters import load_observation_defaults

verbose = False
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

FIXED_LOW_CHANNELS_DEMAND = 128

low_observation_defaults = load_observation_defaults("skalow")

mid_observations_defaults = load_observation_defaults("skamid")


def values_to_nparray(value_map, key):
    """
    take the key and get all values from the map
    :param value_map:
    :param key:
    :return:
    """
    return np.fromiter((y[key] for x, y in value_map.items()), int)


def spread_observations_across_demand(number_obs, demand_pool, pairs,
                                      baseline_limit, seed=None):
    """
    given the number of observations and a 'demand pool' of resources (e.g. [64, 128]), spread
    the number of observations across that pool of resources.

    the outcome should be a list that maps a certain number of observations
    to each resource amount, such that all numbers match the total number of observations
    required for that hpso in a given plan (based on the ratio).

    :param number_obs:
    :param demand_pool:
    :return: observations for each resource amount
    """

    if seed is not None:
        random.seed(seed)

    fraction = demand_pool.get('ratio', {})

    if not fraction:
        raise ValueError("'fraction' must be provided and be non-empty.")

    station_types = list(fraction.keys())
    station_counts = {}
    allocated = 0

    # step 1: allocate number of stations per ratio
    for i, s in enumerate(station_types):
        if i == len(station_types) - 1:
            count = number_obs - allocated
        else:
            count = int(round(number_obs * fraction[s]))
            allocated += count
        station_counts[s] = count

    # step 2: track unique (station, baseline) combinations manually
    grouped = {}
    for station, num_obs in station_counts.items():
        acceptable_pairs = [(y, x) for (x, y) in pairs if y == station]
        sample = random.choices(acceptable_pairs, k=num_obs)
        for key in sample:
            # key = (station, baseline)
            if key in grouped:
                grouped[key] += 1
            else:
                grouped[key] = 1

    # step 3: format the output
    result = []
    for (station, baseline), count in grouped.items():
        result.append({'stations': station, 'baseline': baseline, 'num': count,
                       'alpha': pairs, })

    return result


def calc_demand_ratio(hpso_demand, telescope):
    # TODO Deprecate this officially when transitioning mid to new lattice approach
    total_obs = sum([sum(x.values()) for x in hpso_demand.values()])
    total_demand = total_obs * telescope.max_stations
    cumulative_demand = 0
    for hpso, items in hpso_demand.items():
        for antenna, num in items.items():
            cumulative_demand += antenna * num

    return cumulative_demand / total_demand


def make_ternary_experiment(N: int, step: int = 1,
                            max_large_percentage: float = 0.0) -> pd.DataFrame:
    """
    Create ternary-style sequence of experiments with maximum_large for the
    number of "large" observations.

    For every N, there will be Nx3 experiments generated; one for each
    variation of all observations-types.
    """
    if max_large_percentage > 1.0:
        raise ValueError(
            f"max_large_percentage ({max_large_percentage}) must be <= 1.0")

    if step > N:
        raise ValueError(f"step value must be <= N")

    max_large = int(N * max_large_percentage)
    rows = []
    for s in range(0, N + 1, step):
        for m in range(0, N - s + 1, step):
            l = N - s - m
            status = "valid" if l <= max_large else "excluded"
            rows.append({"small": s, "medium": m, "large": l, "status": status})
    return pd.DataFrame(rows)


def ternary_coordinates(df, N):
    """
    Convert (small, medium, large) counts to 2D coordinates for a ternary plot.
    """

    small = df["small"].to_numpy()
    medium = df["medium"].to_numpy()
    large = df["large"].to_numpy()
    x = 0.5 * (2 * medium + large) / N
    y = (np.sqrt(3) / 2) * large / N
    return x, y


# observing multiplier, then
# total observation amounts, then
# lattice generation


def allocate_observations(hpso_counts: dict,
                          total_obs: int,
                          observation_sizes: pd.DataFrame) -> dict:
    """
    Create observation pairs by randomly assigning large/medium/small baseline pairs to HPSOs.

    This function operates in two phases:
    1. First allocates large and medium baseline pairs
    2. Then allocates remaining small baseline pairs

    Parameters
    ----------
    hpso_counts : dict
        Dictionary mapping HPSO IDs to their required observation counts
    total_obs : int 
        Total number of observations that must be allocated
    observation_sizes : pd.DataFrame
        DataFrame containing counts of small/medium/large observations to generate

    Returns
    -------
    dict
        Dictionary mapping HPSO IDs to lists of (baseline, stations) tuples

    Raises
    ------
    ValueError
        If total observations in observation_sizes doesn't match total_obs
    """

    # Validate total observations match
    total_requested = (observation_sizes['small'] +
                       observation_sizes['medium'] +
                       observation_sizes['large'])

    if total_requested != total_obs:
        raise ValueError(
            f"Requested observations ({total_requested}) do not match "
            f"total observations ({total_obs})")

    # Initialize parameters
    excluded_hpsos = ["hpso04a", "hpso05a"]
    experiment_name = (f"experiment_small-{observation_sizes['small']}_"
                       f"medium-{observation_sizes['medium']}_"
                       f"large-{observation_sizes['large']}")
    logger.info("Experiment params: %s", experiment_name)

    remaining_obs = dict(hpso_counts)
    rng = np.random.default_rng()
    # Phase 1: Allocate large and medium pairs
    available_pairs = []
    available_pairs.extend(
        rng.choice(SKALOW_LARGE_PAIRS,
                   observation_sizes['large']).tolist())
    available_pairs.extend(
        rng.choice(SKALOW_MED_PAIRS,
                   observation_sizes['medium']).tolist())

    observation_pairs = {}
    active_hpsos = [h for h in remaining_obs if h not in excluded_hpsos]

    while available_pairs and active_hpsos:
        for hpso in active_hpsos:
            if not available_pairs:
                break

            if hpso not in observation_pairs:
                observation_pairs[hpso] = []

            if len(observation_pairs[hpso]) < remaining_obs[hpso]:
                pair_idx = rng.integers(len(available_pairs))
                observation_pairs[hpso].append(
                    tuple(available_pairs.pop(pair_idx)))
            else:
                del remaining_obs[hpso]
                active_hpsos.remove(hpso)

    # Phase 2: Allocate small pairs
    available_pairs.extend(rng.choice(
        SKALOW_SMALL_PAIRS,
        observation_sizes['small']).tolist())

    active_hpsos = list(remaining_obs.keys())

    while available_pairs and active_hpsos:
        for hpso in active_hpsos[:]:  # Copy for safe iteration
            if not available_pairs:
                break

            if hpso not in observation_pairs:
                observation_pairs[hpso] = []

            if len(observation_pairs[hpso]) >= remaining_obs[hpso]:
                active_hpsos.remove(hpso)
                continue

            pair_idx = rng.integers(len(available_pairs))
            observation_pairs[hpso].append(
                tuple(available_pairs.pop(pair_idx)))

    return observation_pairs


def get_ratio_multiplier_from_seconds(time: int, durations: np.array,
                                      ratios: np.array):
    """
    determine the 'ratio' multiplier for a given set of hpso ratios and durations,
    such that n * ratios gives a total observation plan of at least 'time' length.
    """
    total = 0
    n = 0
    while total < time:
        total += sum(durations * (ratios))
        n += 1
    return n


def create_hpso_counts_from_ratios(days: int =1):
    """
    Produce the correct number of HPSOs based on the default ratios and
    durations, given a set number of days.

    Parameters
    ----------
    days: Number of days for which to create the observation amounts.

    Returns
    -------

    """
    plan_duration = days * 24 * 3600
    observing_ratio_multiplier = get_ratio_multiplier_from_seconds(
            plan_duration,
            values_to_nparray(low_observation_defaults['hpsos'], "duration"),
            values_to_nparray(low_observation_defaults['hpsos'], "observing_ratio")
        )
    total_obs=0
    observation_amounts = {}
    for hpso, d in low_observation_defaults['hpsos'].items():
        tmp = d['observing_ratio'] * observing_ratio_multiplier
        observation_amounts[hpso] = tmp
        total_obs += tmp
    return observation_amounts, total_obs


def generate_multiple_plans(telescope: str, days: int = 1,
                            max_large_percentage: float = 0.25,
                            percent_experiments: float = 1.0,
                            seed: int = 100):
    """
    Generate up to 100 plans

    The variation is in different combinations of small/medium/large observations.

    Returns
    -------

    """

    hpso_counts, total_obs = create_hpso_counts_from_ratios(days)
    step = int(total_obs / (total_obs * percent_experiments))
    experiments = make_ternary_experiment(N=total_obs, step=step, max_large_percentage=0.25)
    plans = []
    valid_experiments = experiments[experiments["status"] == "valid"]
    for i, observation_sizes in valid_experiments.iterrows():

        plans.append((allocate_observations(hpso_counts,
                                            total_obs,
                                            observation_sizes),
                      observation_sizes))
    return plans


def create_hpso_plan(telescope: str, plan_duration: int = 1,
                     max_large_percentage: float = 0.25, percent_experiments: float = 1.0,
                     num_plans: int = 0, seed: int = 100):
    """
    Create a week's worth of observations

    Parameters
    ----------
    telescope : str
        Which telescope to create plans for ('low' or 'mid')
    max_large_percentage : float
        Maximum percentage of large observations allowed (default 0.25)
    percent_experiments : float 
        Percentage of generated permutations to randomly select (default 1.0)
    """

    # one day
    random.seed(seed)
    duration = plan_duration * 24 * 3600
    if telescope == "low":

        permutations = allocate_observations(observation_amounts,
                                             total_obs,
                                             experiment_lattice)
        # Use percentage experiments unless num_observations is set
        if num_plans > 0:
            k = num_plans
        else:
            k = int(len(permutations) * percent_experiments)

        # Randomly select k experiments from permutations
        selected_keys = random.sample(list(permutations.keys()), k=k)

        selected_permutations = {key: permutations[key] for key in
                                 selected_keys}
        return selected_permutations


    elif telescope == "mid":
        ratio_multiplier = get_ratio_multiplier_from_seconds(duration,
                                                             values_to_nparray(
                                                                 mid_observations_defaults[
                                                                     "hpsos"],
                                                                 "duration"),
                                                             values_to_nparray(
                                                                 mid_observations_defaults[
                                                                     "hpsos"],
                                                                 "observing_ratio"), )
        logger.info("creating %d iterations of observations")
        return standard_mid_obs_plan(
            permute_mid_observation_plan(ratio_multiplier))
    else:
        return None


def permute_mid_observation_plan(n=1):
    """
    create combinations of demand
    """

    final_set = {}
    max_largest_demand = 2
    telescope = common.skamid()
    random.seed(100)

    for g in range(100):
        hpso_demand = {key["hpso"]: {} for key in
                       mid_observations_defaults["hpsos"]}
        for i, antenna in enumerate(telescope.stations):
            for hpso in hpso_demand:
                for j in telescope.stations[0:i + 1]:
                    hpso_demand[hpso].update({j: 0})
            # demand pool slowly gets bigger
            number_obs = values_to_nparray(mid_observations_defaults["hpsos"],
                                           "ratio") * n
            ## new code
            prev_hpso = None
            for j, items in enumerate(hpso_demand.items()):
                hpso, demand = items
                obs = spread_observations_across_demand(number_obs[j],
                                                        hpso_demand[hpso])
                prev_d = []
                # allocate demand across antenna options
                for i, d in enumerate(demand):
                    if d == telescope.max_stations:
                        tmp = obs[i]
                        leftover = tmp - max_largest_demand
                        if leftover > 0:
                            demand[d] = max_largest_demand
                            # todo consider experimenting with this by just using
                            # smallest
                            intermediate_obs = {p: 0 for p in prev_d}
                            int_obs = spread_observations_across_demand(
                                leftover, intermediate_obs)
                            for x, key in enumerate(intermediate_obs):
                                demand[key] += int_obs[x]
                        else:
                            demand[d] = obs[i]
                    else:
                        demand[d] = obs[i]
                    if d > 64:
                        prev_d.append(d)

            tmp = {}
            demand_ratio = np.round(calc_demand_ratio(hpso_demand, telescope),
                                    2)
            if demand_ratio in final_set:
                continue
            for hpso, demand in hpso_demand.items():
                tmp[hpso] = []
                for antenna, obs in demand.items():
                    tmp[hpso].append({"demand": antenna, "num_obs": obs})
            final_set[demand_ratio] = tmp


def standard_mid_obs_plan(num_obs_repeats: dict):
    """
    currently, this is a placeholder method to generate one of a couple different
    observation plans.

    expect this method to be a) renamed in the future and b) improved upon

    'hpso13': {'duration': 28800, 'workflows': ["ical", "dprepa", "dprepb", "dprepc"]},
    'hpso15': {'duration': 15840, 'workflows': ["ical", "dprepa", "dprepb", "dprepc"]},
    'hpso22': {'duration': 28800, 'workflows': ["ical", "dprepa", "dprepb"]},
    'hpso32': {'duration': 7920, 'workflows': ["ical", "dprepb"]}


    returns
    -------

    """
    params = []
    # permutations = permute_mid_observation_plan()
    telescope = common.skamid
    for demand, hpso_numbers in num_obs_repeats.items():
        plan = telescope.initialise_plan()
        for hpso, items in hpso_numbers.items():
            for el in items:
                plan.add_observation(
                    HPSOParameter(count=el["num_obs"], hpso=hpso, duration=
                    mid_observations_defaults["hpsos"][hpso]["duration"],
                                  workflows=
                                  mid_observations_defaults["hpsos"][hpso][
                                      "workflows"], demand=el["demand"],
                                  channels=FIXED_LOW_CHANNELS_DEMAND * plan.telescope.channel_multiplier,
                                  workflow_parallelism=el["demand"], baseline=
                                  mid_observations_defaults["hpsos"][hpso][
                                      "baseline"],
                                  telescope=str(plan.telescope)))
        params.append(plan)

    if verbose:
        print(json.dumps(params, indent=2, cls=common.npencoder))

    return params


def convert_low_plan_to_json(selection: dict, verbose: bool = False):
    """
    currently, this is a placeholder method to generate one of a couple different
    observation plans.

    expect this method to be a) renamed in the future and b) improved upon

    parameters
    ----------

    returns
    -------

    """
    # observation_numbers = permute_low_observation_plans(days)

    params = {}
    from collections import Counter
    for name, combination in selection.items():
        plan = ObservationPlan("low")
        logger.info("generating plan for: %s", name)
        for hpso, items in combination.items():
            counter = dict(Counter(items))
            for pair, count in counter.items():
                baseline, stations = pair
                plan.add_observation(HPSOParameter(count=count, hpso=hpso,
                                                   duration=
                                                   low_observation_defaults[
                                                       "hpsos"][hpso][
                                                       "duration"], workflows=
                                                   low_observation_defaults[
                                                       "hpsos"][hpso][
                                                       "workflows"],
                                                   stations=stations,
                                                   channels=FIXED_LOW_CHANNELS_DEMAND * plan.telescope.channels_multiplier,
                                                   workflow_parallelism=stations,
                                                   baseline=baseline,
                                                   # *1000, # convert to meters
                                                   telescope=str(
                                                       plan.telescope)))
        params[name] = plan.to_json()

    if verbose:
        print(
            json.dumps(params, indent=2, cls=common.npencoder, sort_keys=True))

    logger.info("Plans created:")
    for key in params:
        logger.info("\t %s", key)
    return params


import argparse

if __name__ == "__main__":

    parser = argparse.ArgumentParser(Path(__file__).name, )
    parser.add_argument("path")
    parser.add_argument("telescope", help="choose from 'low' or 'mid'")
    parser.add_argument("graph_type", help="prototype, parallel")
    parser.add_argument("--test", default=False, action="store_true")
    parser.add_argument("--tables", default=False, action="store_true",
                        help='Generate tables and do not run config generation')

    # parser.add_argument() # todo num_observation_repeats, seed
    args = parser.parse_args()

    workflow_type_map = {"ICAL": args.graph_type, "DPrepA": args.graph_type,
                         "DPrepB": args.graph_type, "DPrepC": args.graph_type,
                         "DPrepD": args.graph_type, "Pulsar": "pulsar", }

    random.seed(2)
    if args.test:
        verbose = True
        random.seed(0)
        n = get_ratio_multiplier_from_seconds(7 * 24 * 3600, values_to_nparray(
            low_observation_defaults, "duration"), values_to_nparray(
            low_observation_defaults, "ratio"), )
        params = convert_low_plan_to_json(days=7)
        json.dumps(params, indent=2, cls=common.npencoder)

        sys.exit(0)

    all_params = convert_low_plan_to_json(create_hpso_plan(args.telescope))
    if args.tables:
        sys.exit(0)

    low_path = Path(args.path) / args.telescope

    print("creating config")
    # sys.exit()
    print(f"total plans: {len(all_params)}")
    # for ap in all_params:
    # sorted_keys = sorted(ap)
    # for multiplier in [1, 2, 5]:
    for name, plan in all_params.items():
        print(f"creating plan with demand: {name}")
        create_config(plan, low_path, workflow_type_map, timestep=5, data=False,
                      data_distribution="standard", multiple_plans=False, )
        create_config(plan, low_path, workflow_type_map, timestep=5, data=True,
                      data_distribution="standard", multiple_plans=False, )
        create_config(plan, low_path, workflow_type_map, timestep=5, data=True,
                      data_distribution="edges", multiple_plans=False, )
