"""
Calculate metrics and useful summative information for observation plans
"""
import numpy as np
from skaworkflows.common import SKALOW_LARGE_PAIRS, SKALOW_MED_PAIRS, \
    SKALOW_SMALL_PAIRS

def list_observation_tuples_from_json(json_dict):
    """
    Given json-dictionary of complete observtion information, construct a
    simpler list of tuples of (baseline, station) pairs.

    For use in conjunction with `count_observation_instances`.

    Parameters
    ----------
    json_dict

    Returns
    -------
    list
    """
    keys = set([v['hpso'] for v in json_dict["hpsos"]])
    pairs = {hpso:[] for hpso in keys}
    for observation in json_dict["hpsos"]:
        pairs[observation['hpso']].append((observation["baseline"], observation[
            "stations"]))
    return pairs

def count_observation_instances(plan):
    """
    Counts the number of observations of each observing pair in a given plan

    Parameters
    ----------
    plan

    Returns
    -------
    dict: number of instances of each observing pair
    """

    # Squash all pairwise values from the plan into a single list
    pairs = [el for sub in plan.values() for el in sub]

    return {
        'small':sum(1 for p in pairs if p in SKALOW_SMALL_PAIRS),
        'medium': sum(1 for p in pairs if p in SKALOW_MED_PAIRS),
        'large': sum(1 for p in pairs if p in SKALOW_LARGE_PAIRS)
    }

def get_observation_weight(obs):
    """Helper function to determine observation weight based on baseline-station pairs"""
    baseline = obs.baseline
    stations = obs.stations
    if (baseline, stations) in SKALOW_LARGE_PAIRS:
        return 9  # 3^2
    elif (baseline, stations) in SKALOW_MED_PAIRS:
        return 4  # 2^2
    else:
        return 1  # 1^2


def observation_weighting(plan: list):
    """
    This metric determines how far apart 'large' observations are from small
    this observaitons in a given plan, based on the small/medium/large pairs
    that we construct in skaworkflows.common.

    We use a sliding window of 24 hours of observations, with the sliding
    incrementing of 18000 seconds. There, we calculate the weighting factor of
    that each observation using a qudratically increasing scale:
        - Small: 1^2
        - Medium: 2^2
        - Large: 3^2

    From this, we multiply each weighting factor by the observation duration,
    and normalise the sum of this over 24 hours. This will give us the average
    'weighted' observation demand across that window. The goal is to move this
    through the entire plan to identify the largest weighted window.

    To take into account an observing plan is not run in isolation,
    as it both follows and precedes another, we pre- and append the plan to
    itself, starting the first 24 hour period with 18 hours in the previous
    plan, and continuing to 18 hours into the following plan. This is to
    reduce the bias that may be associated with a plan that on the face of it
    looks 'good', but actually puts all high-demand observations at the
    beginning and end.
    """
    if not plan:
        return 0

    # Constants
    DAY_SECONDS = 24 * 3600
    WINDOW_STEP = 18000

    # Create extended plan (pre and post append)
    extended_plan = plan + plan + plan
    max_weighted_window = 0

    # Slide window through extended plan
    start_time = extended_plan[
                     0].start_time + DAY_SECONDS * 0.75  # Start at 18 hours into first plan
    end_plan = extended_plan[
                   -1].start_time - DAY_SECONDS * 0.75  # End 18 hours before last plan ends

    current_time = start_time
    while current_time < end_plan:
        window_end = current_time + DAY_SECONDS
        window_weight = 0

        # Calculate weights for observations in current window
        for obs in extended_plan:
            if current_time <= obs.start_time < window_end:
                weight = get_observation_weight(obs)
                window_weight += weight * obs.duration

        max_weighted_window = max(max_weighted_window, window_weight)
        current_time += WINDOW_STEP

    return int(max_weighted_window)

