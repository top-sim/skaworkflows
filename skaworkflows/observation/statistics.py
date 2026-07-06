"""
Calculate metrics and useful summative information for observation plans
"""
import copy

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
        return 9  # 2^2
    elif (baseline, stations) in SKALOW_MED_PAIRS:
        return 4  # 2^2
    else:
        return 1  # 2^1


def observation_weighting(plan: list, post_run=False):
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

    To take into account that an observing plan is not run in isolation,
    as it both follows and precedes another, we prepend and append the plan to
    itself, starting the first 24 hour period with 18 hours in the previous
    plan, and continuing to 18 hours into the following plan. This is to
    reduce the bias that may be associated with a plan that on the face of it
    looks 'good', but actually puts all high-demand observations at the
    beginning and end.
    """
    if not plan:
        return 0

    # Constants
    step = 21600
    window = 86400
    spillover = 2*3600
    # Create extended plan (pre and post append)
    # The beginning of the plan is what will appear just after the end of this plan
    total = 0
    i = 0
    post_plan = []
    while total < step:
        o = copy.deepcopy(plan[i])
        total += o.duration
        post_plan.append(o)
        i+=1
    # The beginning of the plan is what will appear just before the start of this plan
    total = 0
    i = 1
    pre_plan = []
    while total < step:
        o = copy.deepcopy(plan[-i])
        total += o.duration
        pre_plan.append(o)
        i+=1

    pre_plan.reverse()
    extended_plan = pre_plan + plan + post_plan

    max_weighted_window = 0

    elapsed = 0
    left = 0
    right = 0
    start_time = 0  # NEW: cumulative time at left
    total_duration = sum(obs.duration for obs in extended_plan)

    while start_time + window <= total_duration:  # CHANGED: hard stop

        spillover_used = False
        window_duration = 0
        total_weight = 0

        # Expand window to the right until we have at least 24h (+ optional spillover)
        while right < len(extended_plan):

            obs = extended_plan[right]
            dur = obs.duration

            if window_duration + dur <= window:
                window_duration += dur
                total_weight += get_observation_weight(obs) * dur
                right += 1

            elif not spillover_used:
                # allow ONE spillover event
                window_duration += dur
                total_weight += get_observation_weight(obs) * dur
                right += 1
                break

            else:
                break

        if window_duration == 0:
            break
        # At this point we KNOW we have >= 24h (guaranteed by outer while)
        weighted_window = total_weight / window_duration
        max_weighted_window = max(weighted_window, max_weighted_window)

        # Slide window forward by 6h
        step_remaining = step

        while left < right and step_remaining > 0:
            dur = extended_plan[left].duration

            if dur <= step_remaining:
                step_remaining -= dur
                window_duration -= dur
                start_time += dur  # NEW: advance absolute time
                left += 1
            else:
                # we don't allow partial events on the left
                break

        max_weighted_window = max(weighted_window, max_weighted_window)

    return max_weighted_window

