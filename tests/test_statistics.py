# Test the observation.statistics module
import pprint
import random
import unittest
import pandas as pd

from skaworkflows.common import Telescope
from skaworkflows.observation.permutations import  allocate_observations, create_hpso_counts_from_ratios

from skaworkflows.observation.statistics import count_observation_instances, \
    list_observation_tuples_from_json, observation_weighting

from skaworkflows.config_generator import create_observing_plans

class TestCountObservations(unittest.TestCase):

    def test_count(self):
        observation_amounts, total_obs = create_hpso_counts_from_ratios(days=1)
        single_experiment = pd.Series(
            {"small": 15, "medium": 3, "large": 2})
        plan = allocate_observations(observation_amounts, total_obs,
                                     single_experiment)
        counts = count_observation_instances(plan)
        self.assertEqual(15, counts['small'])
        self.assertEqual(3, counts['medium'])
        self.assertEqual(2, counts['large'])

    def test_count_week(self):
        """
        Confirm we can get the expected number of observations from a week of plans
        Returns
        -------

        """
        observation_amounts, total_obs = create_hpso_counts_from_ratios(days=7)
        # plans = create_hpso_plan('low', num_plans=1)
        observation_sizes = pd.Series(
            {"small": 15, "medium": 70, "large": 5})
        plan = allocate_observations(observation_amounts, total_obs,
                                     observation_sizes)
        counts = count_observation_instances(plan)
        self.assertEqual(15, counts['small'])
        self.assertEqual(70, counts['medium'])
        self.assertEqual(5, counts['large'])

class TestObservationPlanWeightingCalc(unittest.TestCase):
    """
    Generate an observation plan using:
    - generate_multiple_plans
    - process_hpso_from_spec
    - create_basic_plan
    """

    def test_plan_calc(self):
        """
        Confirm that the weightings we get from observation plans that have been shuffled will not always be true.

        For certain plans, there'll be the same because it will be a plan of all the same observing size.
        """
        random.seed(101)
        plans = create_observing_plans(days=3, telescope=Telescope('low'), num_shuffled_plans=5, concurrent_demand=False)
        weights = []
        for plan in plans:
            w = plan['plan_weights']
            weights += w
        self.assertTrue(len(set(weights)) > 1)
