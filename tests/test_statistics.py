# Test the observation.statistics module
import pprint
import unittest
import pandas as pd

from skaworkflows.observation.permutations import create_hpso_plan, \
    allocate_observations, create_hpso_counts_from_ratios, \
    convert_low_plan_to_json

from skaworkflows.observation.statistics import count_observation_instances, \
    list_observation_tuples_from_json


class TestCountObservations(unittest.TestCase):

    def test_count(self):
        observation_amounts, total_obs = create_hpso_counts_from_ratios(days=1)
        single_experiment = pd.DataFrame(
            {"small": [15], "medium": [3], "large": [2], 'status': 'valid'})
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
        plans = create_hpso_plan('low', num_plans=1)
        counts = count_observation_instances(plans)
        self.assertEqual(15, counts['small'])
        self.assertEqual(70, counts['medium'])
        self.assertEqual(5, counts['large'])

    def test_count_week_json_to_counts(self):
        """
        Confirm we can get the expected number of observations from the json
        """
        plans = create_hpso_plan('low', num_plans=1)
        json_dict = convert_low_plan_to_json(plans)
        key = list(json_dict.keys())[0]
        config_dict = list_observation_tuples_from_json(json_dict[key])
        counts = count_observation_instances(plans)
        self.assertEqual(15, counts['small'])
        self.assertEqual(70, counts['medium'])
        self.assertEqual(5, counts['large'])
