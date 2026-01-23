import pprint
import unittest

import pandas as pd

from skaworkflows.common import SKALOW_SMALL_PAIRS, SKALOW_LARGE_PAIRS, SKALOW_MED_PAIRS
from skaworkflows.observation.permutations import allocate_observations, \
    create_hpso_counts_from_ratios, generate_multiple_plans
from skaworkflows.observation.permutations import (
    get_ratio_multiplier_from_seconds, values_to_nparray,
    make_ternary_experiment)
from skaworkflows.observation.parameters import load_observation_defaults

from skaworkflows.observation.observation import process_hpso_from_spec
from skaworkflows.observation.statistics import count_observation_instances

DAY_SECONDS = 24 * 3600
WEEK_SECONDS = 7 * DAY_SECONDS

class TestTernaryExperimentGenerator(unittest.TestCase):

    def test_one_step(self):
        experiment_df = make_ternary_experiment(1, 1)
        print(experiment_df)
        self.assertEqual(3, len(experiment_df))
        experiment_df = make_ternary_experiment(5, 1)
        self.assertEqual(21, len(experiment_df))

    def test_alt_step(self):
        experiment_df = make_ternary_experiment(6, 2)
        self.assertEqual(10, len(experiment_df))

    def test_boundaries(self):
        self.assertRaises(ValueError, make_ternary_experiment, 1, 10)
        self.assertRaises(ValueError, make_ternary_experiment, 5, 1, 1.5)

class TestObservationPlanSupportMethods(unittest.TestCase):
    """
    Confirm that the observation plan support methods work as expected
    """
    def test_create_observation_amounts(self):
        """
        Make sure that we get the correct number of observations with single
        ratio multiplier.

        create_observation amounts builds on the observation default
        parameters.
        """
        obs_amounts, total_obs = create_hpso_counts_from_ratios(days=1)
        self.assertEqual(total_obs, 20)


    def test_create_observation_amounts_week(self):
        """
        This tests both the get_ratio_multiplier_from_seconds method and the
        the create_observation_amounts method and ensures they work together.

        """
        # ratio_multiplier = get_ratio_multiplier_from_seconds(
        #     WEEK_SECONDS,
        #     values_to_nparray(load_observation_defaults("skalow"), "duration"),
        #     values_to_nparray(load_observation_defaults("skalow"), "ratio")
        # )
        # self.assertEqual(9, ratio_multiplier)
        obs_amounts, total_obs = create_hpso_counts_from_ratios(days=7)
        self.assertEqual(total_obs, 90)


class TestPermuteObservationPlans(unittest.TestCase):
    def test_single_plan_all_small(self):
        import random
        observation_amounts, total_obs = create_hpso_counts_from_ratios(1)
        observation_sizes = pd.Series(
            {"small": 20, "medium": 0, "large": 0})
        plan = allocate_observations(observation_amounts, total_obs,
                                     observation_sizes)
        random.seed(0)

        for hpso, pairs in plan.items():
            self.assertEqual(observation_amounts[hpso], len(pairs))

        counts = count_observation_instances(plan)
        self.assertEqual(observation_sizes['small'], counts['small'])
        self.assertEqual(observation_sizes['medium'], counts['medium'])
        self.assertEqual(observation_sizes['large'], counts['large'])

    def test_single_plan(self):
        import random
        observation_amounts, total_obs = create_hpso_counts_from_ratios(1)
        observation_sizes = pd.Series(
            {"small": 15, "medium": 3, "large": 2})
        plan = allocate_observations(observation_amounts, total_obs,
                                     observation_sizes)
        random.seed(0)

        for hpso, pairs in plan.items():
            self.assertEqual(observation_amounts[hpso], len(pairs))

        counts = count_observation_instances(plan)
        self.assertEqual(observation_sizes['small'], counts['small'])
        self.assertEqual(observation_sizes['medium'], counts['medium'])
        self.assertEqual(observation_sizes['large'], counts['large'])

    def test_single_plan_no_small(self):
        import random
        observation_amounts, total_obs = create_hpso_counts_from_ratios(1)
        observation_sizes = pd.Series(
            {"small": 0, "medium": 15, "large": 5})
        plan = allocate_observations(observation_amounts, total_obs,
                                     observation_sizes)
        random.seed(0)
        # Make sure we have all 5 HPSOs
        self.assertEqual(5, len(plan))
        for hpso, pairs in plan.items():
            self.assertEqual(observation_amounts[hpso], len(pairs))

        counts = count_observation_instances(plan)
        self.assertEqual(observation_sizes['small'], counts['small'])
        self.assertEqual(observation_sizes['medium'], counts['medium'])
        self.assertEqual(observation_sizes['large'], counts['large'])

    def test_multiple_plans(self):
        """
        Use the same test as above, but with 'generate_multiple_plans'

        Returns
        -------
        """

        plans = generate_multiple_plans('low', 1, percent_experiments=1)
        self.assertEqual(111, len(plans))
        prev = plans.pop(0)
        for plan in plans:
            prv_plan, prv_counts = prev
            p, expected_counts = plan
            actual_counts = count_observation_instances(p)

            # Make sure we have all 5 HPSOs in every plan
            self.assertEqual(5, len(p))

            # Make sure each plan is different from the previous
            self.assertNotEqual(count_observation_instances(prv_plan),
                                actual_counts)

            # Make sure each plan has what we expect
            self.assertEqual(expected_counts['small'], actual_counts['small'])
            self.assertEqual(expected_counts['medium'],actual_counts['medium'])
            self.assertEqual(expected_counts['large'], actual_counts['large'])

            prev = plan

class TestPlanShuffle(unittest.TestCase):

    def test_shuffle(self):
        pass


class TestPermutationRatioMultiplier(unittest.TestCase):

    def setUp(self):
        low_observation_defaults = load_observation_defaults("skalow")
        self.default_duration = values_to_nparray(low_observation_defaults["hpsos"],
                                                  "duration")
        self.default_ratio = values_to_nparray(low_observation_defaults["hpsos"],
                                               "observing_ratio")

    def test_number_observations_in_day(self):
        n = get_ratio_multiplier_from_seconds(DAY_SECONDS, self.default_duration,
                                              self.default_ratio)
        self.assertEqual(2, n)

    def test_number_observations_in_week(self):
        n = get_ratio_multiplier_from_seconds(WEEK_SECONDS,
                                              self.default_duration, self.default_ratio)
        self.assertEqual(9, n)


class TestPlanGenerationSKALow(unittest.TestCase):

    def test_create_day_plan(self):
        """
        Confirm we create a day of observations based on observation defaults

        See data.observation.low_defaults.toml
        """

    def test_create_weekly_plan(self):
        pass

class TestPlanGenerationStacking(unittest.TestCase):
    """
    Test our ability to generate a week long plan, _and_ 'stack' the
    observations if they are smaller
    """


@unittest.skip("Limited support for Mid at the moment.")
class TestObservationCalculationsMid(unittest.TestCase):

    def setUp(self):
        mid_observation_defaults = load_observation_defaults("mid")
        self.default_duration = values_to_nparray(mid_observation_defaults,
                                                  "duration")
        self.default_ratio = values_to_nparray(mid_observation_defaults,
                                               "ratio")

    def test_number_observations_in_day_low(self):
        n = get_ratio_multiplier_from_seconds(self.day_seconds,
                                              self.default_duration, self.default_ratio)

    def test_number_observations_in_day(self):
        low_observation_defaults = load_observation_defaults("skalow")
        day_seconds = 24 * 3600
        one_week_seconds = 7 * day_seconds
        n = get_ratio_multiplier_from_seconds(day_seconds,
                                              values_to_nparray(low_observation_defaults, "duration"),
                                              values_to_nparray(low_observation_defaults, "ratio"), )
