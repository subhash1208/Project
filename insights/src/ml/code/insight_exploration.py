# Created by Abdul on 16-April-2019

from __future__ import division
import numpy as np
import pandas as pd
import s3fs
import random
import os
import pyarrow.parquet as pq





class InsightExploration:
    """
    Class used for Insight Exploration using the thompson sampling algorithm
    """
    def __init__(self,
                 data_loc="/data/",
                 arms_col_name="arms",
                 click_col_name="clicks",
                 no_clicks_col_name="no_clicks"):
        """

        :param data_loc: the location of the input data
        :param arms_col_name: the name of the arm column
        :param click_col_name: the name of the click column
        :param no_clicks_col_name: the name of the no_clicks column
        """
        self.max_trails_threshold = 500  # maximum number of arm trails after which the information if perturbed
        s3 = s3fs.S3FileSystem()
        self.data = pq.ParquetDataset(data_loc, filesystem=s3).read_pandas().to_pandas()
        self.arms_col_name = arms_col_name
        self.arms = self.data[arms_col_name].values
        self.clicks = self.data[click_col_name].values
        self.no_clicks = self.data[no_clicks_col_name].values

    def pull_arm(self, alpha, beta):
        """
        Function to draw reward from beta distribution
        :param alpha: the first parameter to a beta distribution
        :param beta: the second parameter to a beta distribution
        :return: random sample drawn from beta distribution
        """
        return random.betavariate(alpha + 1, beta + 1)

    def best_arm(self, arms_index):
        """
        Function used to determine the best arm
        :param arms_index: the indices value of the given arms
        :return: the arm with the highest reward
        """
        max_reward = -1
        selected_arm = -1
        arms = self.arms[arms_index]
        clicks = self.clicks[arms_index]
        no_clicks = self.no_clicks[arms_index]
        arms_pull = np.sum((clicks, no_clicks), axis=0)

        for i, arm in enumerate(arms):
            if arms_pull[i] > self.max_trails_threshold:  # used to provide some uncertainity when trails increase
                clicks[i] = self.max_trails_threshold * clicks[i] / arms_pull[i]
                no_clicks[i] = self.max_trails_threshold * no_clicks[i] / arms_pull[i]

            beta_reward = self.pull_arm(clicks[i], no_clicks[i])
            if beta_reward > max_reward:
                max_reward = beta_reward
                selected_arm = i

        return arms[selected_arm]

    def pull(self, given_arms):
        """
        # Function used to pull and return the best arm
        :param given_arms: the list of arms to be explored
        :return: the best arm selected
        """
        arms_index = np.where(self.data[self.arms_col_name].isin(given_arms))
        arm_selected = self.best_arm(arms_index)

        return arm_selected


''' Usage
from bandit_algorithm import InsightExploration

ins_exp = InsightExploration(data_loc="/Users/abdulqam/data", file_name="multi_arm_stats.csv")
given_arms = ['yr_cd-month_cd-client_internal-15', 'yr_cd-client_internal-12']
selected_arm = ins_exp.pull(given_arms)
'''






