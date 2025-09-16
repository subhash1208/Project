from __future__ import absolute_import
import os
import joblib
import pandas as pd
import json
from random import randint
import datetime
from config import logger as logger
from pandas.io.json import json_normalize

import config as config
from insight_exploration import InsightExploration

from pre_processing import PreProcessing
from feature_engineering import FeatureEngineering

from sagemaker_containers.beta.framework import (
    encoders, env, modules, transformer, worker)


JSON_CONTENT_TYPE = 'application/json'

def validate_input(request_json):
    """ Validates input request for
        1. If zero insights
        2. If insights information missing
        3. If user is missing

    :return: None  if successful, otherwise throws exception

    """
    logger.info("JSON Request received  %s ", request_json)
    try:
        if len(request_json["insights"]) == 0:
            raise Exception("At least one Insight.")
        elif len([ins for ins in request_json["insights"] if ins["ins_hash_val"] == {}]) > 0:
            raise Exception("Insight information")
        elif not request_json["user"]:
            raise Exception("User information")
    except Exception as exp:
            raise Exception("Key error. Request doesn't contain "+str(exp))


def read_input(request_json):
    """ Reads data from request and Flatten/explodes Nested JSON
        ensuring column names without prefix

    :return:  data frame with properly structured for prediction

    """

    logger.info("User id = %s , Client id = %s", request_json["user"].get("user_id"),
                 request_json["user"].get("client_id"))
    request_ip = json_normalize(data=request_json, record_path=["insights"],
                                meta=config.META, errors='ignore')
    request_ip.columns = request_ip.columns.str.replace("^user.", "")
    request_ip.columns = request_ip.columns.str.replace("^interaction_history.", "")
    request_ip[config.HIST_COLS] =request_ip[config.HIST_COLS].fillna(0)

    request_ins_meta = json_normalize(request_ip["ins_json"])

    request_ins_meta.columns = request_ins_meta.columns.str.replace("^insight.", "")
    return pd.merge(request_ip, request_ins_meta, left_index=True, right_index=True)



def label_time_grain(row):
    try:
        if row['mnth_cd']:
            return 'month'
        if row['qtr_cd']:
            return 'quarter'
    except Exception as exp:
        logger.exception("Exception occurred while calculating label time grain weight. So default label (year) is set. Root cause is %s", exp)
    return 'year'

def calc_time_grain_weight(row):
    try:
        month = datetime.datetime.strptime(row['rec_crt_ts'], "%b %d, %Y %H:%M:%S %p").month
        if row['time_grain'] == 'month':
            return 1
        if row['time_grain'] == 'year':
            return 1.0/month
        if row['time_grain'] == 'quarter':
            if month % 3 == 0:
                return 1/3
            if month % 3 == 1:
                return 1
            if month % 3 == 2:
                return 2/3
    except Exception as exp:
        logger.exception("Exception occurred while calculating time grain weight. So default weight = 1 is set. Root cause is %s", exp)
    return 1

def get_ins_rsn(row):
    try:
        if "TIME" in row["ins_rsn"]:
            return row["ins_rsn"][0:row["ins_rsn"].index('_TIME')]
    except Exception as exp:
        logger.exception("Exception occurred while deriving insight reason. So default is used. Root cause is %s",exp)
    return row["ins_rsn"]

def get_hash_dimension(row):
    return '_'.join(sorted([col for col in config.DIM_COLS if pd.notnull(row[col])]))


def modify_input(request_df):
    """

    :return: data frame with hash_dimension_comb , dimension_comb, grain, grain_weight and ins_rsn
    """
    logger.debug ("request_df shape before adding derived features".format(request_df.shape))
    request_df['dimension_comb'] = request_df.apply(lambda row: get_hash_dimension(row), axis=1)
    request_df['hash_dimension_comb'] = request_df[['metric', 'ins_type', 'dimension_comb']].apply(lambda x: '{}_{}_{}'.format(x[0], x[1].lower(), x[2]), axis=1)
    request_df['time_grain'] = request_df.apply(lambda row: label_time_grain(row), axis=1)
    request_df['time_grain_weight'] = request_df.apply(lambda row: calc_time_grain_weight(row), axis=1)
    request_df['ins_rsn'] = request_df.apply(lambda row: get_ins_rsn(row), axis=1)
    try:
        request_df.drop(config.DIM_COLS, axis=1, inplace=True)
        request_df.drop(config.DROP_COLS, axis=1, inplace=True)
    except Exception as exp:
        logger.error("Exception occurred while dropping dim_cols/drop_cols.Hence dropping skipped.Root cause is %s ",exp)
    logger.debug("data shape after  derived features".format(request_df.shape))
    return request_df


def preprocess(request_ip_df, pipeline):
    """ Does pre-processing
        1. dummies
        2. lower case convertion
        3. feature re-indexing

    :param request_ip_df: input data frame with user, insights and interaction history

    :return:  preprocessed data frame

    """
    # select features needed for prediction

    include_cols = request_ip_df.columns.difference(config.EXCLUDE_COLS)
    request_data = request_ip_df[include_cols]
    logger.debug("Input request shape %s %s", request_data.shape, request_data.columns)

    request_data = pipeline["pre_process"].transform(request_data)


    # pre processing to ensure same features
    request_data = pd.get_dummies(data=request_data, columns=config.CAT_COLS)
    request_data.columns = request_data.columns.str.lower()
    logger.info("Input request after pre processing and dummies(before re-index) ,shape = %s ,"
                 "columns list = %s ", request_data.shape, request_data.columns)
    request_data = request_data.reindex(columns=pipeline["features"], fill_value=0)

    logger.info("Input request after re-index ,shape = %s , columns list = %s ", request_data.shape,
                  request_data.columns)
    request_data = pipeline["feature_engg"].transform(request_data)
    logger.info("Input request data shape after complete pre processing and reindexing  %s %s", request_data.shape,
                 request_data.columns)
    return request_data


def model_fn(model_dir):
    """Load a model. For XGBoost Framework, a default function to load a model is not provided.
    Users should provide customized model_fn() in script.
    Args:
        model_dir: a directory where model is saved.
    Returns: A XGBoost model.
    """
    print("In default_model_fn, MAB path ", os.environ.get("mab_file_path"))
    try:
        with open(os.path.join(model_dir, config.PIPELINE_PKL), "rb") as pl:
            pipeline = joblib.load(pl)
        logger.info("pipeline loaded successfully. %s", pipeline)

        mab_obj = InsightExploration(data_loc=os.environ.get("mab_file_path"),
                                         arms_col_name=config.MAB_ARM,
                                         click_col_name=config.MAB_CLICKS, no_clicks_col_name=config.MAB_NO_CLICKS)
        logger.info("MAB object loaded successfully. %s", mab_obj)
        return pipeline, mab_obj
    except Exception as exp:
        raise Exception('Exception occurred  in loading model/mab . Error is {}'.format(exp))


def input_fn(input_data, content_type=JSON_CONTENT_TYPE):
    """Take request data and de-serializes the data into an object for prediction.
        When an InvokeEndpoint operation is made against an Endpoint running SageMaker model server,
        the model server receives two pieces of information:
            - The request Content-Type, for example "application/json"
            - The request data, which is at most 5 MB (5 * 1024 * 1024 bytes) in size.
        The input_fn is responsible to take the request data and pre-process it before prediction.
    Args:
        input_data (obj): the request data.
        content_type (str): the request Content-Type.
    Returns:
        (obj): data ready for prediction. For XGBoost, this defaults to DMatrix.
    """
    logger.info('Deserializing input data')
    logger.info('Request  data received  {}'.format(input_data))
    logger.info('Raw Request type {}'.format(str(type(input_data))))
    if content_type == JSON_CONTENT_TYPE:
        if input_data is not None:
            request_json = json.loads(input_data)
            validate_input(request_json)
            request_ip = read_input(request_json)
            request_ip = modify_input(request_ip)
            return request_ip
    else:
        raise Exception('This predictor only supports JSON data. Requested unsupported content_type {}'.format(content_type))


def predict_fn(request_ip, model):
    logger.info("Loading  model and MAB objects as returned by model_fn")
    pipeline, mab_obj = model
    request_data = preprocess(request_ip,pipeline)
    request_ip["likes_pred_prob"] = pipeline["model"].predict_proba(request_data)[:, 1]
    request_ip["is_bandit"] = 0
    logger.debug("Sorted Insights  immediately after ML prediction are  %s ",
                 request_ip.sort_values(by="likes_pred_prob", ascending=False)["ins_hash_val"])
    # ensuring business rule - top four insights should be distinct with respect to metric
    ml_model_ins = request_ip.sort_values(by="likes_pred_prob", ascending=False).drop_duplicates('metric').head(
        4)
    response_idx = ml_model_ins.index.tolist()
    logger.info("Top four  insights from ML model : %s", request_ip.loc[response_idx]["ins_hash_val"])

    # prediction from MAB
    if len(set(request_ip.index) - set(response_idx)) > 0:
        data_mab = request_ip.loc[set(request_ip.index) - set(response_idx)]
        logger.info("Data frame size for MAB prediction : %s ", data_mab.shape)
        try:
            hash_dimension = mab_obj.pull(data_mab["hash_dimension_comb"].unique())
            logger.info("hash_dimension returned from MAB : %s ", hash_dimension)
            mab_ins = data_mab[(data_mab["hash_dimension_comb"] == hash_dimension)].sort_values(by=["empl_cnt"],
                                                                                                ascending=False).head(
                1)
            random_idx = randint(0, len(response_idx))
            logger.info("Random index for MAB : %s ", random_idx + 1)
            response_idx.insert(random_idx, mab_ins.index.tolist()[0])
            request_ip.at[mab_ins.index.tolist()[0], "is_bandit"] = 1
            logger.info("Top five  insights including MAB: %s ", request_ip.loc[response_idx]["ins_hash_val"])
        except Exception as exp:
            logger.exception("An error occurred when pulling MAB arm - No hash_dimension returned from MAB. "
                             "Hence skipped MAB integration with ML model.Error message is %s", exp)

    # Rank insights other than the above five insights (ML+MAB) based on likes_pred_prob
    # append them at the end of above 5
    other_idx = request_ip.loc[set(request_ip.index) - set(response_idx)].sort_values(by="likes_pred_prob",
                                                                                      ascending=False).index.tolist()
    response_idx.extend(other_idx)
    logger.debug("Total insights to be returned with sorted order: %s ",
                 request_ip.loc[response_idx]["ins_hash_val"])

    # Return all insights based on rank
    insights = request_ip.iloc[response_idx]
    # logger.info("total time taken for EMI recommendation API is {} seconds".format(time() - t_start))
    return insights[config.RESPONSE_COLS].values.tolist()


def output_fn(prediction, accept=JSON_CONTENT_TYPE):
    logger.info('Serializing predictions as JSON')
    if accept == JSON_CONTENT_TYPE:
        # return json.dumps(pred_output), accept
        return worker.Response(encoders.encode(prediction, accept), mimetype=accept)
    raise Exception('Requested unsupported content_type {}'.format(accept))