import sys
import gc
import os
import pandas as pd
import pyarrow.parquet as pq

from sklearn.model_selection import train_test_split

import config as config
from config import logger

from pre_processing import PreProcessing
from feature_engineering import FeatureEngineering


input_dir = "/opt/ml/processing/input/"
output_dir = "/opt/ml/processing/output/"
output_dir_train = output_dir + "train/"
output_dir_validation = output_dir + "validation/"

def save_df(df, path):
    df.to_csv(path, sep='\t', index=False)


def create_dir(dir):
    try:
        os.makedirs(dir)
    except:
        pass


def read_data_from_s3():
    logger.info("reading data...")
    # insights_raw = pd.read_csv(input_dir + filename, delimiter='|', index_col=None)
    insights_raw = pq.ParquetDataset(input_dir, filesystem=None).read_pandas().to_pandas()
    logger.info("Number of rows and columns in data is {}".format(insights_raw.shape))
    logger.info("Size of data is {}".format(str(sys.getsizeof(insights_raw) / 10 ** 6) + " MB"))
    insights_raw['ins_serv_dt'] = pd.to_datetime(insights_raw['ins_serv_dt'], format='%Y-%m-%d').dt.date
    logger.info("given data min insight served date: {}".format(insights_raw['ins_serv_dt'].min()))
    logger.info("given data max insight served date: {}".format(insights_raw['ins_serv_dt'].max()))
    return insights_raw

def split_save_data(insights_raw_df, train_file_name, valid_file_name, test_file_name, full_file_name):
    logger.info("split_save_data...")
    insights_raw_df = insights_raw_df[
        insights_raw_df.ins_serv_dt > insights_raw_df['ins_serv_dt'].max() - pd.to_timedelta(config.NUM_TRAIN_DAYS)].reset_index(drop=True)
    logger.info("After  {} filter , min insight served date: {} ,  max insight served date: {}, shape :{}".format(config.NUM_TRAIN_DAYS, insights_raw_df['ins_serv_dt'].min(),  insights_raw_df['ins_serv_dt'].max(),  insights_raw_df.shape))


    test_startdate = insights_raw_df['ins_serv_dt'].max() - pd.to_timedelta(config.NUM_TEST_DAYS)
    logger.info("test startdate is {}".format(test_startdate))

    train_df = insights_raw_df.loc[insights_raw_df.ins_serv_dt < test_startdate]
    test_df = insights_raw_df.loc[insights_raw_df.ins_serv_dt >= test_startdate]
    logger.info("shape of train data: {} and test data: {}".format(train_df.shape, test_df.shape))

    gc.collect()


    train_df, valid_df = train_test_split(train_df, test_size=config.VALID_PCT, random_state=1)
    logger.info("shape of train data: {} and valid data: {}".format(train_df.shape, valid_df.shape))


    create_dir(f"{output_dir}/train")
    train_output_file = f"{output_dir}/train/{train_file_name}"
    train_df.to_csv(train_output_file, index=False)
    logger.info(f"Saved train dataset to {train_output_file}. shape :{train_df.shape}")

    create_dir(f"{output_dir}/valid")
    valid_output_file = f"{output_dir}/valid/{valid_file_name}"
    valid_df.to_csv(valid_output_file, index=False)
    logger.info(f"Saved valid dataset to {valid_output_file}. shape :{valid_df.shape}")

    create_dir(f"{output_dir}/test")
    test_output_file = f"{output_dir}/test/{test_file_name}"
    test_df.to_csv(test_output_file, index=False)
    logger.info(f"Saved test dataset to {test_output_file}. shape :{test_df.shape}")

    create_dir(f"{output_dir}/full")
    full_output_file = f"{output_dir}/full/{full_file_name}"
    insights_raw_df.to_csv(full_output_file, index=False)
    logger.info(f"Saved full dataset to {full_output_file}. shape :{insights_raw_df.shape}")

    return


def preprocess(filename , train_csv, valid_csv, test_csv, full_csv):
    """Read Dataset from S3. Preprocess. Do train-test split. Save as CSV.

    :return: None
    """
    insights_raw_df = read_data_from_s3()
    split_save_data(insights_raw_df, train_csv, valid_csv, test_csv, full_csv)
    return


if __name__ == "__main__":
    filename = config.DATAFILENAME
    train_file_name = config.TRAIN_CSV
    valid_file_name = config.VALID_CSV
    test_file_name = config.TEST_CSV
    full_file_name = config.FULL_CSV

    try:
        preprocess(filename, train_file_name, valid_file_name, test_file_name, full_file_name)
        logger.info("Pre-processing successful.")
        sys.exit(0)
    except Exception as e:
        logger.error("Pre-processing unsuccessful due to an error. Error is %s", e)
        sys.exit(255)