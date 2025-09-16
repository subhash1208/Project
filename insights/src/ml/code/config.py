import os
import sys
import logging


# s3 details
BUCKET = "adp-ds-emi"
PREFIX = "emi/data"
DATAFILENAME = "emi_recommender_dataset.csv.gz"
DATA_DIR = "data"
ENV = "dev"
TRAIN_PATH = "/train"
VALID_PATH = "/validation"
INFERENCE_OUTPUT_PATH = "/inference"
TEST_PATH = "/test"


# file name details
TRAIN_CSV = "train.csv.gz"
VALID_CSV = "valid.csv.gz"
TEST_CSV = "test.csv.gz"
FULL_CSV = "full.csv.gz"

# data details
VALID_PCT = 0.2
NUM_TRAIN_DAYS = "90day"
NUM_TEST_DAYS = "15day"

# MAB config parameters
MAB_ZIP = "multi_arm_stats.zip"
MAB_FILE = "multi_arm_stats.csv"
MAB_ARM = "bandit_arm"
MAB_CLICKS = "decay_clicks_rt"
MAB_NO_CLICKS = "decay_non_clicks_rt"

#Meta-inf
MF_LOC= "META-INF"
MF_FILE = "MANIFEST.MF"

# model
CAT_COLS = ["metric", "ins_rsn", "ins_type", 'dimension_comb','age_band_ky_', 'time_grain', 'flsa_status_', 'is_manager_',
            'nbr_of_diments']
DIM_COLS = ["hr_orgn_id", "job_cd", "yr_cd", "qtr_cd", "mnth_cd", "iso_3_char_cntry_cd",
               "state_prov_cd", "work_loc_cd", "trmnt_rsn", "adp_lense_cd", "inds_cd", "sector_cd", "super_sect_cd",
               "mngr_pers_obj_id"]
DROP_COLS = ["city_id", "normalised_percentage_diff", "percentile_rank"]
HIST_COLS=["hist_metric_2","hist_metric_201","hist_metric_202","hist_metric_5","hist_metric_57","hist_metric_60",
           "hist_metric_69","hist_metric_73","hist_metric_74","hist_metric_76","hist_metric_79"]

META = [['user', 'age_band_ky_'], ['user', 'direct_children'],['user', 'total_children'],['user', 'flsa_status_'],['user', 'is_manager_'],
        ['interaction_history', 'hist_metric_2'],['interaction_history', 'hist_metric_201'],['interaction_history', 'hist_metric_202'],
        ['interaction_history', 'hist_metric_5'],['interaction_history', 'hist_metric_57'],['interaction_history', 'hist_metric_59'],
        ['interaction_history', 'hist_metric_60'],['interaction_history', 'hist_metric_63'],['interaction_history', 'hist_metric_65'],
        ['interaction_history', 'hist_metric_69'],['interaction_history', 'hist_metric_73'],['interaction_history', 'hist_metric_74'],
        ['interaction_history', 'hist_metric_76'],['interaction_history', 'hist_metric_78'], ['interaction_history', 'hist_metric_79']]

# Config parameters
TARGET = "clicks"
EXCLUDE_COLS = ["ins_hash_val"]
RESPONSE_COLS = ["ins_hash_val", "metric", "is_bandit"]
FEATURES_PKL = "model/features.pkl"
PIPELINE_PKL = "emi_model_pipeline.pkl"

# log details
LOG_PATH = "logs"
LOG_FILENAME = "emi.log"

try:
    if not os.path.exists(LOG_PATH):
        os.makedirs(LOG_PATH)

    log_file = os.path.join(LOG_PATH, LOG_FILENAME)

    logging.basicConfig(level=logging.INFO,
                        format='%(asctime)s %(name)s %(levelname)-8s %(funcName)s:%(lineno)d %(message)s',
                        handlers=[
                            logging.FileHandler(log_file),
                            logging.StreamHandler(sys.stdout)
                        ])

    logger = logging.getLogger()

except Exception as exp:
    print("Exception while initializing  EMI recommender model logger", exp)
    logger.error("Exception occured while setting up EMI recommender model logger", exp)


