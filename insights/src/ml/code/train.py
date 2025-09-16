import argparse
import os
from time import time
import joblib
import pandas as pd
import config as config
from config import logger as logger
from pre_processing import PreProcessing
from feature_engineering import FeatureEngineering
import subprocess
import xgboost as xgb


# hyperparameter details
hyperparameters = {
    "objective": "binary:logistic",
    "class_weight": "balanced",
    "n_estimators": 91,
     "learning_rate": 0.02,
    "colsample_bytree": 0.9,
    "subsample":0.7,
    "min_child_weight": 9,
    "max_depth": 6,
    "gamma": 0
}

from sklearn.metrics import accuracy_score, f1_score, precision_score, recall_score, roc_auc_score, roc_curve

def determine_metrics(actual, prediction, prediction_proba):
    """

    :param actual: the true label values
    :param prediction: the predicted label values
    :param prediction_proba: the probability of the label values
    :return: various metrics like accuracy, recall, precision, f1_score, roc_auc_score
    """
    accy = accuracy_score(actual, prediction)
    recal = recall_score(actual, prediction)
    precis = precision_score(actual, prediction)
    f1_scr = f1_score(actual, prediction)
    auc = roc_auc_score(actual, prediction_proba)

    logger.info('accuracy: {} '
                'recall: {} precision: {} '
                'f1_scr: {} auc: {}'.format(accy, recal, precis, f1_scr, auc))

    results = {
        "Accuracy": round(accy, 2),
        "Recall": round(recal, 2),
        "Precision": round(precis, 2),
        "F1_Score": round(f1_scr, 2),
        "AUC": round(auc, 2),
    }

    return results

def parse_args():
    parser = argparse.ArgumentParser()

    ## Required parameters
    parser.add_argument('--train_dir', default=os.environ['SM_CHANNEL_TRAIN'], type=str)

    parser.add_argument('--valid_dir', default=os.environ.get('SM_CHANNEL_VALID', None), type=str)

    parser.add_argument('--output_data_dir', type=str, default=os.environ['SM_OUTPUT_DATA_DIR'])

    parser.add_argument('--model_dir', type=str, default=os.environ['SM_MODEL_DIR'],
                        help='Directory where fine tuned model and pickles are  written.')


    parser.add_argument('--objective', type=str, default=hyperparameters["objective"],
                        help='objective')
    parser.add_argument('--class_weight', type=str, default=hyperparameters["class_weight"],
                        help='epochs')
    parser.add_argument('--n_estimators', type=int, default=hyperparameters["n_estimators"],
                        help='n_estimators')
    parser.add_argument('--learning_rate', type=float, default=hyperparameters["learning_rate"],
                        help='learning_rate')
    parser.add_argument('--colsample_bytree', type=float, default=hyperparameters["colsample_bytree"],
                        help='colsample_bytree')
    parser.add_argument('--subsample', type=float, default=hyperparameters["subsample"],
                        help='subsample')
    parser.add_argument('--min_child_weight', type=int, default=hyperparameters["min_child_weight"],
                        help='min_child_weight')
    parser.add_argument('--max_depth', type=int, default=hyperparameters["max_depth"],
                        help='max_depth')
    parser.add_argument('--gamma', type=float, default=hyperparameters["gamma"],
                        help='gamma')
    parser.add_argument('--tuningBoolean', type=int, default=0,
                        help='0 for training, 1 for hyper-parameter tuning')
    return parser.parse_args()



if __name__ == '__main__':
    t_start = time()  # capturing start time
    # Reading raw data
    logger.info("modeling full training started...")

    try:
        args = parse_args()
        logger.info("Arguments passed {}".format(args));

        ##Reading data from local
        train_loc = os.path.join(args.train_dir, config.FULL_CSV)
        train_df = pd.read_csv(train_loc, index_col=None)
        logger.info("train data frame size  {}".format(train_df.shape));

        y_train = train_df[config.TARGET]
        X_train = train_df[train_df.columns.difference([config.TARGET])]
        #model building
        pp_obj = PreProcessing()
        X_train = pp_obj.fit_transform(X_train)
        logger.info("train data frame size after pre processing {}".format(X_train.shape));

        fe_obj = FeatureEngineering()
        X_train = fe_obj.fit_transform(X_train)
        logger.info("train data frame size after feature engineering {}".format(X_train.shape));

        final_model = xgb.XGBClassifier(objective= args.objective,
                               class_weight=args.class_weight,
                               n_estimators=args.n_estimators,
                               learning_rate=args.learning_rate,
                               #subsample=0.5,
                               colsample_bytree=args.colsample_bytree,
                               subsample=args.subsample,
                               min_child_weight=args.min_child_weight,
                               max_depth=args.max_depth,
                               gamma = args.gamma)

        _predictors = X_train.columns
        final_model.fit(X_train, y_train, verbose=100)

        logger.info("Training successfully completed!");

        prediction = final_model.predict(X_train)
        prediction_prob = final_model.predict_proba(X_train)[:, 1]
        result = determine_metrics(y_train, prediction, prediction_prob)
        logger.info("Model training results {}".format(result))
        # logger.info("Confusion matrix {}".format(pd.crosstab(y_train, prediction, rownames=['actuals'], colnames=['predictions'])))

        # model pipeline artifacts
        pipeline_obj = dict()
        pipeline_obj['pre_process'] = pp_obj
        pipeline_obj['feature_engg'] = fe_obj
        pipeline_obj['features'] = _predictors
        pipeline_obj['model'] = final_model

        model_pickle_file = config.PIPELINE_PKL
        joblib.dump(pipeline_obj, model_pickle_file)



        subprocess.call("mv {} {}".format(model_pickle_file, args.model_dir).split(" "))
        logger.info("Training artifacts moved to {}".format(args.model_dir));
    except Exception as e:
        logger.error(e)
        raise e

    t_end = time()
    logger.info("total time taken for full modeling is {} minutes".format((t_end - t_start) / 60))
    print("model full training is completed.")