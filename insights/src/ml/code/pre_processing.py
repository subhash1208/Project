from sklearn.base import BaseEstimator, TransformerMixin
import numpy as np
import pandas as pd
import math


class PreProcessing(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.feature_vals = {}

    def fit(self, X):
        cat_col = list(X.columns[X.dtypes == object])
        num_col = [col for col in X.columns if col not in cat_col]

        for col in num_col:  # Capturing median for numerical features
            notnull_index = X[col].notnull()
            if sum(notnull_index) > 0:
                self.feature_vals[col] = X[col][notnull_index].median()
            else:
                self.feature_vals[col] = -99

        for col in cat_col:  # Capturing mode for categorical features
            notnull_index = X[col].notnull()
            if sum(notnull_index) > 0:
                self.feature_vals[col] = X[col][notnull_index].mode()[0]
            else:
                self.feature_vals[col] = -99

        return self

    def transform(self, X):

        null_col = list(set(self.feature_vals.keys()).intersection(set(X.columns[X.isnull().sum() > 0].tolist())))
        not_null_col = list(X.columns.difference(null_col))
        numeric_cols = X.select_dtypes(np.number).columns

        sub_dict = {x: self.feature_vals[x] for x in null_col if x in self.feature_vals}
        imputed_values = np.where(X[null_col].isna(), list(sub_dict.values()), X[null_col].values)
        Y = pd.concat([pd.DataFrame(imputed_values, columns=null_col),
                       pd.DataFrame(X[not_null_col].values, columns=not_null_col)], axis=1)
        Y[numeric_cols] = Y[numeric_cols].apply(pd.to_numeric)
        Y = Y.reindex(columns=X.columns.tolist(), fill_value=0)

        return Y
