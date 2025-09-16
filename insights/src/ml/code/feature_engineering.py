from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.preprocessing import RobustScaler
import pandas as pd


class FeatureEngineering(BaseEstimator, TransformerMixin):
    def __init__(self):
        self.sel_cols = pd.read_csv("features.txt", header=None)[0]
        self.scaler = RobustScaler()
        self.numeric_cols = None
        self.cat_cols = None

    def fit(self, X):
        numeric_cols = set(X.columns[X.dtypes != object])
        self.numeric_cols = list(numeric_cols.intersection(set(self.sel_cols)))
        cat_cols = set(X.columns[X.dtypes == object])
        self.cat_cols = list(cat_cols.intersection(set(self.sel_cols)))

        self.scaler.fit(X[self.numeric_cols])

        return self

    def transform(self, X):
        # Ensuring all features to be present
        X = X.reindex(columns=self.sel_cols, fill_value=0)
        # sel_cols = [col for col in X.columns if col not in self.drop_cols]
        X = X[self.sel_cols].copy()

        X[self.numeric_cols] = self.scaler.transform(X[self.numeric_cols])

        if len(self.cat_cols) > 0:
            X = pd.get_dummies(X, columns=self.cat_cols)

        return X
