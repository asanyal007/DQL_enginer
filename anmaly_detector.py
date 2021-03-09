from pyod.models.copod import COPOD
from pyod.models.abod import ABOD
from pyod.utils.utility import standardizer
from pyod.models.iforest import IForest
import pandas as pd
import pickle

class trainer:
    def __init__(self, train_data, test_data):
        self.train_data = train_data
        self.test_data = test_data

    def test_train_split(self, col):
        X_train = pd.DataFrame(self.train_data.drop(col, axis=1))
        y_train = pd.DataFrame(self.train_data[col])
        X_test = pd.DataFrame(self.test_data.drop(col, axis=1))
        y_test = pd.DataFrame(self.test_data[col])
        return X_train, y_train, X_test, y_test

    def models(self, y_train, model, model_name):
        model_trained = model.fit(y_train)
        pickle.dump(model_trained, open('saved_models/{model_name}'.format(model_name=model_name), 'wb'))
        return model_trained

    def test(self, model_trained, y_test):
        y_train_scores = model_trained.decision_scores_
        y_test_scores = model_trained.decision_function(y_test)
        return y_train_scores, y_test_scores

    def get_anomal_predictions(self, columns, model ):
        # columns = ['L_EXTENDEDPRICE', 'L_TAX']
        scores = {}
        for col in columns:
            X_train, y_train, X_test, y_test = self.test_train_split(col, self.train_data, self.test_data)
            #model = COPOD()
            model_trained = self.models(y_train, model, 'COPOD')
            y_train_scores, y_test_scores = self.test(model_trained, y_test)
            y_test_pred = model_trained.predict(y_test)
            self.test_data['score_anomaly_{}'.format(col)] = y_test_scores
            self.test_data[col] = y_test
            self.test_data['Predicted_anomaly_{}'.format(col)] = y_test_pred
        return self.test_data



