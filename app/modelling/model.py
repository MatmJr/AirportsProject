from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator
import pyspark.ml.tuning as tune
import numpy as np

class LogisticRegressionTrainer:
    def __init__(self, data):
        self.data = data
        self.training, self.test = self.data.randomSplit([0.6, 0.4])
        self.lr = LogisticRegression()
        self.evaluator = BinaryClassificationEvaluator(metricName='areaUnderROC')

    def train(self):
        # Create the parameter grid
        grid = tune.ParamGridBuilder()

        # Add the hyperparameters
        grid = grid.addGrid(self.lr.regParam, np.arange(0, .1, .01))
        grid = grid.addGrid(self.lr.elasticNetParam, [0, 1])

        # Build the grid
        grid = grid.build()

        # Create the CrossValidator
        cv = tune.CrossValidator(estimator=self.lr,
                                 estimatorParamMaps=grid,
                                 evaluator=self.evaluator)

        # Fit cross-validation models
        self.models = cv.fit(self.training)

        # Extract the best model
        self.best_lr = self.models.bestModel

    def evaluate(self):
        # Use the best model to predict the test set
        test_results = self.best_lr.transform(self.test)

        # Evaluate the predictions
        roc_auc = self.evaluator.evaluate(test_results)
        print("Area Under ROC:", roc_auc)
