#!/usr/bin/env python3

"""
This flow isn't really meant to run.

The logic in `log_housing_events.py` is meant to log lineage events
that simulate this flow being run.

In other words, if this flow were instrumented with OpenLineage,
if would log the same events that `log_housing_events.py` does.
"""

from metaflow import FlowSpec, step, Parameter, conda_base
import pandas as pd
import sqlite3  # Replace with appropriate DB connector for production
from sklearn.linear_model import LinearRegression
from sklearn.metrics import mean_squared_error

@conda_base(libraries={"pandas": "1.5.3", "scikit-learn": "1.2.2"})
class HousingRegressionFlow(FlowSpec):

    db_path = Parameter("db_path", help="Path to database", default="housing.db")

    @step
    def start(self):
        self.conn = sqlite3.connect(self.db_path)  # Replace with real DB client
        self.next(self.prepare_data)

    @step
    def prepare_data(self):
        cursor = self.conn.cursor()

        query = """
        CREATE OR REPLACE TABLE cleaned_sales AS
        SELECT *
        FROM house_sales
        WHERE price BETWEEN 10000 AND 1000000;

        CREATE OR REPLACE TABLE enriched_sales AS
        SELECT s.*, l.zipcode, l.school_rating
        FROM cleaned_sales s
        JOIN location_info l ON s.house_id = l.house_id;

        CREATE OR REPLACE TABLE features AS
        SELECT sqft, bedrooms, bathrooms, school_rating, price
        FROM enriched_sales
        WHERE sqft IS NOT NULL
          AND bedrooms IS NOT NULL
          AND bathrooms IS NOT NULL
          AND school_rating IS NOT NULL;
        """
        cursor.executescript(query)
        self.conn.commit()
        self.next(self.train_model)

    @step
    def train_model(self):
        df = pd.read_sql_query("SELECT sqft, bedrooms, bathrooms, school_rating, price FROM features", self.conn)
        X = df.drop(columns="price")
        y = df["price"]

        model = LinearRegression().fit(X, y)
        self.model = model
        self.mse = mean_squared_error(y, model.predict(X))
        self.next(self.end)

    @step
    def end(self):
        print(f"Model trained successfully. MSE: {self.mse:,.2f}")
