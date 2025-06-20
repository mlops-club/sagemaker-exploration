from metaflow import FlowSpec, card, current, pypi, pypi_base, step, Config
from metaflow.cards import ProgressBar
from pydantic import BaseModel
from pathlib import Path
from helpers.validate_config import make_pydantic_parser_fn


THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Config for the flow--provides validation and autocompletion."""

    glue_database_name: str
    glue_database_s3_bucket_name: str


# @pypi_base(
#     python="3.12",
#     packages={
#         "numpy": "1.26.3",
#         "pandas": "2.2.3",
#         "httpx": "0.28.1",
#         "snowflake-connector-python": "3.13.1",
#         "scikit-learn": "1.6.1",
#     },
# )
class ExtractLoadDataFlow(FlowSpec):
    """Flow to extract NYC taxi data from the TLC website and load it into Snowflake."""

    config: FlowConfig = Config(
        name="config",
        default=str(THIS_DIR / "configs/default.yaml"),
        parser=make_pydantic_parser_fn(FlowConfig),
    )  # type: ignore[assignment]

    @step
    def start(self):
        """Start the flow by downloading the last 3 months of NYC taxi data."""
        self.next(self.download_data)

    @card(type="blank")
    @step
    def download_data(self):
        """Load the downloaded data into the target system (e.g., database or data warehouse); show progress bar."""
        from helpers.download_trip_data import (
            download_last_n_months_of_data_if_not_already_downloaded,
        )

        progress_bar = ProgressBar(value=0, max=3, label="Downloading months", unit="month")
        current.card.append(progress_bar)

        download_iterator = download_last_n_months_of_data_if_not_already_downloaded(n_months=3)
        for i, _ in enumerate(download_iterator):
            progress_bar.update(i + 1)
            current.card.refresh()

        self.next(self.create_tables)


    # @pypi(packages={"awswrangler": ""})
    @step
    def create_tables(self):
        """Create Iceberg table in Athena using raw SQL CREATE TABLE statement."""
        import awswrangler as wr
        import os

        os.environ["AWS_PROFILE"] = "sandbox"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        import boto3
        print(boto3.client("sts").get_caller_identity())
        
        # Define the Iceberg table creation SQL statement
        create_yellow_table_stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.config.glue_database_name}.raw_yellow (
            unique_row_id string,
            filename string,
            VendorID string,
            tpep_pickup_datetime timestamp,
            tpep_dropoff_datetime timestamp,
            passenger_count int,
            trip_distance double,
            RatecodeID string,
            store_and_fwd_flag string,
            PULocationID string,
            DOLocationID string,
            payment_type int,
            fare_amount double,
            extra double,
            mta_tax double,
            tip_amount double,
            tolls_amount double,
            improvement_surcharge double,
            total_amount double,
            congestion_surcharge double
        )
        PARTITIONED BY (day(tpep_pickup_datetime))
        LOCATION 's3://{self.config.glue_database_s3_bucket_name}/raw_yellow/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        )
        """
        
        try:
            # Execute the CREATE TABLE statement using AWS Data Wrangler
            result = wr.athena.start_query_execution(
                sql=create_yellow_table_stmt,
                database=self.config.glue_database_name,
                wait=True
            )
            
            print(f"Successfully created Iceberg table 'raw_yellow' in database '{self.config.glue_database_name}'")
            print(f"Query execution ID: {result['QueryExecutionId']}")
            print(f"Table location: s3://{self.config.glue_database_s3_bucket_name}/raw_yellow/")
            
        except Exception as e:
            print(f"Error creating table: {str(e)}")
            # Check if it's just because the table already exists
            if "already exists" in str(e).lower():
                print("Table already exists, which is expected with 'IF NOT EXISTS' clause.")
            else:
                raise e

        self.next(self.upsert_yellow_taxi_data)

    @pypi(packages={"awswrangler": ""})
    @step
    def upsert_yellow_taxi_data(self):
        """Load and upsert yellow taxi data into the raw_yellow Iceberg table."""
        import awswrangler as wr
        import pandas as pd
        import os
        import uuid
        from helpers.download_trip_data import DATA_DIR

        os.environ["AWS_PROFILE"] = "sandbox"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        # Find all downloaded yellow taxi CSV files
        yellow_taxi_files = list(DATA_DIR.glob("*yellow_tripdata*.csv"))
        
        if not yellow_taxi_files:
            print("No yellow taxi data files found to load.")
            self.next(self.end)
            return
        
        print(f"Found {len(yellow_taxi_files)} yellow taxi files to process")
        
        for file_path in yellow_taxi_files:
            print(f"Processing file: {file_path.name}")
            
            try:
                # Read the CSV file
                df = pd.read_csv(file_path)
                
                # Add metadata columns
                df['filename'] = file_path.name
                df['unique_row_id'] = [str(uuid.uuid4()) for _ in range(len(df))]
                
                # Ensure proper data types for timestamp columns
                timestamp_cols = ['tpep_pickup_datetime', 'tpep_dropoff_datetime']
                for col in timestamp_cols:
                    if col in df.columns:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                
                # Handle any missing or null values that might cause issues
                df = df.dropna(subset=['tpep_pickup_datetime'])  # Remove rows without pickup datetime
                
                if len(df) == 0:
                    print(f"No valid data in {file_path.name} after cleaning")
                    continue
                
                # Upsert data using merge functionality
                wr.athena.to_iceberg(
                    df=df,
                    database=self.config.glue_database_name,
                    table="raw_yellow",
                    temp_path=f"s3://{self.config.glue_database_s3_bucket_name}/temp/",
                    table_location=f"s3://{self.config.glue_database_s3_bucket_name}/raw_yellow/",
                    partition_cols=["day(tpep_pickup_datetime)"],
                    merge_cols=["unique_row_id"],  # Use unique_row_id as the merge key
                    merge_condition="update",  # Update existing records if they match
                    mode="append",
                    keep_files=True
                )
                
                print(f"Successfully upserted {len(df)} records from {file_path.name}")
                
            except Exception as e:
                print(f"Error processing {file_path.name}: {str(e)}")
                # Continue with next file instead of failing the entire step
                continue
        
        print("Completed upserting yellow taxi data")
        self.next(self.end)

    


    # @pypi(packages={"duckdb": "1.2.2"})
    # @step
    # def load_data_into_duckdb(self):
    #     """Load the downloaded data into a DuckDB database."""
    #     from helpers.create_duckdb import create_all_tables
    #     from helpers.download_trip_data import DATA_DIR

    #     create_all_tables(data_dir=DATA_DIR, db_path=DATA_DIR / "nyc_taxi_data.duckdb")
    #     self.next(self.end)

    # form each of the 4 sets of tables into a duckdb database

    # @pypi(packages={"snowflake-connector-python": "3.12.2"})
    # @step
    # def upload_data_to_s3(self):
    #     print("SampleSnowflakeFlow is starting.")
    #     from metaflow import Snowflake

    #     with Snowflake(integration="outerbounds-snowflake-prod-integration") as cn:
    #         print("Connected to Snowflake using static")

    #         cursor = cn.cursor()
    #         cursor.execute("SELECT CURRENT_ROLE()")
    #         current_role = cursor.fetchone()[0]
    #         print(f"Current role: {current_role}")
    #     self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        ...


if __name__ == "__main__":
    ExtractLoadDataFlow()
