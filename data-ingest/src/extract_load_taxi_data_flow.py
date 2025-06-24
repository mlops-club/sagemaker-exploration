from metaflow import FlowSpec, card, current, step, Config
from metaflow.cards import ProgressBar
from pydantic import BaseModel
from pathlib import Path
import pandas as pd
import hashlib
from helpers.validate_config import make_pydantic_parser_fn


THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Config for the flow--provides validation and autocompletion."""

    glue_database_name: str
    glue_database_s3_bucket_name: str


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
            -- we should really add an `ingest_ts` column as well
               -- since it's so easy to accidentally get this ingest wrong and need to delete bad data
            unique_row_id string, -- we calculate this col in Python before upserting
            filename string,
            ingest_timestamp timestamp, -- timestamp when data was ingested
            vendorid int,
            tpep_pickup_datetime timestamp,
            tpep_dropoff_datetime timestamp,
            passenger_count double,
            trip_distance double,
            ratecodeid double,
            store_and_fwd_flag string,
            pulocationid int,
            dolocationid int,
            payment_type bigint,
            fare_amount double,
            extra double,
            mta_tax double,
            tip_amount double,
            tolls_amount double,
            improvement_surcharge double,
            total_amount double,
            congestion_surcharge double,
            cbd_congestion_fee double,
            airport_fee double
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
            # result = wr.athena.start_query_execution(
            #     sql=f"""DELETE FROM {self.config.glue_database_name}.raw_yellow WHERE TRUE;""",
            #     database=self.config.glue_database_name,
            #     wait=True
            # )
            # result = wr.athena.start_query_execution(
            #     sql=f"""DROP TABLE IF EXISTS {self.config.glue_database_name}.raw_yellow;""",
            #     database=self.config.glue_database_name,
            #     wait=True
            # )
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

    @step
    def upsert_yellow_taxi_data(self):
        """Load and upsert yellow taxi data into the raw_yellow Iceberg table."""
        import awswrangler as wr
        # import fireducks.pandas as pd
        import pandas as pd
        import os
        import uuid
        from helpers.download_trip_data import DATA_DIR

        os.environ["AWS_PROFILE"] = "sandbox"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        # Find all downloaded yellow taxi parquet files
        yellow_taxi_dir = DATA_DIR / "yellow"
        yellow_taxi_files = list(yellow_taxi_dir.glob("*.parquet"))
        
        if not yellow_taxi_files:
            print("No yellow taxi parquet files found to load.")
            self.next(self.end)
            return
        
        print(f"Found {len(yellow_taxi_files)} yellow taxi parquet files to process")
        
        for file_path in yellow_taxi_files:
            print(f"Processing file: {file_path.name}")
            
            try:
                # Read the parquet file
                df = pd.read_parquet(file_path)
                df.rename(columns=lambda x: x.lower(), inplace=True)  # Ensure all columns are lowercase
                
                # Add metadata columns
                df['filename'] = file_path.name
                df['ingest_timestamp'] = pd.Timestamp.now(tz='UTC').floor('S').tz_localize(None)

                # Assign the result
                hashed_columns = [
                    'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime',
                    'pulocationid', 'dolocationid', 'fare_amount', 'trip_distance',
                    'total_amount', 'improvement_surcharge', 'congestion_surcharge',
                    'airport_fee', 'cbd_congestion_fee', 'passenger_count', 'ratecodeid',
                    'store_and_fwd_flag', 'payment_type'
                ]
                df['unique_row_id'] = fast_md5_hash(df, hashed_columns=hashed_columns)

                df.drop_duplicates(inplace=True)

                temp_path = f"s3://{self.config.glue_database_s3_bucket_name}/temp/{uuid.uuid4()}/"

                # NOTE: WAP would have saved me here. My IDs weren't unique enough because
                # even with all the fields I hashed, total_amount was still different between
                # otherwise *completely identical rows*. If I had done a WAP, I could have
                # run a query to assert that the incoming parquet file's unique_row_id's were
                # ACTUALLY unique when compared to the entire existing table. That could be done
                # on a branch and then rejected if the IDs were not unique.

                # Upsert data using merge functionality
                wr.athena.to_iceberg(
                    df=df,
                    database=self.config.glue_database_name,
                    table="raw_yellow",
                    temp_path=temp_path,
                    table_location=f"s3://{self.config.glue_database_s3_bucket_name}/{self.config.glue_database_name}/raw_yellow/",
                    partition_cols=["day(tpep_pickup_datetime)"],
                    merge_cols=["unique_row_id"],  # merge on all columns # Use unique_row_id as the merge key
                    # merge_cols=list(df.columns.str.lower()),  # merge on all columns # Use unique_row_id as the merge key
                    merge_condition="update",  # Update existing records if they match
                    mode="append",
                    # keep_files=True
                )

                print(f"Deleting objects as temporary path {temp_path} ...", sep="")
                wr.s3.delete_objects(path=temp_path)
                print("Objects deleted")
                
                print(f"Successfully upserted {len(df)} records from {file_path.name}")
                
            except Exception as e:
                print(f"Error processing {file_path.name}: {str(e)}")
                # Continue with next file instead of failing the entire step
                raise e
        
        print("Completed upserting yellow taxi data")
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        ...


def fast_md5_hash(df: pd.DataFrame, hashed_columns: list[str]) -> pd.Series:
    """Vectorized MD5 hash from concatenated string of selected columns."""
    
    # Lowercase column names for uniformity (optional, if source is inconsistent)
    df_renamed = df.rename(columns={col: col.lower() for col in hashed_columns})
    
    # Fill NaNs and convert to string
    str_cols = df_renamed[[col.lower() for col in hashed_columns]].fillna('').astype(str)
    
    # Concatenate all fields into a single string per row
    concat_series = str_cols.agg(''.join, axis=1)
    
    # Hash each row's string using MD5
    return concat_series.map(lambda x: hashlib.md5(x.encode('utf-8')).hexdigest())


if __name__ == "__main__":
    ExtractLoadDataFlow()
