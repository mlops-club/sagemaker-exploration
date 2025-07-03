from metaflow import FlowSpec, card, current, step, Config
from metaflow.cards import ProgressBar
from pydantic import BaseModel
from pathlib import Path
from helpers.validate_config import make_pydantic_parser_fn
from helpers.data_utils import fast_md5_hash


THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Config for the flow--provides validation and autocompletion."""

    glue_database_name: str
    glue_database_s3_bucket_name: str


class ExtractLoadWeatherDataFlow(FlowSpec):
    """Flow to extract NOAA weather data and load it into an Iceberg table."""

    config: FlowConfig = Config(
        name="config",
        default=str(THIS_DIR / "configs/default.yaml"),
        parser=make_pydantic_parser_fn(FlowConfig),
    )  # type: ignore[assignment]

    @step
    def start(self):
        """Start the flow by downloading the last 3 months of weather data."""
        self.next(self.download_data)

    @card(type="blank")
    @step
    def download_data(self):
        """Download weather data from NOAA; show progress bar."""
        from helpers.download_weather_data import (
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
        """Create a single Iceberg table in Athena for all weather measurement types."""
        import awswrangler as wr
        import os

        os.environ["AWS_PROFILE"] = "sandbox"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        import boto3
        print(boto3.client("sts").get_caller_identity())

        # Define a single Iceberg table for all weather data
        create_table_stmt = f"""
        CREATE TABLE IF NOT EXISTS {self.config.glue_database_name}.raw_weather (
            unique_row_id string, -- calculated hash for deduplication
            filename string,
            region_type string, -- cty, ste, etc.
            region_code bigint, -- FIPS code or state code (changed from int to bigint)
            region_name string, -- County/State name
            year bigint, -- changed from int to bigint
            month bigint, -- changed from int to bigint
            meteorological_element string, -- PRCP, TAVG, TMIN, TMAX
            day_01 double, day_02 double, day_03 double, day_04 double, day_05 double,
            day_06 double, day_07 double, day_08 double, day_09 double, day_10 double,
            day_11 double, day_12 double, day_13 double, day_14 double, day_15 double,
            day_16 double, day_17 double, day_18 double, day_19 double, day_20 double,
            day_21 double, day_22 double, day_23 double, day_24 double, day_25 double,
            day_26 double, day_27 double, day_28 double, day_29 double, day_30 double,
            day_31 double,
            ingest_timestamp timestamp
        )
        PARTITIONED BY (year, month, meteorological_element)
        LOCATION 's3://{self.config.glue_database_s3_bucket_name}/raw_weather/'
        TBLPROPERTIES (
            'table_type' = 'ICEBERG',
            'format' = 'parquet',
            'write_compression' = 'snappy'
        )
        """

        try:
            # Drop the table first to ensure clean schema
            drop_table_stmt = f"DROP TABLE IF EXISTS {self.config.glue_database_name}.raw_weather"
            wr.athena.start_query_execution(
                sql=drop_table_stmt,
                database=self.config.glue_database_name,
                wait=True
            )
            print("Dropped existing table 'raw_weather' if it existed")

            result = wr.athena.start_query_execution(
                sql=create_table_stmt,
                database=self.config.glue_database_name,
                wait=True
            )

            print(f"Successfully created Iceberg table 'raw_weather' in database '{self.config.glue_database_name}'")
            print(f"Query execution ID: {result['QueryExecutionId']}")
            print(f"Table location: s3://{self.config.glue_database_s3_bucket_name}/raw_weather/")

        except Exception as e:
            print(f"Error creating table: {str(e)}")
            # Check if it's just because the table already exists
            if "already exists" in str(e).lower():
                print("Table raw_weather already exists, which is expected with 'IF NOT EXISTS' clause.")
            else:
                raise e

        self.next(self.upsert_weather_data)

    @step
    def upsert_weather_data(self):
        """Load and upsert weather data into the Iceberg tables using bulk operations."""
        import awswrangler as wr
        import pandas as pd
        import os
        import uuid
        from helpers.download_weather_data import DATA_DIR, MEASUREMENT_TYPES

        os.environ["AWS_PROFILE"] = "sandbox"
        os.environ["AWS_REGION"] = "us-east-1"
        os.environ["AWS_DEFAULT_REGION"] = "us-east-1"

        # Process each measurement type
        for measurement in MEASUREMENT_TYPES:
            measurement_dir = DATA_DIR / "weather" / measurement

            if not measurement_dir.exists():
                print(f"No {measurement} directory found. Skipping.")
                continue

            csv_files = list(measurement_dir.glob("*.csv"))

            if not csv_files:
                print(f"No {measurement} CSV files found to load.")
                continue

            print(f"Found {len(csv_files)} {measurement} CSV files to process")

            # Collect all dataframes for this measurement type
            all_dataframes = []

            for file_path in csv_files:
                print(f"Reading file: {file_path.name}")

                try:
                    # Read the CSV file - no header row expected based on the format description
                    df = pd.read_csv(file_path, header=None)

                    # Based on the format description, assign proper column names
                    # Columns: region_type, region_code, region_name, year, month, meteorological_element, day1-31
                    column_names = ['region_type', 'region_code', 'region_name', 'year', 'month', 'meteorological_element'] + \
                                  [f'day_{i:02d}' for i in range(1, 32)]

                    df.columns = column_names

                    # Add metadata columns
                    df['filename'] = file_path.name
                    df['ingest_timestamp'] = pd.Timestamp.now(tz='UTC').floor('S').tz_localize(None)

                    # Convert -999.99 values to NaN (these represent non-existent days)
                    # Ensure all day columns are consistently typed as float64
                    day_columns = [f'day_{i:02d}' for i in range(1, 32)]
                    for col in day_columns:
                        # Convert to numeric first, then replace sentinel values with None
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                        df[col] = df[col].replace(-999.99, None)
                        # Explicitly cast to float64 to ensure consistency
                        df[col] = df[col].astype('float64')

                    all_dataframes.append(df)
                    print(f"Successfully read {len(df)} records from {file_path.name}")

                except Exception as e:
                    print(f"Error processing {file_path.name}: {str(e)}")
                    raise e

            if not all_dataframes:
                print(f"No dataframes to process for {measurement}")
                continue

            # Concatenate all dataframes for this measurement type
            print(f"Concatenating all {measurement} dataframes...")
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            print(f"Combined {measurement} dataframe has {len(combined_df)} total records")

            # Create unique row ID for deduplication
            hashed_columns = ['region_type', 'region_code', 'year', 'month', 'meteorological_element']
            combined_df['unique_row_id'] = fast_md5_hash(combined_df, hashed_columns=hashed_columns)

            # Remove duplicates
            initial_count = len(combined_df)
            combined_df.drop_duplicates(subset=['unique_row_id'], inplace=True)
            final_count = len(combined_df)
            if initial_count != final_count:
                print(f"Removed {initial_count - final_count} duplicate records for {measurement}")

            temp_path = f"s3://{self.config.glue_database_s3_bucket_name}/temp/{uuid.uuid4()}/"

            print(f"Performing bulk upsert for {measurement} data to Iceberg table...")

            # Ensure all object columns are strings to avoid type issues
            for col in combined_df.select_dtypes(include=['object']).columns:
                if col not in ['filename', 'region_type', 'region_name', 'meteorological_element', 'unique_row_id']:
                    combined_df[col] = combined_df[col].astype(str)

            # Single bulk upsert operation for this measurement type
            wr.athena.to_iceberg(
                df=combined_df,
                database=self.config.glue_database_name,
                table="raw_weather",
                temp_path=temp_path,
                table_location=f"s3://{self.config.glue_database_s3_bucket_name}/{self.config.glue_database_name}/raw_weather/",
                partition_cols=["year", "month", "meteorological_element"],
                merge_cols=["unique_row_id"],
                merge_condition="update",
                mode="append",
                dtype={
                    'ingest_timestamp': 'timestamp'
                }
            )

            print(f"Deleting objects at temporary path {temp_path} ...")
            wr.s3.delete_objects(path=temp_path)
            print("Objects deleted")

            print(f"Successfully upserted {len(combined_df)} {measurement} records in a single bulk operation")

        print("Completed upserting all weather data")
        self.next(self.end)

    @step
    def end(self):
        """End the flow."""
        ...


if __name__ == "__main__":
    ExtractLoadWeatherDataFlow()
