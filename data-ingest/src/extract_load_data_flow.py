from metaflow import FlowSpec, card, current, pypi, pypi_base, step, Config
from metaflow.cards import ProgressBar
from pydantic import BaseModel
from pathlib import Path
from helpers.validate_config import make_pydantic_parser_fn


THIS_DIR = Path(__file__).parent


class FlowConfig(BaseModel):
    """Config for the flow--provides validation and autocompletion."""

    glue_database_name: str


@pypi_base(
    python="3.12",
    packages={
        "numpy": "1.26.3",
        "pandas": "2.2.3",
        "httpx": "0.28.1",
        "snowflake-connector-python": "3.13.1",
        "scikit-learn": "1.6.1",
    },
)
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


    @pypi(packages={"awswrangler": ""})
    @step
    def create_tables(self):
        
        create_yellow_table_stmt = """\
        CREATE TABLE IF NOT EXISTS raw_yellow (
            unique_row_id          text,
            filename               text,
            VendorID               text,
            tpep_pickup_datetime   timestamp,
            tpep_dropoff_datetime  timestamp,
            passenger_count        integer,
            trip_distance          double precision,
            RatecodeID             text,
            store_and_fwd_flag     text,
            PULocationID           text,
            DOLocationID           text,
            payment_type           integer,
            fare_amount            double precision,
            extra                  double precision,
            mta_tax                double precision,
            tip_amount             double precision,
            tolls_amount           double precision,
            improvement_surcharge  double precision,
            total_amount           double precision,
            congestion_surcharge   double precision
        );
        """
        

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
