"""
The data downloaded by this file comes from here:

https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/
"""

import datetime
from pathlib import Path
from typing import Iterator, Literal

import httpx
from dateutil.relativedelta import relativedelta

THIS_DIR = Path(__file__).parent
DATA_DIR = THIS_DIR / "../data"

TMeasurementTypes = Literal["prcp", "tavg", "tmin", "tmax"]
MEASUREMENT_TYPES: list[TMeasurementTypes] = ["prcp", "tavg", "tmin", "tmax"]

MEASUREMENT_NAMES = {
    "prcp": "precipitation",
    "tavg": "average_temperature", 
    "tmin": "min_temperature",
    "tmax": "max_temperature"
}


def make_download_weather_url(year: int, month: int, measurement: TMeasurementTypes) -> str:
    """
    Create download URL for weather data.
    
    Format: https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/yyyy/{measurement}-yyyymm-cty-scaled.csv
    """
    return f"https://www.ncei.noaa.gov/pub/data/daily-grids/v1-0-0/averages/{year}/{measurement}-{year}{month:02d}-cty-scaled.csv"


def make_outfile_fpath(
    measurement: TMeasurementTypes,
    year: int,
    month: int,
    extension: str,
    base_dir: Path = DATA_DIR,
) -> Path:
    return base_dir / "weather" / measurement / f"{measurement}-{year}-{month:02d}.{extension}"


def download_last_n_months_of_data_if_not_already_downloaded(
    n_months: int = 3,
) -> Iterator[None]:
    # Weather data is typically available with some delay, let's try 2 months back
    weather_upload_delay_months = 2

    today = datetime.date.today()
    most_recently_published_month = today - relativedelta(months=weather_upload_delay_months)

    for i in range(n_months):
        target_date = most_recently_published_month - relativedelta(months=i)
        year = target_date.year
        month = target_date.month
        for measurement in MEASUREMENT_TYPES:
            download_month_if_not_already_downloaded(year, month, measurement)
        yield


def download_month_if_not_already_downloaded(
    year: int,
    month: int,
    measurement: TMeasurementTypes,
):
    """Download weather data for a specific month and measurement type if not already downloaded."""
    outfile_fpath: Path = make_outfile_fpath(measurement, year, month, "csv")
    if outfile_fpath.exists():
        return

    url = make_download_weather_url(year, month, measurement)
    outfile_fpath.parent.mkdir(parents=True, exist_ok=True)
    response = httpx.get(url)
    response.raise_for_status()
    outfile_fpath.write_bytes(response.content)


def download_specific_month(
    year: int,
    month: int,
    measurements: list[TMeasurementTypes] | None = None,
):
    """Download weather data for specific measurements and month."""
    if measurements is None:
        measurements = MEASUREMENT_TYPES
    
    for measurement in measurements:
        download_month_if_not_already_downloaded(year, month, measurement)


if __name__ == "__main__":
    for _ in download_last_n_months_of_data_if_not_already_downloaded(n_months=5):
        print(_)
