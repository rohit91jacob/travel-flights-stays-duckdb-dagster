"""Registry of the upstream Kaggle datasets this pipeline ingests."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SourceDataset:
    key: str
    kaggle_ref: str
    files: tuple[str, ...]
    license: str
    attribution: str

    @property
    def owner(self) -> str:
        return self.kaggle_ref.split("/", 1)[0]

    @property
    def slug(self) -> str:
        return self.kaggle_ref.split("/", 1)[1]


FLIGHTS = SourceDataset(
    key="flights",
    kaggle_ref="shubhambathwal/flight-price-prediction",
    # Clean_Dataset.csv in the same archive is a derived file without journey dates; we
    # deliberately land only the two raw scrapes and do our own cleaning.
    files=("economy.csv", "business.csv"),
    license="CC0-1.0",
    attribution="Shubham Bathwal, 'Flight Price Prediction' (EaseMyTrip scrape), Kaggle, CC0 1.0.",
)

HOTEL_BOOKINGS = SourceDataset(
    key="hotel_bookings",
    kaggle_ref="jessemostipak/hotel-booking-demand",
    files=("hotel_bookings.csv",),
    license="CC-BY-4.0",
    attribution=(
        "Antonio, N., de Almeida, A. & Nunes, L. (2019). Hotel booking demand datasets. "
        "Data in Brief, 22, 41-49. https://doi.org/10.1016/j.dib.2018.11.126 (CC BY 4.0); "
        "Kaggle mirror by Jesse Mostipak."
    ),
)

HOTEL_LISTINGS = SourceDataset(
    key="hotel_listings",
    kaggle_ref="andrewgeorgeissac/hotel-price-data-of-cities-in-india-makemytrip",
    files=(
        "bangalore.csv",
        "chennai.csv",
        "delhi.csv",
        "hyderabad.csv",
        "kolkata.csv",
        "mumbai.csv",
    ),
    license="CDLA-Sharing-1.0",
    attribution=(
        "Andrew George Issac, 'Hotel Price Data of Cities in India (MakeMyTrip)', Kaggle, "
        "Community Data License Agreement - Sharing 1.0."
    ),
)

ALL_DATASETS: tuple[SourceDataset, ...] = (FLIGHTS, HOTEL_BOOKINGS, HOTEL_LISTINGS)
DATASETS_BY_KEY = {d.key: d for d in ALL_DATASETS}
