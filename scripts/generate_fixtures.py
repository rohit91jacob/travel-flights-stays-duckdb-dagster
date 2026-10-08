"""Generate the small synthetic fixtures used by CI and the test suite.

The files mimic the exact headers and value formats of the three Kaggle datasets
(including their quirks: comma prices, multi-line stop text, malformed durations, NA/NULL
markers, zero-night stays, a negative ADR), but every value is synthetic, so no Kaggle
data is redistributed. Output is deterministic for a given seed.

    uv run python scripts/generate_fixtures.py            # writes tests/fixtures/kaggle
"""

from __future__ import annotations

import argparse
import csv
import random
from datetime import date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUT = ROOT / "tests" / "fixtures" / "kaggle"

CITIES = ["Delhi", "Mumbai", "Bangalore", "Kolkata", "Hyderabad", "Chennai"]
AIRLINES = [
    ("Vistara", "UK"),
    ("Air India", "AI"),
    ("Indigo", "6E"),
    ("SpiceJet", "SG"),
    ("GO FIRST", "G8"),
]
FLIGHT_HEADER = [
    "date",
    "airline",
    "ch_code",
    "num_code",
    "dep_time",
    "from",
    "time_taken",
    "stop",
    "arr_time",
    "to",
    "price",
]
HOTEL_HEADER = [
    "hotel", "is_canceled", "lead_time", "arrival_date_year", "arrival_date_month",
    "arrival_date_week_number", "arrival_date_day_of_month", "stays_in_weekend_nights",
    "stays_in_week_nights", "adults", "children", "babies", "meal", "country", "market_segment",
    "distribution_channel", "is_repeated_guest", "previous_cancellations",
    "previous_bookings_not_canceled", "reserved_room_type", "assigned_room_type", "booking_changes",
    "deposit_type", "agent", "company", "days_in_waiting_list", "customer_type", "adr",
    "required_car_parking_spaces", "total_of_special_requests", "reservation_status",
    "reservation_status_date",
]  # fmt: skip
LISTING_HEADER = [
    "Hotel Name", "Rating", "Rating Description", "Reviews", "Star Rating", "Location",
    "Nearest Landmark", "Distance to Landmark", "Price", "Tax",
]  # fmt: skip


def _fmt_price(value: int) -> str:
    return f"{value:,}"


def _write(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle, lineterminator="\n")
        writer.writerow(header)
        writer.writerows(rows)


def flights(rng: random.Random, out: Path) -> None:
    departures = [date(2022, 2, 11) + timedelta(days=i) for i in range(3)]
    routes = [
        ("Delhi", "Mumbai"),
        ("Mumbai", "Delhi"),
        ("Bangalore", "Kolkata"),
        ("Chennai", "Hyderabad"),
    ]
    for cabin, base, file_name in (
        ("Economy", 4500, "economy.csv"),
        ("Business", 28000, "business.csv"),
    ):
        rows: list[list[str]] = []
        airlines = AIRLINES if cabin == "Economy" else AIRLINES[:2]
        for departure in departures:
            for origin, destination in routes:
                for airline, code in airlines:
                    for option in range(2):
                        stops = rng.choice([0, 1, 1, 2])
                        minutes = rng.randint(120, 200) if stops == 0 else rng.randint(300, 1500)
                        dep = datetime.combine(departure, datetime.min.time()) + timedelta(
                            minutes=rng.randrange(0, 24 * 60, 5)
                        )
                        arr = dep + timedelta(minutes=minutes)
                        stop_text = {
                            0: "non-stop ",
                            1: "1-stop\n\t\t\t\t\t\tVia IXU\n\t\t\t\t",
                            2: "2+-stop",
                        }[stops]
                        if stops == 1 and option == 0:
                            stop_text = "1-stop "
                        time_taken = f"{minutes // 60:02d}h {minutes % 60:02d}m"
                        price = int(base * rng.uniform(0.7, 1.8)) + (departure.day - 11) * 150
                        rows.append(
                            [
                                departure.strftime("%d-%m-%Y"),
                                airline,
                                code,
                                str(rng.randint(100, 9999)),
                                dep.strftime("%H:%M"),
                                origin,
                                time_taken,
                                stop_text,
                                arr.strftime("%H:%M"),
                                destination,
                                _fmt_price(price),
                            ]
                        )
        # The real scrape has a handful of malformed durations like '1.03h m' (4 in 300k rows).
        if cabin == "Economy":
            rows[5][6] = "1.03h m"
        _write(out / "flights" / file_name, FLIGHT_HEADER, rows)


def hotel_bookings(rng: random.Random, out: Path) -> None:
    rows: list[list[str]] = []
    for index in range(160):
        hotel = "Resort Hotel" if index % 3 == 0 else "City Hotel"
        arrival = date(2016, 3, 1) + timedelta(days=rng.randint(0, 20))
        lead = rng.choice([0, 1, 3, 7, 14, 30, 45, 60, 90])
        weekend, week = rng.randint(0, 2), rng.randint(0, 4)
        if index == 7:
            weekend, week = 0, 0  # zero-night stay
        nights = weekend + week
        booked = arrival - timedelta(days=lead)
        outcome = rng.choices(["Check-Out", "Canceled", "No-Show"], weights=[60, 37, 3])[0]
        if outcome == "Check-Out":
            status_date = arrival + timedelta(days=nights)
        elif outcome == "Canceled":
            status_date = booked + timedelta(days=rng.randint(0, max(lead - 1, 0)))
        else:
            status_date = arrival
        adults = 0 if index == 11 else rng.choice([1, 2, 2, 3])
        children = "NA" if index == 13 else str(rng.choice([0, 0, 1]))
        adr = round(rng.uniform(40, 220), 2)
        if index == 17:
            adr = -6.38
        rows.append(
            [
                hotel,
                "0" if outcome == "Check-Out" else "1",
                str(lead),
                str(arrival.year),
                arrival.strftime("%B"),
                str(arrival.isocalendar().week),
                str(arrival.day),
                str(weekend),
                str(week),
                str(adults),
                children,
                "0",
                rng.choice(["BB", "BB", "HB", "SC", "Undefined", "FB"]),
                "NULL" if index == 19 else rng.choice(["PRT", "GBR", "FRA", "ESP", "DEU"]),
                rng.choice(["Online TA", "Offline TA/TO", "Direct", "Groups", "Corporate"]),
                rng.choice(["TA/TO", "Direct", "Corporate"]),
                str(int(index % 25 == 0)),
                str(rng.choice([0, 0, 0, 1])),
                str(rng.choice([0, 0, 1, 2])),
                rng.choice(["A", "A", "D", "E"]),
                rng.choice(["A", "D", "E"]),
                str(rng.choice([0, 0, 1])),
                rng.choice(["No Deposit", "No Deposit", "Non Refund"]),
                "NULL" if index % 4 == 0 else str(rng.randint(1, 300)),
                "NULL" if index % 9 else str(rng.randint(1, 100)),
                "0",
                rng.choice(["Transient", "Transient-Party", "Contract", "Group"]),
                f"{adr:g}",
                str(rng.choice([0, 0, 1])),
                str(rng.choice([0, 1, 2])),
                outcome,
                status_date.isoformat(),
            ]
        )
    _write(out / "hotel_bookings" / "hotel_bookings.csv", HOTEL_HEADER, rows)


def hotel_listings(rng: random.Random, out: Path) -> None:
    for city in CITIES:
        rows: list[list[str]] = []
        for index in range(4):
            stars = "" if index == 3 else str(rng.choice([3, 4, 5]))
            rating = f"{rng.uniform(3.2, 4.8):.1f}"
            distance = (
                f"{rng.randint(200, 950)} m " if index % 2 else f"{rng.uniform(1, 30):.1f} km "
            )
            price = rng.randint(1500, 15000)
            tax = "" if index == 1 else str(int(price * 0.12))
            rows.append(
                [
                    f"Fixture {city} Hotel {index + 1}",
                    rating,
                    "Very Good",
                    str(rng.randint(10, 9000)),
                    stars,
                    f"In {city} Central ",
                    f"{city} Airport",
                    distance,
                    _fmt_price(price),
                    tax,
                ]
            )
        _write(out / "hotel_listings" / f"{city.lower()}.csv", LISTING_HEADER, rows)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT)
    parser.add_argument("--seed", type=int, default=20220210)
    args = parser.parse_args()
    rng = random.Random(args.seed)
    flights(rng, args.out)
    hotel_bookings(rng, args.out)
    hotel_listings(rng, args.out)
    print(f"fixtures written to {args.out}")


if __name__ == "__main__":
    main()
