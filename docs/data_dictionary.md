# Data dictionary

Generated from the dbt YAML by `scripts/generate_data_dictionary.py`; do not edit by hand.
Mart column types are enforced by dbt model contracts.

## Staging (views)

### `stg_flights__fare_quotes`

Parsed fare quotes. Grain: one itinerary option for one cabin class and departure date, as listed on the observation date. Exact duplicate rows exist upstream (the same option listed twice) and are kept; `fare_quote_id` is derived from the source file and row number.

| Column | Type | Description |
| --- | --- | --- |
| `fare_quote_id` |  |  |
| `departure_date` |  |  |
| `days_to_departure` |  | Days between the observation date and departure (the dataset's `days_left`). |
| `fare_inr` |  |  |
| `stops` |  |  |
| `departure_time` |  |  |
| `arrival_time` |  |  |
| `duration_minutes` |  | Null when the scrape published an unparseable duration (e.g. '1.03h m'). |
| `is_arrival_time_consistent` |  | Departure + duration lands on the published arrival time (HH:MM). |

### `stg_stays__booking_events`

Cleaned booking events. Grain: one event (`booking_created` or `status_changed`) per booking. Data-quality quirks of the source are flagged, not dropped: 180 bookings have no guests, 715 are zero-night stays, one has a negative ADR.

| Column | Type | Description |
| --- | --- | --- |
| `booking_event_id` |  |  |
| `booking_id` |  |  |
| `hotel_code` |  |  |
| `event_date` |  |  |
| `arrival_date` |  |  |
| `lead_time_days` |  |  |
| `meal_plan` |  |  |
| `adr_eur` |  | Average daily rate in EUR (sum of lodging transactions / nights). |

### `stg_stays__hotel_listings`

MakeMyTrip listings. Grain: one hotel in one city as listed on the capture date. `nightly_price_inr` is the displayed nightly price; `taxes_and_fees_inr` is only shown for some listings.

| Column | Type | Description |
| --- | --- | --- |
| `listing_id` |  |  |
| `city` |  |  |
| `nightly_price_inr` |  |  |
| `star_rating` |  |  |
| `rating` |  |  |

## Intermediate (tables)

### `int_stays__booking_status_history`

Type-2 status history per booking (valid_from inclusive, valid_to exclusive).

| Column | Type | Description |
| --- | --- | --- |
| `booking_status_version_id` |  |  |
| `status` |  |  |

### `int_stays__bookings`

One row per booking as of the latest replayed event date (`as_of_date`). `current_status` is 'Booked' until the booking's status event has been replayed; `is_canceled` is null while the outcome is unknown.

| Column | Type | Description |
| --- | --- | --- |
| `booking_id` |  |  |
| `current_status` |  |  |

### `int_stays__occupied_room_nights`

One row per occupied room-night of completed stays; revenue is the booking's ADR.

| Column | Type | Description |
| --- | --- | --- |
| `stay_night` |  |  |

## Marts: conformed dimensions

### `dim_city`

Conformed geography for flights and listings. Grain: one row per metro.

| Column | Type | Description |
| --- | --- | --- |
| `city_name` | varchar |  |
| `city_code` | varchar |  |
| `primary_airport_iata` | varchar |  |
| `state_name` | varchar |  |
| `country_code` | varchar |  |
| `currency_code` | varchar |  |

### `dim_currency`

Conformed currency dimension. Grain: one row per ISO 4217 code used by a fact.

| Column | Type | Description |
| --- | --- | --- |
| `currency_code` | varchar |  |
| `currency_name` | varchar |  |
| `minor_unit` | integer |  |

### `dim_date`

Conformed calendar shared by both subject areas. Grain: one row per day.

| Column | Type | Description |
| --- | --- | --- |
| `date_day` | date |  |
| `date_key` | integer | yyyymmdd surrogate used by the facts' *_date_key columns. |
| `year_number` | integer |  |
| `quarter_number` | integer |  |
| `month_number` | integer |  |
| `month_name` | varchar |  |
| `month_start` | date |  |
| `iso_year` | integer |  |
| `iso_week` | integer |  |
| `iso_day_of_week` | integer | 1 = Monday ... 7 = Sunday. |
| `day_name` | varchar |  |
| `is_weekend` | boolean |  |

## Marts: flights

### `dim_airline`

Airlines seen in the scrape. Grain: one row per IATA carrier code.

| Column | Type | Description |
| --- | --- | --- |
| `carrier_code` | varchar |  |
| `airline_name` | varchar |  |
| `fare_quotes` | bigint |  |
| `flight_codes` | bigint |  |
| `sells_business_class` | boolean |  |

### `dim_route`

Directed city-to-city routes. Grain: one row per origin -> destination.

| Column | Type | Description |
| --- | --- | --- |
| `origin_city` | varchar |  |
| `origin_city_code` | varchar |  |
| `destination_city` | varchar |  |
| `destination_city_code` | varchar |  |
| `route_code` | varchar | <origin IATA>-<destination IATA>, e.g. DEL-BOM. |
| `city_pair_code` | varchar | Direction-agnostic pair (alphabetical), e.g. BOM-DEL for both directions. |

### `fct_flight_fare_quotes`

Fare quotes. Grain: one itinerary option for one cabin class and departure date, as listed on `observed_on`. Incremental (delete+insert by departure date).

| Column | Type | Description |
| --- | --- | --- |
| `fare_quote_id` | varchar |  |
| `departure_date_key` | integer |  |
| `departure_date` | date |  |
| `observed_on` | date |  |
| `days_to_departure` | integer |  |
| `route_code` | varchar |  |
| `origin_city` | varchar |  |
| `destination_city` | varchar |  |
| `carrier_code` | varchar |  |
| `airline_name` | varchar |  |
| `flight_code` | varchar | Code of the first leg (carrier-number); connections are not itemised upstream. |
| `cabin_class` | varchar |  |
| `stops` | integer |  |
| `stops_label` | varchar |  |
| `via_point` | varchar | Stopover published for some 1-stop itineraries (airport code or city name). |
| `departure_time` | time |  |
| `arrival_time` | time |  |
| `departure_time_band` | varchar | late_night 00-04, early_morning 04-08, morning 08-12, afternoon 12-16, evening 16-20, night 20-24. |
| `duration_minutes` | integer |  |
| `fare_inr` | integer |  |
| `currency_code` | varchar |  |
| `is_duration_malformed` | boolean |  |
| `is_arrival_time_consistent` | boolean |  |
| `dataset_version` | varchar |  |
| `_loaded_at` | timestamp |  |

### `mart_flight_class_premium`

Business-class premium per route and airline (only airlines selling both classes). `business_premium_ratio` compares class medians; `median_like_for_like_premium_ratio` compares the same itinerary priced in both classes.

| Column | Type | Description |
| --- | --- | --- |
| `route_code` | varchar |  |
| `carrier_code` | varchar |  |
| `airline_name` | varchar |  |
| `economy_quotes` | bigint |  |
| `business_quotes` | bigint |  |
| `economy_median_fare_inr` | decimal(12,2) |  |
| `business_median_fare_inr` | decimal(12,2) |  |
| `business_premium_inr` | decimal(12,2) |  |
| `business_premium_ratio` | decimal(8,3) |  |
| `matched_itineraries` | bigint |  |
| `median_like_for_like_premium_ratio` | decimal(8,3) |  |

### `mart_flight_departure_time`

Fares by departure time band, indexed to the cabin class median.

| Column | Type | Description |
| --- | --- | --- |
| `cabin_class` | varchar |  |
| `departure_time_band` | varchar |  |
| `fare_quotes` | bigint |  |
| `median_fare_inr` | decimal(12,2) |  |
| `avg_fare_inr` | decimal(12,2) |  |
| `median_fare_index_vs_class` | decimal(8,3) |  |
| `band_order` | integer |  |

### `mart_flight_fare_curve`

Median fare by days to departure, per route and for all routes ('ALL'). Grain: route x cabin class x days to departure. Because every quote was observed on 2022-02-10, days to departure and departure date move together: read this as a cross-section of departures, not a single flight getting dearer over time.

| Column | Type | Description |
| --- | --- | --- |
| `route_code` | varchar |  |
| `cabin_class` | varchar |  |
| `days_to_departure` | integer |  |
| `departure_date` | date |  |
| `fare_quotes` | bigint |  |
| `median_fare_inr` | decimal(12,2) |  |
| `avg_fare_inr` | decimal(12,2) |  |
| `p25_fare_inr` | decimal(12,2) |  |
| `p75_fare_inr` | decimal(12,2) |  |
| `min_fare_inr` | integer |  |
| `median_fare_index_vs_furthest` | decimal(8,3) | Median fare divided by the median at the furthest-out observed departure date. |

### `mart_flight_route_airline_fares`

Fare distribution per route, airline and cabin class.

| Column | Type | Description |
| --- | --- | --- |
| `route_code` | varchar |  |
| `origin_city` | varchar |  |
| `destination_city` | varchar |  |
| `carrier_code` | varchar |  |
| `airline_name` | varchar |  |
| `cabin_class` | varchar |  |
| `fare_quotes` | bigint |  |
| `departure_dates` | bigint |  |
| `flight_codes` | bigint |  |
| `median_fare_inr` | decimal(12,2) |  |
| `avg_fare_inr` | decimal(12,2) |  |
| `p25_fare_inr` | decimal(12,2) |  |
| `p75_fare_inr` | decimal(12,2) |  |
| `min_fare_inr` | integer |  |
| `max_fare_inr` | integer |  |
| `median_duration_minutes` | decimal(8,1) |  |
| `nonstop_share` | decimal(6,4) |  |
| `median_fare_rank_on_route` | integer | 1 = cheapest airline (by median fare) on the route for that cabin class. |

### `mart_flight_stops_duration`

Fares by number of stops and total journey duration band.

| Column | Type | Description |
| --- | --- | --- |
| `cabin_class` | varchar |  |
| `stops` | integer |  |
| `stops_label` | varchar |  |
| `duration_band` | varchar |  |
| `fare_quotes` | bigint |  |
| `duration_band_order` | integer |  |
| `median_fare_inr` | decimal(12,2) |  |
| `avg_fare_inr` | decimal(12,2) |  |
| `median_duration_minutes` | decimal(8,1) |  |

## Marts: stays (hotel booking demand)

### `dim_hotel`

The two Portuguese properties of the booking demand dataset. Grain: one row per hotel.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `hotel_name` | varchar |  |
| `hotel_type` | varchar |  |
| `region_name` | varchar |  |
| `country_code` | varchar |  |
| `currency_code` | varchar |  |

### `dim_hotel_booking_status_history`

Type-2 status history per booking, derived from the event log (valid_from inclusive, valid_to exclusive, null while current). Grain: one row per booking status version.

| Column | Type | Description |
| --- | --- | --- |
| `booking_status_version_id` | varchar |  |
| `booking_id` | varchar |  |
| `hotel_code` | varchar |  |
| `status` | varchar |  |
| `valid_from` | date |  |
| `valid_to` | date |  |
| `is_current` | boolean |  |
| `version_number` | integer |  |

### `fct_hotel_bookings`

Bookings with their status as of `as_of_date` (the latest replayed event date). Grain: one row per booking. Money is in EUR. `realized_room_revenue_eur` counts completed stays only; `booked_room_revenue_eur` is ADR x nights regardless of outcome.

| Column | Type | Description |
| --- | --- | --- |
| `booking_id` | varchar | HB + 6-digit row number in the source file (the source has no booking id). |
| `hotel_code` | varchar |  |
| `booking_date` | date | Arrival date minus lead time. |
| `arrival_date` | date |  |
| `checkout_date` | date |  |
| `status_changed_on` | date | Reservation status date (cancellation, check-out or no-show date); null until replayed. |
| `current_status` | varchar |  |
| `is_outcome_known` | boolean |  |
| `is_canceled` | boolean | Canceled or No-Show (the source's is_canceled definition); null while unknown. |
| `lead_time_days` | integer |  |
| `nights` | integer |  |
| `weekend_nights` | integer |  |
| `week_nights` | integer |  |
| `adults` | integer |  |
| `children` | integer |  |
| `babies` | integer |  |
| `guests` | integer |  |
| `meal_plan` | varchar |  |
| `country_code` | varchar | ISO 3166-1 alpha-3 country of origin as recorded at check-in (null for 488 bookings). |
| `market_segment` | varchar |  |
| `distribution_channel` | varchar |  |
| `customer_type` | varchar |  |
| `deposit_type` | varchar |  |
| `reserved_room_type` | varchar |  |
| `assigned_room_type` | varchar |  |
| `is_room_reassigned` | boolean |  |
| `is_repeated_guest` | boolean |  |
| `previous_cancellations` | integer |  |
| `previous_bookings_not_canceled` | integer |  |
| `booking_changes` | integer |  |
| `days_in_waiting_list` | integer |  |
| `agent_id` | varchar |  |
| `company_id` | varchar |  |
| `required_car_parking_spaces` | integer |  |
| `total_of_special_requests` | integer |  |
| `currency_code` | varchar |  |
| `has_no_guests` | boolean |  |
| `is_zero_night_stay` | boolean |  |
| `has_negative_adr` | boolean |  |
| `as_of_date` | date |  |
| `booking_date_key` | integer |  |
| `arrival_date_key` | integer |  |
| `adr_eur` | decimal(10,2) |  |
| `booked_room_revenue_eur` | decimal(12,2) |  |
| `realized_room_revenue_eur` | decimal(12,2) |  |

### `mart_stays_adr_los`

ADR and length of stay of completed stays. Grain: hotel x reserved room type x LOS band. `adr_eur` is room-night weighted (revenue / room nights).

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `reserved_room_type` | varchar |  |
| `los_band` | varchar |  |
| `stays` | bigint |  |
| `room_nights` | bigint |  |
| `los_band_order` | integer |  |
| `adr_eur` | decimal(10,2) |  |
| `median_booking_adr_eur` | decimal(10,2) |  |
| `avg_length_of_stay` | decimal(8,2) |  |
| `median_length_of_stay` | decimal(8,1) |  |

### `mart_stays_booking_pace`

On-the-books curve per hotel and arrival month at fixed days before arrival. Grain: hotel x arrival month x days before arrival.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `arrival_month` | date |  |
| `final_arrivals` | bigint | Bookings for the month that ended in Check-Out. |
| `all_outcomes_known` | boolean |  |
| `days_before_arrival` | integer |  |
| `gross_bookings` | bigint | Bookings created at least this many days before their arrival. |
| `cancelled_by_then` | bigint |  |
| `net_bookings` | bigint |  |
| `share_of_final_arrivals` | decimal(8,4) |  |

### `mart_stays_cancellation_drivers`

Cancellation rate by candidate driver and value, per hotel and overall ('ALL'). Grain: hotel x driver x driver value. Bookings with a known outcome only.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `driver` | varchar |  |
| `driver_value` | varchar |  |
| `bookings` | bigint |  |
| `cancellations` | bigint |  |
| `cancellation_rate` | decimal(6,4) |  |
| `share_of_bookings` | decimal(6,4) |  |
| `lift_vs_hotel_rate` | decimal(8,3) |  |

### `mart_stays_revenue_proxies`

Monthly room-nights, revenue and ADR from completed stays, plus occupancy and RevPAR proxies. True occupancy/RevPAR need room inventory, which the dataset does not publish; the proxies assume capacity = the peak rooms ever occupied on one night (a lower bound), so they overstate true occupancy and RevPAR.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `stay_month` | date |  |
| `room_nights_sold` | bigint |  |
| `peak_occupied_rooms` | bigint | Most rooms occupied on any single night at the hotel across the whole dataset. |
| `max_occupied_rooms` | bigint | Most rooms occupied on any single night within the month. |
| `days_in_month` | integer |  |
| `is_complete_month` | boolean | False for months at the edges of the data window, where stays are only partly covered. |
| `room_revenue_eur` | decimal(14,2) |  |
| `adr_eur` | decimal(10,2) |  |
| `avg_occupied_rooms` | decimal(10,2) |  |
| `occupancy_proxy` | decimal(6,4) |  |
| `revpar_proxy_eur` | decimal(10,2) |  |

### `mart_stays_seasonality`

Bookings, cancellations, room nights and ADR by arrival month. Grain: hotel x arrival month.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `arrival_month` | date |  |
| `bookings` | bigint |  |
| `cancellations` | bigint |  |
| `completed_stays` | bigint |  |
| `arrival_year` | integer |  |
| `arrival_month_number` | integer |  |
| `cancellation_rate` | decimal(6,4) |  |
| `room_nights` | bigint |  |
| `realized_room_revenue_eur` | decimal(14,2) |  |
| `realized_adr_eur` | decimal(10,2) |  |
| `avg_lead_time_days` | decimal(8,1) |  |

### `mart_stays_segment_channel_mix`

Booking mix, cancellation rate and realized ADR by market segment and channel. Grain: hotel x arrival year x segment x channel.

| Column | Type | Description |
| --- | --- | --- |
| `hotel_code` | varchar |  |
| `arrival_year` | integer |  |
| `market_segment` | varchar |  |
| `distribution_channel` | varchar |  |
| `bookings` | bigint |  |
| `cancellations` | bigint |  |
| `share_of_hotel_year_bookings` | decimal(6,4) |  |
| `cancellation_rate` | decimal(6,4) |  |
| `avg_lead_time_days` | decimal(8,1) |  |
| `realized_room_revenue_eur` | decimal(14,2) |  |
| `realized_adr_eur` | decimal(10,2) |  |

## Marts: stays (India listings)

### `dim_hotel_listing`

Current version of each MakeMyTrip listing (open rows of `snap_hotel_listings`). Grain: one row per hotel per city.

| Column | Type | Description |
| --- | --- | --- |
| `listing_id` | varchar |  |
| `city` | varchar |  |
| `hotel_name` | varchar |  |
| `star_rating` | integer |  |
| `rating` | decimal(3,1) | Guest rating out of 5. |
| `rating_description` | varchar |  |
| `review_count` | integer |  |
| `locality` | varchar |  |
| `nearest_landmark` | varchar |  |
| `distance_to_landmark_km` | decimal(8,2) |  |
| `nightly_price_inr` | integer |  |
| `taxes_and_fees_inr` | integer |  |
| `total_nightly_price_inr` | integer | Nightly price plus taxes and fees where the listing shows them. |
| `has_tax_breakdown` | boolean |  |
| `captured_on` | date |  |
| `dataset_version` | varchar |  |
| `currency_code` | varchar |  |
| `valid_from` | timestamp |  |

### `mart_city_stay_prices`

Nightly price distribution of listings by city and star band ('all' = every listing). Grain: city x star band.

| Column | Type | Description |
| --- | --- | --- |
| `city` | varchar |  |
| `star_band` | varchar |  |
| `listings` | bigint |  |
| `captured_on` | date |  |
| `median_total_nightly_price_inr` | decimal(12,2) |  |
| `p25_total_nightly_price_inr` | decimal(12,2) |  |
| `p75_total_nightly_price_inr` | decimal(12,2) |  |
| `avg_rating` | decimal(4,2) |  |
| `median_review_count` | decimal(10,1) |  |

## Marts: cross-subject

### `mart_city_trip_cost_benchmark`

Indicative cost of flying into each metro and staying 3 nights at a mid-range (3-4 star) hotel. Grain: one row per city. Joins flights and listings on the conformed city dimension only; the inputs were captured ~18 months apart (see `fares_observed_on`, `listing_prices_captured_on`), so compare cities with each other rather than reading the totals as prices.

| Column | Type | Description |
| --- | --- | --- |
| `city` | varchar |  |
| `city_code` | varchar |  |
| `inbound_quotes` | bigint |  |
| `mid_range_listings` | bigint |  |
| `fares_observed_on` | date |  |
| `first_departure_date` | date |  |
| `last_departure_date` | date |  |
| `listing_prices_captured_on` | date |  |
| `nights` | integer |  |
| `median_inbound_economy_fare_inr` | decimal(12,2) |  |
| `median_mid_range_nightly_price_inr` | decimal(12,2) |  |
| `indicative_trip_cost_inr` | decimal(12,2) |  |
| `days_between_captures` | integer |  |
