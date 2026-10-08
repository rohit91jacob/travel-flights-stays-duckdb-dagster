"""Daily partitions that replay each static history as a day-by-day feed.

* Flights are keyed on **departure date**. Every quote in the scrape was observed on the
  same day (2022-02-10; ``days_left`` = departure date - 2022-02-10 for every row), so the
  only time axis the data has is the departure date.
* Hotel bookings are keyed on **event date**: the day a booking was created
  (arrival date - lead time) and the day its status became final
  (``reservation_status_date``). A partition holds exactly what was known on that day.

The bounds cover the full span of the current dataset versions. The
``*_within_partition_range`` checks fail if a new upstream version ever contains dates
outside them, so rows can never be dropped silently.
"""

from dagster import DailyPartitionsDefinition

FLIGHT_PARTITIONS = DailyPartitionsDefinition(start_date="2022-02-11", end_date="2022-04-01")
STAY_EVENT_PARTITIONS = DailyPartitionsDefinition(start_date="2013-06-24", end_date="2017-09-15")
