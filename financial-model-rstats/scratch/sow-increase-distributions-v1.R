source("utils.R")


survey_data_raw <- haven::read_sav("C:/Data/survey_raw/BEX 10 16 Alchemer + Program Calibration CT.sav")

survey_data <- read_survey_data()

tx_raw <- survey_data$tx

members <-
  survey_data$cust |>
  select(ccid, join_date = first_ever_bex_ok_join_date) |>
  mutate(join_date = if_else(join_date == "null",NA_character_, join_date)) |>
  filter(!is.na(join_date) & year(join_date) <= analysis_year) |>
  select(ccid)

tx <- 
  tx_raw |>
  lazy_dt() |>
  semi_join(members,join_by(ccid)) |>
  as_tibble() |>
  filter(!is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging")) |>
  rename(gbvlc = gbv) |>
  filter(unit_count > 0) |>
  rename(trip_elements = unit_count)

current_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid) |>
  summarize(trip_elements_current_year = sum(trip_elements),
            bookings_current_year = n_distinct(booking_code)) |>
  as_tibble()

# how many units each customer earned in the previous year
prev_year_units <-
  tx |>
  lazy_dt() |>
  semi_join(current_year_units, join_by(ccid)) |>
  filter(year(begin_use_date) == analysis_year - 1) |>
  group_by(ccid) |>
  summarize(trip_elements_prev_year = sum(trip_elements),
            bookings_prev_year = n_distinct(booking_code)) |>
  as_tibble()

units_compare <-
  current_year_units |>
  left_join(prev_year_units, join_by(ccid))

units_compare |>
  count(bookings_current_year, bookings_prev_year) |>
  group_by(bookings_prev_year) |>
  mutate(p = n/sum(n)) |>
  filter(bookings_prev_year <= 20, bookings_prev_year > 0,
         bookings_current_year <= 20, bookings_current_year > 0) |>
  ggplot(aes(x = bookings_current_year, y = p)) + geom_col() +
  facet_wrap(~bookings_prev_year)
