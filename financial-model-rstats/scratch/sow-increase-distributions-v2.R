# this file is used to get a better understanding of how customers
# will shift tiers.
# what we do is look to get a number so that if the SOW increases by n%,
# how many customers increase by actually that amount or by more/less than
# that to get the average to be n%

source("utils.R")

data <- read_data_from_cache()

tx_raw <- data$tx

members <-
  data$cust |>
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

# bookings -------------
pv1_members <-
  units_compare |>
  filter(bookings_prev_year == 1,
         bookings_current_year > 1,
         bookings_current_year <= 20) 

pv1_members|>
  count(bookings_current_year) |>
  mutate(p=n/sum(n))|>
  ggplot(aes(x=bookings_current_year, y = p)) + geom_col()


pv1_members|>
  count(trip_elements_current_year) |>
  mutate(p=n/sum(n))|>
  ggplot(aes(x=trip_elements_current_year, y = p)) + geom_col() +
  coord_cartesian(xlim=c(0,30))

# trip elements -------------

mean_trip_elements <-
  pv1_members |>
  pull(trip_elements_current_year) |>
  mean()

num_buckets <- 3
pv1_members_grouped <- pv1_members |>
  arrange(trip_elements_current_year) |>
  mutate(percentile = ((1:n())-1)/n()) |>
  mutate(group = floor(percentile*num_buckets)/num_buckets)

pv1_members_agg <- 
  pv1_members_grouped|>
  group_by(group) |>
  summarize(
    group_mean_trip_elements = mean(trip_elements_current_year)
  ) |>
  mutate(group_multiplier = group_mean_trip_elements/mean_trip_elements)

# this should be the same as the mean trip elements directly
# pv1_members_grouped |>
#   inner_join(pv1_members_agg,join_by(group)) |>
#   with(mean(group_multiplier*mean_trip_elements))

pv1_members_agg
