unit_calculation_data |> 
  mutate(tier = case_when(trip_elements_start_of_booking >= 30 | trip_elements_prev_year >= 30~ "platinum", 
                          trip_elements_start_of_booking >= 15 | trip_elements_prev_year >= 15 ~ "gold", 
                          trip_elements_start_of_booking >= 5 | trip_elements_prev_year >= 5 ~ "silver", 
                          TRUE ~ "blue") |> factor(levels = c("blue","silver","gold","platinum"))) |> 
  group_by(tier) |> 
  summarize(gbvlc = sum(gbvlc)/ex_frac,
            bookings = sum(bookings)/ex_frac)

tx |>
  filter(year(begin_use_date) == 2024) |>
  group_by(loyalty_tier) |>
  summarize(gbvlc = sum(gbvlc)/ex_frac)


# -------

tx |> 
  filter(year(begin_use_date) == 2025) |> 
  count(loyalty_tier)

tx |>
  filter(begin_use_date < "2025-08-01") |>
  mutate(begin_use_date = floor_date(begin_use_date, unit="month")) |>
  count(begin_use_date, loyalty_tier) |>
  group_by(begin_use_date) |>
  mutate(p = n/sum(n)) |>
  ggplot(aes(x = begin_use_date, y = p, color = factor(loyalty_tier))) +
  geom_line()

unit_calculation_data |>
  count(begin_use_date) |>
  ggplot(aes(x = begin_use_date, y=n)) +geom_line()


unit_calculation_data |> 
  mutate(begin_use_date = floor_date(begin_use_date, unit="month")) |>
  mutate(tier = case_when(trip_elements_start_of_booking >= 30 | trip_elements_prev_year >= 30~ "platinum", 
                          trip_elements_start_of_booking >= 15 | trip_elements_prev_year >= 15 ~ "gold", 
                          trip_elements_start_of_booking >= 5 | trip_elements_prev_year >= 5 ~ "silver", 
                          TRUE ~ "blue") |> factor(levels = c("blue","silver","gold","platinum"))) |> 
  count(tier,begin_use_date) |>
  ggplot(aes(x = begin_use_date, y = n, color = tier)) + geom_line()


unit_calculation_data |> 
  mutate(begin_use_date = floor_date(begin_use_date, unit="month")) |>
  mutate(tier = case_when(trip_elements_start_of_booking >= 30 | trip_elements_prev_year >= 30~ "platinum", 
                          trip_elements_start_of_booking >= 15 | trip_elements_prev_year >= 15 ~ "gold", 
                          trip_elements_start_of_booking >= 5 | trip_elements_prev_year >= 5 ~ "silver", 
                          TRUE ~ "blue") |> factor(levels = c("blue","silver","gold","platinum"))) |> 
  count(tier) 
