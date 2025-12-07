# GET THE RECORDED TIER DIST

x <- data$tx |>
  filter(year(begin_use_date) == analysis_year, !is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car"), unit_count > 0) |>
  semi_join(members,join_by(ccid))

x |> 
  distinct(booking_code, loyalty_tier) |> 
  count(loyalty_tier, name = "num_bookings") |> 
  filter(loyalty_tier > 0) |>
  mutate(num_bookings = num_bookings/ex_frac) |>
  mutate(prct_bookings = num_bookings/sum(num_bookings))

# NOW COMPARE THAT TO OUR DATA

int_to_tier <- function(loyalty_tier){
  case_when(
    loyalty_tier == 1 ~ "Blue",
    loyalty_tier == 2 ~ "Silver",
    loyalty_tier == 3 ~ "Gold",
    loyalty_tier == 5 ~ "Platinum",
    TRUE ~ NA_character_
  )
}

compare_tier_to_actual <- unit_calculation_data |>
  mutate(
    trip_elements_for_tier = pmax(trip_elements_start_of_booking, trip_elements_prev_year),
    trip_elements_for_tier = if_else(is_first_two_months == "Y", pmax(trip_elements_prev_2_year,
                                                                      trip_elements_for_tier), trip_elements_for_tier),
    calculated_tier = case_when(
      trip_elements_for_tier < 5 ~ 1,
      trip_elements_for_tier < 15 ~ 2,
      trip_elements_for_tier < 30 ~ 3,
      trip_elements_for_tier < Inf ~ 5,
      TRUE ~ NA
    )
  ) |>
  inner_join(tx |> group_by(booking_code) |> summarize(booking_date_pst = min(booking_date_pst),
                                                       loyalty_tier = min(loyalty_tier)), join_by(booking_code)) |>
  mutate(across(c(calculated_tier, loyalty_tier), int_to_tier)) |>
  select(ccid, booking_date_pst, begin_use_date, trip_elements_start_of_booking, trip_elements_prev_year, trip_elements_prev_2_year, trip_elements, booking_code, trip_elements_for_tier, calculated_tier, loyalty_tier)

compare_tier_to_actual |> count(loyalty_tier, calculated_tier) |> 
  ggplot(aes(x = n, y = loyalty_tier, fill = calculated_tier)) + geom_col()

compare_tier_to_actual |> 
  mutate(begin_month = month(begin_use_date, label=TRUE)) |>
  count(loyalty_tier, calculated_tier,begin_month) |> 
  ggplot(aes(x = n, y = loyalty_tier, fill = calculated_tier)) + geom_col() +
  facet_wrap(~begin_month)


compare_tier_to_actual |> filter(ccid == 522604) |> View()
compare_tier_to_actual |> filter(loyalty_tier == "Silver", calculated_tier == "Blue",
                                 month(begin_use_date) == 12)

tx |> filter(ccid == 522604) |>
  arrange(begin_use_date) |>
  select(booking_code, begin_use_date, trip_elements, lob, loyalty_tier)

unit_calculation_data |>
  filter(ccid == 522604) |> View()

# 642989 -- CCID With loyalty=silver calculated=blue in december

tx |> 
  filter(ccid == 1154403) |> View()

data$tx |> 
  filter(ccid == 642989) |> View()
