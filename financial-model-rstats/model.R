source("utils.R")

options(scipen=999)
data <- read_data_from_cache()


# REMOVE THE SAMPLING AND JOIN WHEN READY TO USE FULL
tx <- 
  data$tx |>
  distinct(ccid) |>
  sample_n(10000) |>
  inner_join(data$tx, join_by(ccid))

analysis_year <- 2024

prev_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year - 1) |>
  group_by(ccid) |>
  summarize(units_earned_prev_year = sum(unit_count)) |>
  as_tibble()

max_units_earned_group <- 60
unit_calculation_data <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid, booking_code) |>
  summarize(unit_count = sum(unit_count),
            begin_use_date = min(begin_use_date),
            gbv = sum(gbv),
         .groups="drop_last") |>
  arrange(begin_use_date) |>
  mutate(units_earned_start_of_booking = lag(cumsum(unit_count), default = 0)) |>
  left_join(prev_year_units, join_by(ccid)) |>
  mutate(across(starts_with("units_earned_"),~replace_na(.x,0))) |>
  mutate(across(starts_with("units_earned_"),~if_else(.x>max_units_earned_group,max_units_earned_group,.x))) |>
  as_tibble()



unit_calculation <-
  unit_calculation_data |>
  group_by(units_earned_start_of_booking, units_earned_prev_year) |>
  summarize(gbv = sum(gbv),
            .groups = "drop")

customer_units_earned_totals <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid) |>
  summarize(units_earned_current_year = sum(unit_count),
            gbv_current_year = sum(gbv)) |>
  left_join(prev_year_units, join_by(ccid)) |>
  mutate(across(starts_with("units_earned_"),~replace_na(.x,0))) |>
  mutate(across(starts_with("units_earned_"),~if_else(.x>max_units_earned_group,max_units_earned_group,.x))) |>
  as_tibble()

customer_lob_dist <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid,lob) |>
  summarize(gbv = sum(gbv),.groups="drop") |>
  as_tibble()
