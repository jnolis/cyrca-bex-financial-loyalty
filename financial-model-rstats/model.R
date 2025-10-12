source("utils.R")

options(scipen=999)

config <- jsonlite::read_json("config.json")

is_testing <- FALSE

ex_frac <- config$extract_percent_of_total

output_folder <- "C:/Users/themeanvaluetheorem/Dropbox/BEX Jacqueline/model-output"

data <- read_data_from_cache()

run_id <- format(Sys.time(),"%Y%m%d---%H%M%S")

# Assume all customers are in a single segment
segments <- data$cust |>
  distinct(ccid) |>
  mutate(segment = "all")



# REMOVE THE SAMPLING AND JOIN WHEN READY TO USE FULL


if(is_testing){
  warning("Using a small sample of data")
  tx_raw <-
    data$tx |>
    distinct(ccid) |>
    sample_n(1000) |>
    inner_join(data$tx, join_by(ccid))
} else {
  tx_raw <- data$tx
}

  

  
tx <- 
  tx_raw |>
  inner_join(segments, join_by(ccid)) |>
  mutate(lob_ext = if_else(package_indicator > 0,glue("package_{lob}"),lob)) |>
  filter(unit_count > 0)

# the year we are going to analyze the transactions
analysis_year <- 2024

# how many units each customer earned in the previous year
prev_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year - 1) |>
  group_by(ccid) |>
  summarize(units_earned_prev_year = sum(unit_count)) |>
  as_tibble()

# how many units each customer earned in the current year
current_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid) |>
  summarize(units_earned_current_year = sum(unit_count)) |>
  as_tibble()

# what is the LOB of each member
booking_lob <- 
  tx |>
  lazy_dt() |>
  group_by(booking_code, lob_ext) |>
  summarize(gbv = sum(gbv),
            .groups = "drop_last") |>
  mutate(prct_lob = gbv/sum(gbv)) |>
  select(booking_code, lob_ext, prct_lob) |>
  pivot_wider(names_from = lob_ext, names_prefix = "lob_", values_from = prct_lob, values_fill = 0) |>
  as_tibble()


# how many units before we just group them all together
max_units_earned_group <- 60

# a table with a row for each booking with the units and gbv
unit_calculation_data <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year, unit_count > 0) |>
  group_by(ccid, booking_code, segment) |>
  summarize(unit_count = sum(unit_count),
            begin_use_date = min(begin_use_date),
            gbv = sum(gbv),
         .groups="drop") |>
  group_by(ccid) |>
  arrange(begin_use_date) |>
  mutate(units_earned_start_of_booking = lag(cumsum(unit_count), default = 0)) |>
  left_join(prev_year_units, join_by(ccid)) |>
  left_join(current_year_units, join_by(ccid)) |>
  mutate(across(starts_with("units_earned_"),~replace_na(.x,0))) |>
  mutate(across(starts_with("units_earned_"),~if_else(.x>max_units_earned_group,max_units_earned_group,.x))) |>
  as_tibble()


# LOB BIZ

aggregated_lob <- unit_calculation_data |>
  select(units_earned_current_year,units_earned_prev_year,segment, booking_code, gbv) |>
  inner_join(booking_lob, join_by(booking_code)) |>
  group_by(
    segment, units_earned_current_year, units_earned_prev_year
  ) |>
  summarize(across(starts_with("lob_"), ~sum(.x*gbv,na.rm=T)),.groups="drop")


# OUTPUT

aggregated_bookings_for_simulator <-
  unit_calculation_data |>
  group_by(units_earned_start_of_booking, units_earned_prev_year, segment) |>
  summarize(gbv = sum(gbv),
            num_bookings = n(),
            .groups = "drop") |>
  mutate(across(c(gbv, num_bookings), ~.x / ex_frac))


aggregated_members_for_simulator <-
  unit_calculation_data |>
  group_by(segment, units_earned_prev_year, units_earned_current_year) |>
  summarize(num_members = n_distinct(ccid),
            num_bookings = n(),
            total_gbv = sum(gbv),
            num_units = sum(unit_count),
            across(starts_with("lob_"),~sum(.x,na.rm=T)),
            .groups="drop") |>
  mutate(across(c(num_members, num_bookings, total_gbv, num_units), ~.x/ex_frac)) |>
  mutate(units_per_booking = num_units/num_bookings,
         bookings_per_member = num_bookings/num_members) |>
  left_join(aggregated_lob, join_by(segment, units_earned_current_year, units_earned_prev_year)) |>
  rowwise() |>
  mutate(across(starts_with("lob_"),~.x/sum(c_across(starts_with("lob_"))))) |>
  ungroup()

fs::dir_create(glue("{output_folder}/{run_id}"))
write_csv(aggregated_bookings_for_simulator,glue("{output_folder}/{run_id}/bookings.csv"))
write_csv(aggregated_members_for_simulator,glue("{output_folder}/{run_id}/members.csv"))
