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
  message("Using all transactions")
  tx_raw <- data$tx
}

  

  
tx <- 
  tx_raw |>
  inner_join(segments, join_by(ccid)) |>
  mutate(lob_ext = 
           if_else(package_indicator > 0,glue("package_{lob}"),lob) |>
           factor()
           ) |>
  rename(gbvlc = gbv) |>
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

# for each booking get the LOB gbv and is_lob (to count the bookings)
bookings_lob <- 
  tx |>
  lazy_dt() |>
  group_by(booking_code, lob_ext) |>
  summarize(gbvlc = sum(gbvlc),
            bookings = 1,
            .groups = "drop") |>
  mutate(temp_lob_ext = lob_ext) |>
  pivot_wider(names_from = temp_lob_ext, names_prefix = "lob_gbvlc_", values_from = gbvlc, values_fill = 0, names_sort = TRUE) |>
  mutate(temp_lob_ext = lob_ext) |>
  pivot_wider(names_from = temp_lob_ext, names_prefix = "lob_num_bookings_", values_from = bookings, values_fill = 0, names_sort = TRUE) |>
  select(-lob_ext) |>
  group_by(booking_code) |>
  summarize(across(starts_with("lob_"),sum)) |>
  as_tibble()


# how many units before we just group them all together
max_units_earned_group <- 60

# a table with a row for each booking with the units and gbv
unit_calculation_data <-
  tx |>
  lazy_dt() |>
  # get just the current year and valid bookings
  filter(year(begin_use_date) == analysis_year, unit_count > 0) |>
  
  # get a row for each booking
  group_by(ccid, booking_code, segment) |>
  summarize(unit_count = sum(unit_count),
            begin_use_date = min(begin_use_date),
            loyalty_redemption = sum(loyalty_amount_net_burned_usd),
            gbvlc = sum(gbvlc),
         .groups="drop") |>
  
  # make the units increment across the year
  group_by(ccid) |>
  arrange(begin_use_date) |>
  mutate(units_earned_start_of_booking = lag(cumsum(unit_count), default = 0)) |>
  
  # join to the customer attributes
  left_join(prev_year_units, join_by(ccid)) |>
  left_join(current_year_units, join_by(ccid)) |>
  
  # fill zeros and cap the upper bounds
  mutate(across(starts_with("units_earned_"),~replace_na(.x,0))) |>
  mutate(across(starts_with("units_earned_"),~if_else(.x>max_units_earned_group,max_units_earned_group,.x))) |>
  
  # get the LOB attributes
  left_join(bookings_lob, join_by(booking_code)) |>
  as_tibble()

# OUTPUT

aggregated_bookings_for_simulator <-
  unit_calculation_data |>
  group_by(units_earned_start_of_booking, units_earned_prev_year, segment) |>
  summarize(gbvlc = sum(gbvlc),
            num_bookings = n(),
            loyalty_redemption = sum(loyalty_redemption),
            across(starts_with("lob_"),sum),
            .groups = "drop") |>
  mutate(gbvlc_per_booking = gbvlc / num_bookings) |>
  mutate(across(starts_with("lob_gbvlc_"),~.x/gbvlc)) |>
  mutate(across(starts_with("lob_num_bookings_"),~.x/num_bookings)) |>
  mutate(across(c(gbvlc, num_bookings, loyalty_redemption), ~.x / ex_frac)) |>
  select(units_earned_start_of_booking,
         units_earned_prev_year,
         segment,
         gbvlc,
         num_bookings,
         loyalty_redemption,
         gbvlc_per_booking,
         starts_with("lob_"))
  
aggregated_members_for_simulator <-
  unit_calculation_data |>
  group_by(segment, units_earned_prev_year, units_earned_current_year) |>
  summarize(num_members = n_distinct(ccid),
            num_bookings = n(),
            gbvlc = sum(gbvlc),
            loyalty_redemption = sum(loyalty_redemption),
            num_units = sum(unit_count),
            across(starts_with("lob_"),sum),
            .groups="drop") |>
  mutate(units_per_booking = num_units/num_bookings,
         bookings_per_member = num_bookings/num_members,
         gbvlc_per_booking = gbvlc / num_bookings,
         gbvlc_per_member = gbvlc / num_members) |>
  mutate(across(starts_with("lob_gbvlc_"),~.x/gbvlc)) |>
  mutate(across(starts_with("lob_num_bookings_"),~.x/num_bookings)) |>
  mutate(across(c(num_members, num_bookings, gbvlc, num_units, loyalty_redemption), ~.x/ex_frac)) |>
    select(units_earned_prev_year,
           units_earned_current_year,
           segment,
           gbvlc,
           num_bookings,
           loyalty_redemption,
           num_units,
           num_members,
           gbvlc_per_booking,
           gbvlc_per_member,
           units_per_booking,
           bookings_per_member,
           starts_with("lob_"))


fs::dir_create(glue("{output_folder}/{run_id}"))
write_csv(aggregated_bookings_for_simulator, glue("{output_folder}/{run_id}/bookings.csv") )
write_csv(aggregated_members_for_simulator,  glue("{output_folder}/{run_id}/members.csv")  )
