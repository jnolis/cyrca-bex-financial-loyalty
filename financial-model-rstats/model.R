source("utils.R")

options(scipen=999)

config <- jsonlite::read_json("config.json")

is_testing <- TRUE

ex_frac <- config$extract_percent_of_total

output_folder <- "C:/Users/themeanvaluetheorem/Dropbox/BEX Jacqueline/model-output"

data <- read_data_from_cache()
survey_data <- read_survey_data()

run_id <- format(Sys.time(),"%Y%m%d---%H%M%S")

unit_type <- "trip-element"


print_debug_values <- function(debug_values){
  debug_values |>
    imap_dfr(~list(type = .y, value = .x)) |>
    mutate(metric = str_extract(type,"(gbvlc)|(bookings)|(trip_elements)"),
           df = str_remove(type,"((gbvlc)|(bookings)|(trip_elements))_")
           ) |>
    select(-type) |>
    pivot_wider(names_from = metric, values_from = value)
}

debug_values <- list()

debug_values$gbvlc_raw <-
  data$tx |>
  filter(year(begin_use_date) == analysis_year, !is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car"), unit_count > 0) |>
  with(sum(gbv)/ex_frac)

debug_values$bookings_raw <-
  data$tx |>
  filter(year(begin_use_date) == analysis_year, !is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car"), unit_count > 0) |>
  with(n_distinct(booking_code)/ex_frac)

debug_values$trip_elements_raw <-
  data$tx |>
  filter(year(begin_use_date) == analysis_year, !is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car"), unit_count > 0) |>
  with(sum(unit_count)/ex_frac) 

# REMOVE THE SAMPLING AND JOIN WHEN READY TO USE FULL


if(is_testing){
  warning("Using a small sample of data")
  tx_raw <-
    data$tx |>
    distinct(ccid) |>
    sample_n(10000) |>
    inner_join(data$tx, join_by(ccid))
} else {
  message("Using all transactions")
  tx_raw <- data$tx
}

# Assume all customers are in a single segment
typing_tool <- get_typing_tool(survey_data)
segments <- get_segments(tx_raw, typing_tool)

if(nrow(tx_raw |> anti_join(segments,join_by(ccid))) > 0){
  stop("Some customers missing segments")
}
  

tx_raw_packages <-
  tx_raw |>
  lazy_dt() |>
  filter(!is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car")) |>
  distinct(booking_code, lob) |>
  count(booking_code) |>
  mutate(is_package = n > 1) |>
  as_tibble()
  

tx <- 
  tx_raw |>
  lazy_dt() |>
  inner_join(segments, join_by(ccid)) |>
  left_join(tx_raw_packages, join_by(booking_code)) |>
  as_tibble() |>
  mutate(is_package = replace_na(is_package, FALSE)) |>
  filter(!is.na(lob), eg_brand_code == "BEX", lob %in% c("air","lodging","car")) |>
  mutate(lob_ext = glue("{lob}{if_else(is_package,'_package','')}") |> factor(levels = c("air","lodging","car", "air_package", "lodging_package", "car_package"))) |>
  # mutate(lob_ext = 
  #          if_else(package_indicator > 0,glue("package_{lob_ext}"),lob_ext) |>
  #          factor(levels = c("lodging", "air", "other", "package_lodging", "package_air", "package_other"))
  #          ) |>
  rename(gbvlc = gbv) |>
  filter(unit_count > 0) |>
  rename(trip_elements = unit_count)


debug_values$gbvlc_tx <-
  tx |>
  filter(year(begin_use_date) == analysis_year) |>
  with(sum(gbvlc)/ex_frac)

debug_values$bookings_tx <-
  tx |>
  filter(year(begin_use_date) == analysis_year) |>
  with(n_distinct(booking_code)/ex_frac)

debug_values$trip_elements_tx <-
  tx |>
  filter(year(begin_use_date) == analysis_year) |>
  with(sum(trip_elements)/ex_frac) 


# how many units each customer earned in the previous year
prev_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year - 1) |>
  group_by(ccid) |>
  summarize(trip_elements_prev_year = sum(trip_elements),
            bookings_prev_year = n_distinct(booking_code)) |>
  as_tibble()

# how many units each customer earned in the current year
current_year_units <-
  tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid) |>
  summarize(trip_elements_current_year = sum(trip_elements),
            bookings_current_year = n_distinct(booking_code)) |>
  as_tibble()

debug_values$bookings_current_year <- sum(current_year_units$bookings_current_year)/ex_frac
debug_values$trip_elements_current_year <- sum(current_year_units$trip_elements_current_year)/ex_frac

# for each booking get the LOB gbv and is_lob (to count the bookings)
bookings_lob <- 
  tx |>
  lazy_dt() |>
  group_by(booking_code, lob_ext) |>
  summarize(gbvlc = sum(gbvlc),
            bookings = 1,
            trip_elements = sum(trip_elements),
            .groups = "drop") |>
  mutate(temp_lob_ext = lob_ext) |>
  pivot_wider(names_from = temp_lob_ext, names_prefix = "lob_gbvlc_", values_from = gbvlc, values_fill = 0, names_sort = TRUE) |>
  mutate(temp_lob_ext = lob_ext) |>
  pivot_wider(names_from = temp_lob_ext, names_prefix = "lob_bookings_", values_from = bookings, values_fill = 0, names_sort = TRUE) |>
  mutate(temp_lob_ext = lob_ext) |>
  pivot_wider(names_from = temp_lob_ext, names_prefix = "lob_trip_elements_", values_from = trip_elements, values_fill = 0, names_sort = TRUE) |>
  select(-lob_ext) |>
  group_by(booking_code) |>
  summarize(across(starts_with("lob_"),sum)) |>
  as_tibble() |>
  select(booking_code, 
         lob_gbvlc_air, lob_gbvlc_lodging, lob_gbvlc_car,
         lob_gbvlc_air_package, lob_gbvlc_lodging_package, lob_gbvlc_car_package, 
         lob_bookings_air, lob_bookings_lodging, lob_bookings_car,
         lob_bookings_air_package, lob_bookings_lodging_package, lob_bookings_car_package,
         lob_trip_elements_air, lob_trip_elements_lodging, lob_trip_elements_car,
         lob_trip_elements_air_package, lob_trip_elements_lodging_package, lob_trip_elements_car_package
         )

# how many units before we just group them all together
max_units_earned_group <- 45

# a table with a row for each booking with the units and gbv
unit_calculation_data <-
  tx |>
  lazy_dt() |>
  # get just the current year and valid bookings
  filter(year(begin_use_date) == analysis_year) |>
  
  # get a row for each booking
  group_by(ccid, booking_code, segment) |>
  summarize(trip_elements = sum(trip_elements),
            bookings = 1,
            begin_use_date = min(begin_use_date),
            is_package = n_distinct(lob_ext) > 1,
            gbvlc = sum(gbvlc),
         .groups="drop") |>
  
  # make the units increment across the year
  group_by(ccid) |>
  arrange(begin_use_date) |>
  mutate(trip_elements_start_of_booking = lag(cumsum(trip_elements), default = 0),
         bookings_start_of_booking = lag(cumsum(bookings), default = 0)
         ) |>
  ungroup() |>
  
  # join to the customer attributes
  left_join(prev_year_units, join_by(ccid)) |>
  left_join(current_year_units, join_by(ccid)) |>
  left_join(bookings_lob, join_by(booking_code)) |>
  
  as_tibble() |>
  # fill zeros and cap the upper bounds
  mutate(across(c(starts_with("trip_elements_"),starts_with("bookings")),~replace_na(.x,0))) |>
  mutate(across(c(starts_with("trip_elements_"),starts_with("bookings")),~if_else(.x>max_units_earned_group,max_units_earned_group,.x)))




debug_values$gbvlc_unit_calculation <- unit_calculation_data |> with(sum(gbvlc)/ex_frac)
debug_values$bookings_unit_calculation <- unit_calculation_data |> with(sum(bookings)/ex_frac)
debug_values$trip_elements_unit_calculation <- unit_calculation_data |> with(sum(trip_elements)/ex_frac)

# OUTPUT

unit_calculation_data_trip_elements <-
    unit_calculation_data |>
    mutate(
      units_earned_prev_year = trip_elements_prev_year,
      units_earned_current_year = trip_elements_current_year,
      units_earned_start_of_booking = trip_elements_start_of_booking,
      unit_type = "trip_elements"
    )
  
unit_calculation_data_bookings <-
    unit_calculation_data |>
    mutate(
      units_earned_prev_year = bookings_prev_year,
      units_earned_current_year = bookings_current_year,
      units_earned_start_of_booking = bookings_start_of_booking,
      unit_type = "bookings"
    )

unit_calculation_dfs = list(
  trip_elements = unit_calculation_data_trip_elements,
  bookings = unit_calculation_data_bookings
) 


compute_aggregated_bookings_for_simulator <- function(.x){
  .x |>
    group_by(segment, units_earned_prev_year, units_earned_start_of_booking) |>
    summarize(
      unit_type = unit_type[1],
      num_members = NA_real_,
      gbvlc = sum(gbvlc) / ex_frac,
      bookings = sum(bookings) / ex_frac,
      trip_elements = sum(trip_elements) / ex_frac,
      packages = sum(is_package) / ex_frac,
      across(starts_with("lob_"),~sum(.x)/ ex_frac),
      .groups = "drop") |>
    mutate(across(where(is.numeric),round))
}

aggregated_bookings_for_simulator <- map_dfr(unit_calculation_dfs, compute_aggregated_bookings_for_simulator)

debug_values$gbvlc_book_for_sim <- aggregated_bookings_for_simulator |> with(sum(gbvlc)/length(unit_calculation_dfs))
debug_values$bookings_book_for_sim <- aggregated_bookings_for_simulator |> with(sum(bookings)/length(unit_calculation_dfs))
debug_values$trip_elements_book_for_sim <- aggregated_bookings_for_simulator |> with(sum(trip_elements)/length(unit_calculation_dfs))

compute_aggregated_members_for_simulator <- function(.x){
  .x |>
    group_by(segment, units_earned_prev_year, units_earned_current_year) |>
    summarize(
      unit_type = unit_type[1],
      num_members = n_distinct(ccid) / ex_frac,
      gbvlc = sum(gbvlc) / ex_frac,
      bookings = sum(bookings) / ex_frac,
      trip_elements = sum(trip_elements) / ex_frac,
      packages = sum(is_package) / ex_frac,
      across(starts_with("lob_"),~sum(.x)/ ex_frac),
      .groups = "drop") |>
    mutate(across(where(is.numeric),round))
}

aggregated_members_for_simulator <- map_dfr(unit_calculation_dfs, compute_aggregated_members_for_simulator)

debug_values$gbvlc_mem_for_sim <- aggregated_members_for_simulator |> with(sum(gbvlc)/length(unit_calculation_dfs))
debug_values$bookings_mem_for_sim <- aggregated_members_for_simulator |> with(sum(bookings)/length(unit_calculation_dfs))
debug_values$trip_elements_mem_for_sim <- aggregated_members_for_simulator |> with(sum(trip_elements)/length(unit_calculation_dfs))

debug_values_output <- print_debug_values(debug_values)

print(debug_values_output)

fs::dir_create(glue("{output_folder}/{run_id}"))

rename_output <- function(.z) .z |> rename_with(~str_replace_all(.x,"_"," "))
aggregated_bookings_for_simulator |>
  rename_output() |>
  write_csv(glue("{output_folder}/{run_id}/bookings.csv"))

aggregated_members_for_simulator |>
  rename_output() |>
  write_csv(glue("{output_folder}/{run_id}/members.csv")  )
write_csv(debug_values_output,  glue("{output_folder}/{run_id}/debug.csv")  )
