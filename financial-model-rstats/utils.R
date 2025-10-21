library(tidyverse)
library(glue)
library(patchwork)
library(gt)
library(haven)
library(dtplyr)

analysis_year <- 2024

survey_customer_path <- "C:/Data/survey_raw/with_cid_cyrca_bex_cyrca_sample_customers.csv"
survey_tx_path <- "C:/Data/survey_raw/"
read_cust_raw <- function(path = NULL){
  if(is.null(path)){
    path <- "C:/Data/raw/cyrca_bex_customer.csv"
  }
  read_csv(path, col_types = cols(
    parallax_hashed_email_address = col_character(),
    first_book_date_bex = col_date(format = ""),
    tier_today = col_character(),
    tier_12m_ago = col_character(),
    highest_ever_bex_or_ok_tier = col_character(),
    highest_ever_ok_tier = col_character(),
    loyalty_amount_net_earned_usd_lifetime_bex_and_ok = col_double(),
    loyalty_amount_net_burned_usd_lifetime_bex_and_ok = col_double(),
    loyalty_amount_net_earned_usd_lifetime_ok = col_double(),
    loyalty_amount_net_burned_usd_lifetime_ok = col_double(),
    n_burns_lifetime = col_double(),
    n_burns_lifetime_ok = col_double(),
    onekey_cash_balance_local_currency = col_character(),
    onekey_points_currency = col_character(),
    ok_join_or_migration_date = col_character(),
    first_ever_bex_ok_join_date = col_character()
  ), na = c("","NA","null"))
}

read_tx_raw <- function(path=null){
  if(is.null(path)){
    path <- "C:/Data/raw"
  }
  fs::dir_ls(path, regexp = "_tx.*\\.csv$") |>
    map(~read_csv(.x, col_types = cols(
      booking_date_pst = col_date(format = ""),
      boooking_code = col_character(),
      eg_brand_code = col_character(),
      posa_country_code = col_character(),
      parallax_hashed_email_address = col_character(),
      line_of_business = col_character(),
      package_indicator = col_double(),
      unit_count = col_double(),
      international_flag = col_double(),
      vipa_ind = col_double(),
      vipa_and_burn_flag = col_double(),
      fourfive_star_flag = col_double(),
      any_mod_flag = col_double(),
      tiered_mod_flag = col_double(),
      mod_saving = col_character(),
      chain_flag = col_double(),
      cheap_channels_flag = col_double(),
      app_flag = col_double(),
      coupons_flag = col_double(),
      gross_booking_amount_usd = col_double(),
      loyalty_amount_net_burned_usd = col_double(),
      loyalty_amount_net_earned_usd = col_double(),
      room_night_count = col_double(),
      loyalty_program = col_character(),
      loyalty_tier = col_double(),
      burn_ind = col_double(),
      length_of_stay = col_double(),
      vip_perk_ind = col_double(),
      vip_room_upgrade_ind = col_double(),
      begin_use_date = col_date(format = "")
    ), na = c("","NA","null"))) |>
    data.table::rbindlist() |>
    as_tibble()
}


read_from_cache <- function(path, fn, ...){
  if(fs::file_exists(path)){
    arrow::read_parquet(path)
  } else {
    df <- fn(...)
    arrow::write_parquet(df, path)
    df
  }
}

post_process_tx <- function(df){
  df |>
    rename(booking_code = boooking_code,
           lob = line_of_business,
           gbv = gross_booking_amount_usd) |>
    mutate(lob = str_to_lower(lob))
}


get_ccids <- function(data){
  bind_rows(
    data[["cust"]] |> distinct(parallax_hashed_email_address),
    data[["tx"]] |> distinct(parallax_hashed_email_address)
  ) |> distinct(parallax_hashed_email_address) |>
    mutate(ccid = 1:n())
}

fix_ccids_in_df <- function(df, ccids){
  df |> 
    inner_join(ccids, join_by(parallax_hashed_email_address)) |>
    select(-parallax_hashed_email_address) |>
    select(ccid, everything())
}

process_ccids <- function(data){
  ccids <- get_ccids(data)
  data <- data |> map(~fix_ccids_in_df(.x,ccids))
  data[["ccids"]] <- ccids
  data
}



read_data <- function(cust_path = NULL, tx_path = NULL){
  message("reading customers")
  cust <- read_cust_raw(cust_path)
  message("reading transactions")
  tx <- read_tx_raw(tx_path)
  message("processing transactions")
  tx <- tx |> post_process_tx()
  data <- 
    list(cust = cust,
         tx = tx
    )
  
  message("processing customer ids")
  data <- process_ccids(data)
  
  message("done reading data")
  data
}

write_data_to_cache <- function(data){
  fs::dir_create("C:/data/raw_cache")
  iwalk(data, function(.x,.y){
    arrow::write_parquet(.x,glue("C:/data/raw_cache/{.y}.parquet"))
  })
}

read_data_from_cache <- function(data){
  file_names <- 
    fs::dir_ls("C:/data/raw_cache") |>
    str_remove("\\.parquet$") |>
    str_remove(".*/")
  if(length(file_names) == 0){
    stop("raw_cache is empty. Try running `read_data() |> write_data_to_cache()`")
  }
  file_names |> 
  map(~arrow::read_parquet(glue("C:/data/raw_cache/{.x}.parquet"))) |>
    set_names(file_names)
}


read_survey_map <- function(){
  customer_map <- read_csv(survey_customer_path,col_types = cols_only(
    parallax_hashed_email_address = col_character(),
    cid = col_character()),
    na = "null")
  
  customer_map
}

read_survey_append_data <- function(){
  read_data(survey_customer_path, survey_tx_path)
}

read_survey_data <- function(){
  survey_data_raw <- haven::read_sav("C:/Data/survey_raw/BEX 10 16 Alchemer + Program Calibration CT.sav")
  survey_append_data <- read_survey_append_data()
  survey_map <- read_survey_map()
  
  cid_to_ccid <- 
    survey_append_data$ccids |> 
    inner_join(survey_map, join_by(parallax_hashed_email_address)) |> 
    select(ccid, cid)
  
  questions_raw <-
    survey_data_raw |> 
    map(~attr(.x,"label"))
  
  questions <- 
    tibble(qid = names(questions_raw), full_question = as_vector(questions_raw)) |>
    mutate(qid = stringr::str_to_lower(qid))
  
  answers <- 
    survey_data_raw |> 
    as_tibble() |>
    janitor::clean_names() |> 
    left_join(cid_to_ccid, join_by(cid_external == cid))
  
  survey_append_data$questions <- questions
  survey_append_data$answers <- answers
  
  survey_append_data
}

apply_typing_transformation <- function(tx){
  tx |>
    filter(year(begin_use_date) == analysis_year, lob %in% c("air","lodging"), gbv > 0) |>
    group_by(ccid, lob) |>
    summarize(gbv = sum(gbv),
              .groups = "drop") |>
    mutate(gbv_cut = if_else(gbv > 10000, 10000, floor(gbv/1000)*1000)) |>
    select(-gbv) |>
    pivot_wider(names_from = lob, values_from = gbv_cut, names_sort = TRUE, values_fill = 0, names_prefix = "lob_") |>
    rename_with(~glue("{.x}_gbv_cut"), starts_with("lob_"))
}

get_typing_tool <- function(survey_data){
  survey_segments <- survey_data$answers |> 
    select(ccid, segments_v1) |> 
    filter(!is.na(ccid)) |>
    mutate(segments_v1 = as_factor(segments_v1) |> fct_recode(`High Hotel / High Flight` = "High Hotel / Hight Flight"))
  
  segment_raw_data <- 
    survey_data$tx |>
    apply_typing_transformation() |>
    inner_join(survey_segments, join_by(ccid))
  
  segment_raw_data |>
    filter(segments_v1 != "All Others")|> 
    count(lob_air_gbv_cut, lob_lodging_gbv_cut, segments_v1) |>
    group_by(lob_air_gbv_cut, lob_lodging_gbv_cut) |>
    mutate(segment_frac = n/sum(n)) |>
    ungroup() |>
    select(lob_air_gbv_cut, lob_lodging_gbv_cut, segment_nm = segments_v1, segment_frac)
}

get_segments <- function(tx_raw, typing_tool){
  adj_tx <- tx_raw |> 
    apply_typing_transformation() |>
    left_join(typing_tool, 
              join_by(lob_air_gbv_cut, lob_lodging_gbv_cut), 
              relationship = "many-to-many") |>
    select(ccid, segment_nm, segment_frac)
  
  if(any(is.na(adj_tx$segment_nm))){
    stop("Some customers not placed in segments")
  }
  adj_tx |>
    group_by(ccid) |>
    slice_sample(n=1,weight_by= segment_frac) |>
    ungroup() |>
    select(ccid, segment = segment_nm)
}
