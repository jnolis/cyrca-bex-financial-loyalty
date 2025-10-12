library(tidyverse)
library(glue)
library(patchwork)
library(gt)
library(haven)
library(dtplyr)

read_cust_raw <- function(){
  read_csv("C:/Data/raw/cyrca_bex_customer.csv", col_types = cols(
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
  ))
}

read_tx_raw <- function(){
  fs::dir_ls("C:/Data/raw", regexp = "_tx.*\\.csv$") |>
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
    ))) |>
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



read_data <- function(){
  message("reading customers")
  cust <- read_cust_raw()
  message("reading transactions")
  tx <- read_tx_raw()
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