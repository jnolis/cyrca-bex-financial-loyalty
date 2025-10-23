source("utils.R")

options(scipen=999)

output_folder <- "C:/Users/themeanvaluetheorem/Dropbox/BEX Jacqueline/survey-join-output"

survey_data <- read_survey_data()

run_id <- format(Sys.time(),"%Y%m%d---%H%M%S")

p12_tx <- survey_data$tx |>
  filter(year(begin_use_date) == analysis_year)

# FIELDS -------------------------------

lob_data <- p12_tx |>
  group_by(ccid, lob) |>
  summarize(num_bookings = n_distinct(booking_code),
            unit_count = sum(unit_count),
            gross_booking_amount_usd = sum(gbv),
            loyalty_amount_net_burned_usd = sum(loyalty_amount_net_burned_usd),
            loyalty_amount_net_earned_usd = sum(loyalty_amount_net_earned_usd),
            .groups="drop") |>
  pivot_wider(names_from = lob, names_prefix = "lob_", values_from = -c(ccid,lob), values_fill = 0)

sum_data <-
  p12_tx |>
  group_by(ccid) |>
  summarize(
    across(
      c(
        package_indicator,
        international_flag,
        vipa_ind,
        vipa_and_burn_flag,
        fourfive_star_flag,
        any_mod_flag,
        tiered_mod_flag,
        chain_flag,
        cheap_channels_flag,
        app_flag,
        coupons_flag,
        room_night_count,
        length_of_stay,
        vip_perk_ind,
        vip_room_upgrade_ind,
        business_flag,
        child_flag,
        totl_person_count,
        new_flag_brand,
        reacquired_flag_brand
      ),
      sum
    )
  )

# JOIN ----------------------------

final_join <- survey_data$cid_to_ccid |>
  inner_join(survey_data$ccids, join_by(ccid)) |>
  left_join(lob_data, join_by(ccid)) |>
  left_join(sum_data, join_by(ccid)) |>
  mutate(across(-c(ccid,cid, parallax_hashed_email_address),
                ~replace_na(.x,0))) |>
  select(-ccid) |>
  select(cid, parallax_hashed_email_address, everything())

fs::dir_create(glue("{output_folder}/{run_id}"))
write_csv(final_join, glue("{output_folder}/{run_id}/join-data.csv") )
