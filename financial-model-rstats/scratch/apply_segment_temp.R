library(tidymodels)
survey_data <- read_survey_data()

rf_train_y <- survey_data$answers |> select(ccid, segments_v1) |> mutate(segments_v1 = as_factor(segments_v1))
rf_train_x <- survey_data$tx |>
  filter(year(begin_use_date) == analysis_year) |>
  group_by(ccid, lob) |>
  summarize(gbv = sum(gbv),
            .groups = "drop") |>
  pivot_wider(names_from = lob, values_from = gbv, names_sort = TRUE, values_fill = 0, names_prefix = "lob_")

rf_train_data <- 
  inner_join(rf_train_x, rf_train_y, join_by(ccid)) |>
  filter(!is.na(segments_v1)) |>
  select(-ccid)


rf_mod <- rand_forest(mode = "classification", trees = 200) |>
  set_engine("randomForest")

rf_wf <- 
  workflow() %>%
  add_model(rf_mod) %>%
  add_formula(segments_v1 ~ lob_activity + lob_air + lob_car + lob_insurance + lob_lodging)

rf_fit <- rf_wf |>
  fit(data = rf_train_data)


sample_ccids <- data$ccids |> sample_n(10000) |> select(ccid)
score_rf_x <-
  data$tx |>
  lazy_dt() |>
  filter(year(begin_use_date) == analysis_year) |>
  semi_join(sample_ccids, join_by(ccid)) |>
  group_by(ccid, lob) |>
  summarize(gbv = sum(gbv),
            .groups = "drop") |>
  pivot_wider(names_from = lob, values_from = gbv, names_sort = TRUE, values_fill = 0, names_prefix = "lob_") |>
  as_tibble()

predict(rf_fit, score_rf_x) |>
  count(.pred_class)


