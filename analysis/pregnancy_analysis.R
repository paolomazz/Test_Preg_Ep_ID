library(tidyverse)
library(lubridate)
library(here)

# Load the dataset
dataset <- read_csv(here::here("dataset.csv"))

# Define event type mapping (adjust as needed)
outcome_types <- c("delivery", "stillbirth", "miscarriage", "abortion", "ectopic", "molar")
edc_types <- c("edc") # EDC event group
lmp_types <- c("lmp") # LMP event group
antenatal_types <- c("antenatal", "booking_visit", "pregnancy_test", "dating_scan")

# Pivot to long format for all *_date columns
event_long <- dataset %>%
  mutate(patient_id = row_number()) %>% # If patient_id not present, create one
  pivot_longer(
    cols = ends_with("_date"),
    names_to = "event_type",
    values_to = "event_date"
  ) %>%
  filter(!is.na(event_date)) %>%
  mutate(
    event_date = as.Date(event_date),
    event_group = str_replace(event_type, "_event_\\d+_date", "")
  ) %>%
  arrange(patient_id, event_date)

# Identify all outcome events
outcome_events <- event_long %>%
  filter(event_group %in% outcome_types) %>%
  group_by(patient_id) %>%
  mutate(
    outcome_id = row_number()
  ) %>%
  ungroup()

# For each outcome, cluster all events within ±90 days
CLUSTER_WINDOW <- 90

# Function to assign episode numbers based on outcome anchors
assign_episodes <- function(df) {
  if (nrow(df) == 0) return(df)
  outcomes <- df %>% filter(event_group %in% outcome_types)
  if (nrow(outcomes) == 0) {
    df$episode_num <- NA
    return(df)
  }
  df$episode_num <- NA
  for (i in seq_len(nrow(outcomes))) {
    anchor_date <- outcomes$event_date[i]
    in_window <- abs(as.numeric(difftime(df$event_date, anchor_date, units = "days"))) <= CLUSTER_WINDOW
    df$episode_num[in_window & is.na(df$episode_num)] <- i
  }
  return(df)
}

# Apply to each patient
event_long <- event_long %>%
  group_by(patient_id) %>%
  group_modify(~assign_episodes(.x)) %>%
  ungroup() %>%
  filter(!is.na(episode_num))

# For each episode, identify outcome type and anchor date
episode_summary <- event_long %>%
  filter(event_group %in% outcome_types) %>%
  group_by(patient_id, episode_num) %>%
  summarise(
    outcome_type = first(event_group),
    outcome_date = first(event_date),
    .groups = "drop"
  )

# For each episode, try to get gestational age from EDC or LMP codes
get_gestational_age_edc_lmp <- function(events, outcome_date) {
  # 1. Try EDC (estimated date of confinement)
  edc_events <- events %>% filter(event_group %in% edc_types, event_date <= outcome_date)
  if (nrow(edc_events) > 0) {
    # Use the EDC closest before the outcome
    edc_date <- edc_events$event_date[which.max(edc_events$event_date)]
    gest_age <- as.numeric(difftime(outcome_date, edc_date, units = "days"))
    # If EDC is after outcome, ignore
    if (gest_age >= 0 && gest_age <= 294) return(list(gest_age = gest_age, start_date = edc_date))
  }
  # 2. Try LMP (last menstrual period)
  lmp_events <- events %>% filter(event_group %in% lmp_types, event_date <= outcome_date)
  if (nrow(lmp_events) > 0) {
    lmp_date <- lmp_events$event_date[which.max(lmp_events$event_date)]
    gest_age <- as.numeric(difftime(outcome_date, lmp_date, units = "days"))
    if (gest_age >= 0 && gest_age <= 294) return(list(gest_age = gest_age, start_date = lmp_date))
  }
  return(NULL)
}

# Impute gestational age if not available
impute_gestational_age <- function(outcome_type) {
  impute_days <- c(
    delivery = 280,      # 40 weeks
    stillbirth = 210,    # 30 weeks
    miscarriage = 70,    # 10 weeks
    abortion = 56,       # 8 weeks
    ectopic = 49,        # 7 weeks
    molar = 70           # 10 weeks
  )
  return(impute_days[[outcome_type]] %||% 196)
}

# For each episode, determine gestational age and pregnancy start
episode_details <- episode_summary %>%
  left_join(event_long, by = c("patient_id", "episode_num")) %>%
  group_by(patient_id, episode_num, outcome_type, outcome_date) %>%
  summarise(
    edc_lmp_info = get_gestational_age_edc_lmp(cur_data(), outcome_date),
    gest_age_final = if (!is.null(edc_lmp_info)) edc_lmp_info$gest_age else impute_gestational_age(outcome_type),
    preg_start_date = if (!is.null(edc_lmp_info)) edc_lmp_info$start_date else outcome_date - days(impute_gestational_age(outcome_type)),
    .groups = "drop"
  )

# Merge episode details back to event_long for full event-level data with episode and gestational info
event_long_with_episodes <- event_long %>%
  left_join(episode_details, by = c("patient_id", "episode_num"))

# Save processed event-level data
write_csv(event_long_with_episodes, here::here("output", "event_level_with_episodes.csv"))

# Save episode-level summary
write_csv(episode_details, here::here("output", "episode_summary.csv"))

cat("Event-level and episode-level data saved to output/\n")