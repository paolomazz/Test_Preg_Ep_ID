# Load required R packages
library(tidyverse)
library(lubridate)
library(ggplot2)
library(knitr)
library(kableExtra)
library(rmarkdown)
library(here)

# Set working directory to workspace root
setwd(here::here())

# Load the dataset
dataset <- read_csv(here::here("dataset.csv"))

# Print initial dataset summary
cat("\nInitial Dataset Summary:\n")
cat("Number of patients:", nrow(dataset), "\n")
cat("Number of date columns:", sum(grepl("_date$", names(dataset))), "\n")
cat("\nDate columns present:\n")
print(names(dataset)[grepl("_date$", names(dataset))])
cat("\nFirst few rows of data:\n")
print(head(dataset))

# --- Constants and Configuration ---
# Define validation windows and criteria
GESTATIONAL_AGE_WINDOWS <- list(
    term_birth = c(259, 294),      # 37-42 weeks
    preterm_birth = c(196, 258),   # 28-36 weeks
    very_preterm_birth = c(154, 195), # 22-27 weeks
    miscarriage = c(0, 196),       # 0-28 weeks
    abortion = c(0, 196),          # 0-28 weeks
    ectopic_pregnancy = c(0, 84),  # 0-12 weeks
    molar_pregnancy = c(0, 196)    # 0-28 weeks
)

# Event sequence weights
EVENT_SEQUENCE_WEIGHTS <- list(
    pregnancy_test = 0.3,
    booking_visit = 0.3,
    dating_scan = 0.2,
    antenatal_screening = 0.1,
    antenatal_risk = 0.1
)

# Outcome-specific criteria
OUTCOME_SPECIFIC_CRITERIA <- list(
    live_birth = list(
        required_events = c("booking_visit", "antenatal_screening"),
        gestational_age = c(154, 294),  # 22-42 weeks
        weight = 1.0
    ),
    stillbirth = list(
        required_events = c("booking_visit", "antenatal_screening"),
        gestational_age = c(154, 294),
        weight = 1.0
    ),
    ectopic_pregnancy = list(
        required_events = c("pregnancy_test"),
        gestational_age = c(0, 84),  # 0-12 weeks
        weight = 0.9
    ),
    molar_pregnancy = list(
        required_events = c("pregnancy_test"),
        gestational_age = c(0, 196),  # 0-28 weeks
        weight = 0.9
    ),
    miscarriage = list(
        required_events = c("pregnancy_test"),
        gestational_age = c(0, 196),
        weight = 0.8
    ),
    abortion = list(
        required_events = c("pregnancy_test"),
        gestational_age = c(0, 196),
        weight = 0.8
    )
)

# Confidence factors
CONFIDENCE_FACTORS <- list(
    event_sequence = list(
        pregnancy_test = 0.15,
        booking_visit = 0.20,
        dating_scan = 0.15,
        antenatal_screening = 0.10,
        antenatal_risk = 0.10,
        antenatal_procedures = 0.10
    ),
    clinical_indicators = list(
        gestational_diabetes = 0.10,
        preeclampsia = 0.10,
        pregnancy_hypertension = 0.10,
        hyperemesis = 0.05,
        pregnancy_infection = 0.05,
        pregnancy_bleeding = 0.05
    ),
    outcome_indicators = list(
        live_birth = 0.25,
        stillbirth = 0.20,
        miscarriage = 0.15,
        abortion = 0.15,
        ectopic_pregnancy = 0.10,
        molar_pregnancy = 0.10
    ),
    temporal_factors = list(
        gestational_age_plausibility = 0.20,
        event_sequence_plausibility = 0.15,
        outcome_timing_plausibility = 0.15
    ),
    data_quality = list(
        completeness = 0.20,
        consistency = 0.15,
        plausibility = 0.15
    )
)

# Data quality metrics
DATA_QUALITY_METRICS <- list(
    completeness = list(
        min_required_events = 2,
        min_required_outcomes = 1,
        min_required_dates = 2
    ),
    consistency = list(
        date_gap_range = c(7, 280),
        gestational_age_range = c(154, 294),
        max_concurrent_conditions = 5,
        max_concurrent_medications = 8
    ),
    plausibility = list(
        weight_gain_range = c(5, 20),
        blood_pressure_range = c(90, 160)
    ),
    temporal = list(
        max_booking_delay = 84,
        scan_interval_range = c(14, 84),
        visit_interval_range = c(7, 42)
    )
)

# Care model windows
CARE_MODEL_WINDOWS <- list(
    standard = list(
        antenatal_start = 84,    # 12 weeks
        antenatal_end = 280,     # 40 weeks
        postpartum = 84,         # 12 weeks
        min_episode_gap = 180    # 6 months
    ),
    high_risk = list(
        antenatal_start = 0,     # immediate
        antenatal_end = 280,     # 40 weeks
        postpartum = 180,        # 26 weeks
        min_episode_gap = 365    # 12 months
    ),
    community = list(
        antenatal_start = 84,    # 12 weeks
        antenatal_end = 280,     # 40 weeks
        postpartum = 42,         # 6 weeks
        min_episode_gap = 180    # 6 months
    )
)

# Minimum gap between episodes (in days)
MIN_EPISODE_GAP <- 90  # 3 months

# --- Helper Functions ---
# Function to safely convert to Date
safe_as_date <- function(x) {
    if (is.numeric(x)) {
        as.Date(x, origin = "1970-01-01")
    } else {
        as.Date(x)
    }
}

# Function to convert dates to gestational age
calculate_gestational_age <- function(start_date, end_date) {
    if (is.na(start_date) || is.na(end_date)) return(NA)
    # Ensure dates are in Date format
    start_date <- safe_as_date(start_date)
    end_date <- safe_as_date(end_date)
    as.numeric(difftime(end_date, start_date, units = "days"))
}

# Function to check if dates are within valid range
is_within_range <- function(date, start_date, end_date) {
    if (is.na(date) || is.na(start_date) || is.na(end_date)) return(FALSE)
    date <- safe_as_date(date)
    start_date <- safe_as_date(start_date)
    end_date <- safe_as_date(end_date)
    date >= start_date && date <= end_date
}

# Function to calculate event sequence confidence
calculate_event_sequence_confidence <- function(events, start_date, end_date) {
    # Initialize score
    score <- 0
    max_possible <- sum(unlist(EVENT_SEQUENCE_WEIGHTS))
    
    # Check each event type
    for (event_type in names(EVENT_SEQUENCE_WEIGHTS)) {
        if (event_type %in% events) {
            score <- score + EVENT_SEQUENCE_WEIGHTS[[event_type]]
        }
    }
    
    # Normalize score
    normalized_score <- min(score / max_possible, 1.0)
    return(normalized_score)
}

# Function to calculate clinical confidence
calculate_clinical_confidence <- function(data, start_date, end_date) {
    # Initialize score
    score <- 0
    max_possible <- sum(unlist(CONFIDENCE_FACTORS$clinical_indicators))
    
    # Get date columns
    date_cols <- names(data)[grepl("_date$", names(data))]
    
    # Check each clinical indicator
    for (indicator in names(CONFIDENCE_FACTORS$clinical_indicators)) {
        # Look for matching date columns
        matching_cols <- date_cols[grepl(indicator, date_cols, ignore.case = TRUE)]
        if (length(matching_cols) > 0) {
            for (col in matching_cols) {
                if (any(!is.na(data[[col]]))) {
                    score <- score + CONFIDENCE_FACTORS$clinical_indicators[[indicator]]
                    break
                }
            }
        }
    }
    
    # Normalize score
    normalized_score <- min(score / max_possible, 1.0)
    return(normalized_score)
}

# Function to calculate outcome confidence
calculate_outcome_confidence <- function(outcome_type, gestational_age) {
    if (!outcome_type %in% names(OUTCOME_SPECIFIC_CRITERIA)) {
        return(0)
    }
    
    criteria <- OUTCOME_SPECIFIC_CRITERIA[[outcome_type]]
    score <- criteria$weight
    
    # Check gestational age
    if (!is.na(gestational_age)) {
        if (gestational_age < criteria$gestational_age[1] || 
            gestational_age > criteria$gestational_age[2]) {
            score <- score * 0.5  # Penalty for gestational age outside range
        }
    }
    
    return(score)
}

# Function to calculate data quality confidence
calculate_data_quality_confidence <- function(data, start_date, end_date) {
    # Initialize scores
    completeness_score <- 0
    consistency_score <- 0
    plausibility_score <- 0
    
    # Get date columns
    date_cols <- names(data)[grepl("_date$", names(data))]
    
    # Check completeness
    non_na_dates <- sum(!is.na(unlist(data[date_cols])))
    if (non_na_dates >= DATA_QUALITY_METRICS$completeness$min_required_events) {
        completeness_score <- 1.0
    } else {
        completeness_score <- non_na_dates / DATA_QUALITY_METRICS$completeness$min_required_events
    }
    
    # Check consistency
    gestational_age <- calculate_gestational_age(start_date, end_date)
    if (!is.na(gestational_age)) {
        if (gestational_age >= DATA_QUALITY_METRICS$consistency$gestational_age_range[1] &&
            gestational_age <= DATA_QUALITY_METRICS$consistency$gestational_age_range[2]) {
            consistency_score <- 1.0
        } else {
            consistency_score <- 0.5
        }
    }
    
    # Calculate overall score
    weights <- CONFIDENCE_FACTORS$data_quality
    total_score <- (completeness_score * weights$completeness +
                   consistency_score * weights$consistency +
                   plausibility_score * weights$plausibility) /
                  sum(unlist(weights))
    
    return(total_score)
}

# Function to calculate overall confidence score
calculate_episode_confidence <- function(data, events, start_date, end_date, outcome_type) {
    # Calculate individual scores
    event_score <- calculate_event_sequence_confidence(events, start_date, end_date)
    clinical_score <- calculate_clinical_confidence(data, start_date, end_date)
    gestational_age <- calculate_gestational_age(start_date, end_date)
    outcome_score <- calculate_outcome_confidence(outcome_type, gestational_age)
    quality_score <- calculate_data_quality_confidence(data, start_date, end_date)
    
    # Combine scores with weights
    weights <- CONFIDENCE_FACTORS
    total_score <- (event_score * sum(unlist(weights$event_sequence)) +
                   clinical_score * sum(unlist(weights$clinical_indicators)) +
                   outcome_score * sum(unlist(weights$outcome_indicators)) +
                   quality_score * sum(unlist(weights$data_quality))) /
                  (sum(unlist(weights$event_sequence)) +
                   sum(unlist(weights$clinical_indicators)) +
                   sum(unlist(weights$outcome_indicators)) +
                   sum(unlist(weights$data_quality)))
    
    return(total_score)
}

# --- Validation Functions ---
# Function to validate episode
validate_episode <- function(data, start_date, end_date) {
    # Filter data to this episode's date range
    episode_data <- data %>%
        select(ends_with("_date")) %>%
        filter(across(everything(), ~is_within_range(., start_date, end_date)))
    
    list(
        temporal = validate_temporal(episode_data, start_date, end_date),
        clinical = validate_clinical(episode_data),
        outcome = validate_outcome(episode_data, start_date, end_date)
    )
}

# Function to validate temporal aspects
validate_temporal <- function(data, start_date, end_date) {
    issues <- list()
    
    # Check episode duration
    duration <- calculate_gestational_age(start_date, end_date)
    if (!is.na(duration) && duration > 294) {  # 42 weeks
        issues$duration <- "Episode duration exceeds maximum"
    }
    
    # Check for booking visit delay if both dates exist
    if ("pregnancy_test_date" %in% names(data) && "booking_visit_date" %in% names(data)) {
        test_dates <- data$pregnancy_test_date[!is.na(data$pregnancy_test_date)]
        booking_dates <- data$booking_visit_date[!is.na(data$booking_visit_date)]
        
        if (length(test_dates) > 0 && length(booking_dates) > 0) {
            delay <- calculate_gestational_age(min(test_dates), min(booking_dates))
            if (!is.na(delay) && delay > DATA_QUALITY_METRICS$temporal$max_booking_delay) {
                issues$booking_delay <- "Booking visit delay exceeds maximum"
            }
        }
    }
    
    return(issues)
}

# Function to validate clinical aspects
validate_clinical <- function(data) {
    issues <- list()
    
    # Count non-NA dates
    non_na_dates <- sum(!is.na(unlist(data)))
    if (non_na_dates > DATA_QUALITY_METRICS$consistency$max_concurrent_conditions) {
        issues$too_many_conditions <- "Too many concurrent conditions"
    }
    
    return(issues)
}

# Function to validate outcomes
validate_outcome <- function(data, start_date, end_date) {
    issues <- list()
    
    # Check gestational age
    duration <- calculate_gestational_age(start_date, end_date)
    if (!is.na(duration)) {
        if (duration < DATA_QUALITY_METRICS$consistency$gestational_age_range[1]) {
            issues$gestational_age <- "Gestational age too low"
        } else if (duration > DATA_QUALITY_METRICS$consistency$gestational_age_range[2]) {
            issues$gestational_age <- "Gestational age too high"
        }
    }
    
    return(issues)
}

# Function to generate detailed confidence score report
generate_confidence_report <- function(data, events, start_date, end_date, outcome_type) {
    # Calculate individual scores
    event_score <- calculate_event_sequence_confidence(events, start_date, end_date)
    clinical_score <- calculate_clinical_confidence(data, start_date, end_date)
    gestational_age <- calculate_gestational_age(start_date, end_date)
    outcome_score <- calculate_outcome_confidence(outcome_type, gestational_age)
    quality_score <- calculate_data_quality_confidence(data, start_date, end_date)
    
    # Create detailed report
    report <- list(
        # Event sequence details
        event_sequence = list(
            score = event_score,
            max_possible = 1.0,
            components = sapply(names(EVENT_SEQUENCE_WEIGHTS), function(event) {
                if (event %in% events) {
                    paste0("Present (", EVENT_SEQUENCE_WEIGHTS[[event]], ")")
                } else {
                    "Missing"
                }
            })
        ),
        
        # Clinical indicators details
        clinical_indicators = list(
            score = clinical_score,
            max_possible = 1.0,
            components = sapply(names(CONFIDENCE_FACTORS$clinical_indicators), function(indicator) {
                date_cols <- names(data)[grepl("_date$", names(data))]
                matching_cols <- date_cols[grepl(indicator, date_cols, ignore.case = TRUE)]
                if (length(matching_cols) > 0 && any(!is.na(data[[matching_cols[1]]]))) {
                    paste0("Present (", CONFIDENCE_FACTORS$clinical_indicators[[indicator]], ")")
                } else {
                    "Missing"
                }
            })
        ),
        
        # Outcome details
        outcome = list(
            score = outcome_score,
            max_possible = 1.0,
            gestational_age = gestational_age,
            gestational_age_status = if (!is.na(gestational_age)) {
                if (gestational_age >= OUTCOME_SPECIFIC_CRITERIA[[outcome_type]]$gestational_age[1] &&
                    gestational_age <= OUTCOME_SPECIFIC_CRITERIA[[outcome_type]]$gestational_age[2]) {
                    "Within normal range"
                } else {
                    "Outside normal range"
                }
            } else {
                "Unknown"
            }
        ),
        
        # Data quality details
        data_quality = list(
            score = quality_score,
            max_possible = 1.0,
            completeness = list(
                score = if (sum(!is.na(unlist(data[names(data)[grepl("_date$", names(data))]]))) >= 
                           DATA_QUALITY_METRICS$completeness$min_required_events) 1.0 else 0.5,
                required_events = DATA_QUALITY_METRICS$completeness$min_required_events,
                actual_events = sum(!is.na(unlist(data[names(data)[grepl("_date$", names(data))]])))
            ),
            consistency = list(
                score = if (!is.na(gestational_age) &&
                           gestational_age >= DATA_QUALITY_METRICS$consistency$gestational_age_range[1] &&
                           gestational_age <= DATA_QUALITY_METRICS$consistency$gestational_age_range[2]) 1.0 else 0.5,
                gestational_age = gestational_age,
                gestational_age_range = paste(DATA_QUALITY_METRICS$consistency$gestational_age_range, collapse = "-")
            )
        ),
        
        # Overall score
        overall = list(
            score = (event_score * sum(unlist(CONFIDENCE_FACTORS$event_sequence)) +
                    clinical_score * sum(unlist(CONFIDENCE_FACTORS$clinical_indicators)) +
                    outcome_score * sum(unlist(CONFIDENCE_FACTORS$outcome_indicators)) +
                    quality_score * sum(unlist(CONFIDENCE_FACTORS$data_quality))) /
                   (sum(unlist(CONFIDENCE_FACTORS$event_sequence)) +
                    sum(unlist(CONFIDENCE_FACTORS$clinical_indicators)) +
                    sum(unlist(CONFIDENCE_FACTORS$outcome_indicators)) +
                    sum(unlist(CONFIDENCE_FACTORS$data_quality))),
            max_possible = 1.0
        )
    )
    
    return(report)
}

# Function to convert confidence report to data frame
confidence_report_to_df <- function(report, patient_id, episode_num) {
    # Initialize empty vectors for the data frame
    components <- character()
    scores <- numeric()
    max_possibles <- numeric()
    details <- character()
    
    # Add event sequence details
    for (event in names(report$event_sequence$components)) {
        components <- c(components, paste("Event Sequence:", event))
        if (report$event_sequence$components[event] != "Missing") {
            scores <- c(scores, as.numeric(gsub(".*\\((.*)\\).*", "\\1", report$event_sequence$components[event])))
        } else {
            scores <- c(scores, 0)
        }
        max_possibles <- c(max_possibles, EVENT_SEQUENCE_WEIGHTS[[event]])
        details <- c(details, report$event_sequence$components[event])
    }
    
    # Add clinical indicator details
    for (indicator in names(report$clinical_indicators$components)) {
        components <- c(components, paste("Clinical Indicator:", indicator))
        if (report$clinical_indicators$components[indicator] != "Missing") {
            scores <- c(scores, as.numeric(gsub(".*\\((.*)\\).*", "\\1", report$clinical_indicators$components[indicator])))
        } else {
            scores <- c(scores, 0)
        }
        max_possibles <- c(max_possibles, CONFIDENCE_FACTORS$clinical_indicators[[indicator]])
        details <- c(details, report$clinical_indicators$components[indicator])
    }
    
    # Add outcome details
    components <- c(components, "Outcome")
    scores <- c(scores, report$outcome$score)
    max_possibles <- c(max_possibles, report$outcome$max_possible)
    details <- c(details, paste("Gestational age:", report$outcome$gestational_age, 
                              "Status:", report$outcome$gestational_age_status))
    
    # Add data quality details
    components <- c(components, "Data Quality")
    scores <- c(scores, report$data_quality$score)
    max_possibles <- c(max_possibles, report$data_quality$max_possible)
    details <- c(details, paste("Completeness:", report$data_quality$completeness$actual_events, "/",
                              report$data_quality$completeness$required_events, "events",
                              "Consistency:", report$data_quality$consistency$gestational_age_status))
    
    # Add overall score
    components <- c(components, "Overall Confidence")
    scores <- c(scores, report$overall$score)
    max_possibles <- c(max_possibles, report$overall$max_possible)
    details <- c(details, "Combined weighted score")
    
    # Create the data frame
    df <- data.frame(
        patient_id = rep(patient_id, length(components)),
        episode_num = rep(episode_num, length(components)),
        component = components,
        score = scores,
        max_possible = max_possibles,
        details = details,
        stringsAsFactors = FALSE
    )
    
    return(df)
}

# --- Main Analysis Function ---
run_analysis <- function(data) {
    # 1. Create event-level data
    events <- data %>%
        # Pivot all date columns to long format
        pivot_longer(
            cols = ends_with("_date"),
            names_to = "event_type",
            values_to = "event_date"
        ) %>%
        # Remove rows with missing dates
        filter(!is.na(event_date)) %>%
        # Convert dates to Date type
        mutate(event_date = safe_as_date(event_date)) %>%
        # Sort by patient and date
        arrange(patient_id, event_date)
    
    # 2. Assign episode numbers
    events_with_episodes <- events %>%
        group_by(patient_id) %>%
        mutate(
            # Calculate days since previous event
            days_since_prev = as.numeric(difftime(event_date, lag(event_date), units = "days")),
            # Start new episode if gap > MIN_EPISODE_GAP or first event
            new_episode = is.na(days_since_prev) | days_since_prev > MIN_EPISODE_GAP,
            # Create episode numbers
            episode_num = cumsum(new_episode)
        ) %>%
        ungroup()
    
    # 3. Create episode-level data
    episodes <- events_with_episodes %>%
        group_by(patient_id, episode_num) %>%
        summarise(
            start_date = min(event_date),
            end_date = max(event_date),
            events = list(event_type),
            .groups = "drop"
        ) %>%
        mutate(
            duration = as.numeric(difftime(end_date, start_date, units = "days")),
            duration_weeks = round(duration / 7, 1)
        )
    
    # 4. Add validation results and confidence scores
    episodes_with_validation <- episodes %>%
        left_join(data, by = "patient_id") %>%
        group_by(patient_id, episode_num) %>%
        mutate(
            validation_results = list(validate_episode(cur_data(), start_date, end_date)),
            confidence_score = calculate_episode_confidence(
                cur_data(),
                unlist(events),
                start_date,
                end_date,
                "live_birth"  # Default outcome type
            ),
            confidence_report = list(generate_confidence_report(
                cur_data(),
                unlist(events),
                start_date,
                end_date,
                "live_birth"  # Default outcome type
            ))
        ) %>%
        ungroup()
    
    return(episodes_with_validation)
}

# Run the analysis
results <- run_analysis(dataset)

# Create output directory if it doesn't exist
dir.create(here::here("output"), showWarnings = FALSE, recursive = TRUE)

# Save results
saveRDS(results, here::here("output", "analysis_results.rds"))

# Generate confidence score reports
confidence_reports <- do.call(rbind, lapply(1:nrow(results), function(i) {
    confidence_report_to_df(
        results$confidence_report[[i]],
        results$patient_id[i],
        results$episode_num[i]
    )
}))

# Save confidence reports
write_csv(confidence_reports, here::here("output", "confidence_reports.csv"))

# Print analysis results summary
cat("\nAnalysis Results Summary:\n")
cat("Number of patients:", n_distinct(results$patient_id), "\n")
cat("Total number of episodes:", nrow(results), "\n")
cat("Average episodes per patient:", round(nrow(results) / n_distinct(results$patient_id), 2), "\n")

# Show episode details for first few patients
cat("\nEpisode details for first few patients:\n")
print(head(results %>% select(patient_id, episode_num, start_date, end_date, duration_weeks, confidence_score)))

# Show validation results summary
cat("\nValidation Results Summary:\n")
validation_summary <- results %>%
  mutate(
    has_temporal_issues = map_lgl(validation_results, ~length(.$temporal) > 0),
    has_clinical_issues = map_lgl(validation_results, ~length(.$clinical) > 0),
    has_outcome_issues = map_lgl(validation_results, ~length(.$outcome) > 0)
  ) %>%
  select(-validation_results, -events, -confidence_report)

print(head(validation_summary))

# Save processed dataset with episode information
write_csv(validation_summary, here::here("output", "processed_dataset.csv"))

cat("\nProcessed dataset saved to output/processed_dataset.csv\n")
cat("Analysis results saved to output/analysis_results.rds\n")
cat("Confidence reports saved to output/confidence_reports.csv\n") 