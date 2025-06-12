# Load required R packages
library(tidyverse)
library(lubridate)
library(ggplot2)
library(knitr)
library(kableExtra)
library(rmarkdown)

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

# Define validation rules
VALIDATION_RULES <- list(
    temporal = list(
        max_booking_delay = 84,    # Maximum delay for booking visit (12 weeks)
        min_scan_interval = 14,    # Minimum interval between scans (2 weeks)
        max_scan_interval = 84,    # Maximum interval between scans (12 weeks)
        min_visit_interval = 7,    # Minimum interval between visits (1 week)
        max_visit_interval = 42    # Maximum interval between visits (6 weeks)
    ),
    clinical = list(
        max_conditions = 5,        # Maximum number of concurrent conditions
        max_medications = 8,       # Maximum number of concurrent medications
        min_weight_gain = 5,       # Minimum weight gain in kg
        max_weight_gain = 20,      # Maximum weight gain in kg
        min_blood_pressure = 90,   # Minimum systolic blood pressure
        max_blood_pressure = 160   # Maximum systolic blood pressure
    ),
    outcome = list(
        min_birth_weight = 500,    # Minimum birth weight in grams
        max_birth_weight = 6000,   # Maximum birth weight in grams
        min_apgar = 3,            # Minimum APGAR score
        max_apgar = 10            # Maximum APGAR score
    )
)

# Define confidence scoring factors
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
    )
)

# --- Helper Functions ---
# Function to convert dates to gestational age
calculate_gestational_age <- function(start_date, end_date) {
    as.numeric(difftime(end_date, start_date, units = "days"))
}

# Function to check if dates are within valid range
is_within_range <- function(date, start_date, end_date) {
    !is.na(date) && date >= start_date && date <= end_date
}

# --- Episode Identification Functions ---
# Function to identify episode start
identify_episode_start <- function(events, previous_episode_end = NULL) {
    # Get all relevant dates
    dates <- events[!is.na(events)]
    
    if (length(dates) == 0) return(NA)
    
    # Find earliest date
    start_date <- min(dates)
    
    # If there's a previous episode, ensure minimum gap
    if (!is.null(previous_episode_end)) {
        if (start_date <= previous_episode_end + days(180)) {
            return(NA)
        }
    }
    
    return(start_date)
}

# Function to identify episode end
identify_episode_end <- function(events, start_date, outcomes) {
    if (is.na(start_date)) return(NA)
    
    # Get all relevant dates
    all_dates <- c(events, outcomes)
    all_dates <- all_dates[!is.na(all_dates)]
    
    if (length(all_dates) == 0) return(NA)
    
    # Find latest date
    end_date <- max(all_dates)
    
    # Check if duration is within valid range
    duration <- calculate_gestational_age(start_date, end_date)
    if (duration > 294) {  # 42 weeks
        return(start_date + days(294))
    }
    
    return(end_date)
}

# --- Validation Functions ---
# Function to validate episode
validate_episode <- function(episode) {
    validation_results <- list(
        temporal = validate_temporal(episode),
        clinical = validate_clinical(episode),
        outcome = validate_outcome(episode)
    )
    
    return(validation_results)
}

# Function to validate temporal aspects
validate_temporal <- function(episode) {
    issues <- list()
    
    # Check booking visit delay
    if (!is.na(episode$pregnancy_test_date) && !is.na(episode$booking_visit_date)) {
        delay <- as.numeric(difftime(episode$booking_visit_date, episode$pregnancy_test_date, units = "days"))
        if (delay > VALIDATION_RULES$temporal$max_booking_delay) {
            issues$booking_delay <- "Booking visit delay exceeds maximum"
        }
    }
    
    # Check scan intervals
    if (!is.na(episode$dating_scan_date) && !is.na(episode$antenatal_screening_date)) {
        interval <- as.numeric(difftime(episode$antenatal_screening_date, episode$dating_scan_date, units = "days"))
        if (interval < VALIDATION_RULES$temporal$min_scan_interval) {
            issues$scan_interval <- "Scan interval below minimum"
        }
        if (interval > VALIDATION_RULES$temporal$max_scan_interval) {
            issues$scan_interval <- "Scan interval exceeds maximum"
        }
    }
    
    return(issues)
}

# Function to validate clinical aspects
validate_clinical <- function(episode) {
    issues <- list()
    
    # Count concurrent conditions
    conditions <- sum(sapply(episode, function(x) !is.na(x) && grepl("_date$", names(episode))))
    if (conditions > VALIDATION_RULES$clinical$max_conditions) {
        issues$too_many_conditions <- "Too many concurrent conditions"
    }
    
    # Count concurrent medications
    medications <- sum(sapply(episode, function(x) !is.na(x) && grepl("_medication_date$", names(episode))))
    if (medications > VALIDATION_RULES$clinical$max_medications) {
        issues$too_many_medications <- "Too many concurrent medications"
    }
    
    return(issues)
}

# Function to validate outcomes
validate_outcome <- function(episode) {
    issues <- list()
    
    # Check birth weight if available
    if (!is.null(episode$birth_weight)) {
        if (episode$birth_weight < VALIDATION_RULES$outcome$min_birth_weight) {
            issues$low_birth_weight <- "Birth weight below minimum"
        }
        if (episode$birth_weight > VALIDATION_RULES$outcome$max_birth_weight) {
            issues$high_birth_weight <- "Birth weight above maximum"
        }
    }
    
    # Check APGAR score if available
    if (!is.null(episode$apgar_score)) {
        if (episode$apgar_score < VALIDATION_RULES$outcome$min_apgar) {
            issues$low_apgar <- "APGAR score below minimum"
        }
        if (episode$apgar_score > VALIDATION_RULES$outcome$max_apgar) {
            issues$high_apgar <- "APGAR score above maximum"
        }
    }
    
    return(issues)
}

# --- Confidence Scoring Functions ---
# Function to calculate episode confidence
calculate_episode_confidence <- function(episode) {
    scores <- list(
        event_sequence = calculate_event_sequence_confidence(episode),
        clinical_indicators = calculate_clinical_confidence(episode),
        outcome_indicators = calculate_outcome_confidence(episode)
    )
    
    # Calculate weighted average
    weights <- c(0.4, 0.3, 0.3)  # Weights for each component
    confidence <- sum(unlist(scores) * weights)
    
    return(confidence)
}

# Function to calculate event sequence confidence
calculate_event_sequence_confidence <- function(episode) {
    score <- 0
    
    # Check for required events
    for (event in names(CONFIDENCE_FACTORS$event_sequence)) {
        if (!is.na(episode[[paste0(event, "_date")]])) {
            score <- score + CONFIDENCE_FACTORS$event_sequence[[event]]
        }
    }
    
    return(score)
}

# Function to calculate clinical confidence
calculate_clinical_confidence <- function(episode) {
    score <- 0
    
    # Check for clinical indicators
    for (indicator in names(CONFIDENCE_FACTORS$clinical_indicators)) {
        if (!is.na(episode[[paste0(indicator, "_date")]])) {
            score <- score + CONFIDENCE_FACTORS$clinical_indicators[[indicator]]
        }
    }
    
    return(score)
}

# Function to calculate outcome confidence
calculate_outcome_confidence <- function(episode) {
    score <- 0
    
    # Check for outcome indicators
    for (outcome in names(CONFIDENCE_FACTORS$outcome_indicators)) {
        if (!is.na(episode[[paste0(outcome, "_date")]])) {
            score <- score + CONFIDENCE_FACTORS$outcome_indicators[[outcome]]
        }
    }
    
    return(score)
}

# --- Main Analysis Functions ---
# Function to process episodes
process_episodes <- function(data) {
    # Convert data to long format for episode processing
    long_data <- data %>%
        pivot_longer(
            cols = ends_with("_date"),
            names_to = "event_type",
            values_to = "date"
        ) %>%
        filter(!is.na(date)) %>%
        arrange(patient_id, date)
    
    # Identify episodes
    episodes <- long_data %>%
        group_by(patient_id) %>%
        mutate(
            episode_number = cumsum(
                lag(date, default = first(date)) > date - days(180)
            )
        ) %>%
        group_by(patient_id, episode_number) %>%
        summarise(
            start_date = min(date),
            end_date = max(date),
            events = list(event_type),
            .groups = "drop"
        )
    
    # Validate episodes
    episodes <- episodes %>%
        mutate(
            validation_results = map(events, validate_episode),
            confidence_score = map_dbl(events, calculate_episode_confidence)
        )
    
    return(episodes)
}

# Function to generate quality report
generate_quality_report <- function(episodes) {
    # Calculate summary statistics
    summary_stats <- episodes %>%
        summarise(
            total_episodes = n(),
            valid_episodes = sum(map_lgl(validation_results, ~length(.) == 0)),
            average_confidence = mean(confidence_score)
        )
    
    # Create visualizations
    p1 <- ggplot(episodes, aes(x = start_date)) +
        geom_histogram(bins = 30) +
        labs(title = "Distribution of Episode Start Dates")
    
    p2 <- ggplot(episodes, aes(x = confidence_score)) +
        geom_histogram(bins = 30) +
        labs(title = "Distribution of Confidence Scores")
    
    # Generate report
    report <- list(
        summary_stats = summary_stats,
        plots = list(p1, p2)
    )
    
    return(report)
}

# --- Main Workflow ---
# Function to run analysis
run_analysis <- function(data) {
    # Process episodes
    episodes <- process_episodes(data)
    
    # Generate quality report
    quality_report <- generate_quality_report(episodes)
    
    # Return results
    return(list(
        episodes = episodes,
        quality_report = quality_report
    ))
}

# Execute analysis
results <- run_analysis(dataset)

# Save results
saveRDS(results, "analysis_results.rds")

# Generate HTML report
render("analysis_report.Rmd", output_file = "analysis_report.html") 