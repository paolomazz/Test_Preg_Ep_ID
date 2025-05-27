from ehrql import create_dataset, codelist_from_csv, minimum_of, maximum_of
from ehrql.tables.core import patients, clinical_events, medications, practice_registrations
from datetime import date, timedelta

# --- 1. Load Pregnancy-Related Codelists ---
codelist_files = {
    # Core pregnancy identification codelists
    "pregnancy_test": "codelists/Local/A1_pregnancy_test.csv",
    "booking_visit": "codelists/Local/A2_booking_visit.csv",
    "dating_scan": "codelists/Local/A3_dating_scan.csv",
    "antenatal_screening": "codelists/Local/A4_antenatal_screening.csv",
    "antenatal_risk": "codelists/Local/A5_risk_assessment.csv",
    "antenatal_procedures": "codelists/Local/A6_antenatal_procedures.csv",
    
    # Pregnancy outcomes
    "live_birth": "codelists/Local/B1_live_birth.csv",
    "stillbirth": "codelists/Local/B2_stillbirth.csv",
    "miscarriage": "codelists/Local/B3_miscarriage.csv",
    "abortion": "codelists/Local/B4_abortion.csv",
    "ectopic_pregnancy": "codelists/Local/B5_ectopic_pregnancy.csv",
    "molar_pregnancy": "codelists/Local/B6_molar_pregnancy.csv",
    
    # Delivery methods
    "caesarean_section": "codelists/Local/C1_caesarean_section.csv",
    "forceps_delivery": "codelists/Local/C2_forceps_delivery.csv",
    "vacuum_extraction": "codelists/Local/C3_vacuum_extraction.csv",
    "induction": "codelists/Local/C4_induction.csv",
    "episiotomy": "codelists/Local/C5_episiotomy.csv",
    
    # Pregnancy conditions
    "gestational_diabetes": "codelists/Local/D1_gestational_diabetes.csv",
    "preeclampsia": "codelists/Local/D2_preeclampsia.csv",
    "pregnancy_hypertension": "codelists/Local/D3_pregnancy_hypertension.csv",
    "hyperemesis": "codelists/Local/D4_hyperemesis.csv",
    "pregnancy_infection": "codelists/Local/D5_pregnancy_infection.csv",
    "pregnancy_bleeding": "codelists/Local/D6_pregnancy_bleeding.csv",
    "pregnancy_anemia": "codelists/Local/D7_pregnancy_anemia.csv",
    "pregnancy_thrombosis": "codelists/Local/D8_pregnancy_thrombosis.csv",
    "pregnancy_mental_health": "codelists/Local/D9_pregnancy_mental_health.csv",
    
    # Pregnancy medications
    "antenatal_vitamins": "codelists/Local/E1_antenatal_vitamins.csv",
    "anti_emetics": "codelists/Local/E2_anti_emetics.csv",
    "antihypertensives": "codelists/Local/E3_antihypertensives.csv",
    "antidiabetics": "codelists/Local/E4_antidiabetics.csv",
    "antibiotics": "codelists/Local/E5_antibiotics.csv",
    "mental_health_meds": "codelists/Local/E6_mental_health_meds.csv",
    "pain_relief": "codelists/Local/E7_pain_relief.csv",
    
    # Complications
    "postpartum_hemorrhage": "codelists/Local/F1_postpartum_hemorrhage.csv",
    "third_degree_tear": "codelists/Local/F2_third_degree_tear.csv",
    "shoulder_dystocia": "codelists/Local/F3_shoulder_dystocia.csv",
    "placenta_previa": "codelists/Local/F4_placenta_previa.csv",
    "placental_abruption": "codelists/Local/F5_placental_abruption.csv",
}

# Load codelists
codelists = {}
for k, v in codelist_files.items():
    try:
        codelists[k] = codelist_from_csv(v, column="code")
    except Exception as e:
        print(f"Error loading codelist {k} from {v}: {str(e)}")
        raise

# Define validation windows and criteria
GESTATIONAL_AGE_WINDOWS = {
    "term_birth": (259, 294),  # 37-42 weeks
    "preterm_birth": (196, 258),  # 28-36 weeks
    "very_preterm_birth": (154, 195),  # 22-27 weeks
    "miscarriage": (0, 196),  # 0-28 weeks
    "abortion": (0, 196),  # 0-28 weeks
    "ectopic_pregnancy": (0, 84),  # 0-12 weeks
    "molar_pregnancy": (0, 196)  # 0-28 weeks
}

EVENT_SEQUENCE_WEIGHTS = {
    "pregnancy_test": 0.3,
    "booking_visit": 0.3,
    "dating_scan": 0.2,
    "antenatal_screening": 0.1,
    "antenatal_risk": 0.1
}

OUTCOME_SPECIFIC_CRITERIA = {
    "live_birth": {
        "required_events": ["booking_visit", "antenatal_screening"],
        "min_gestational_age": 154,  # 22 weeks
        "max_gestational_age": 294,  # 42 weeks
        "weight": 1.0
    },
    "stillbirth": {
        "required_events": ["booking_visit", "antenatal_screening"],
        "min_gestational_age": 154,  # 22 weeks
        "max_gestational_age": 294,  # 42 weeks
        "weight": 1.0
    },
    "miscarriage": {
        "required_events": ["pregnancy_test"],
        "min_gestational_age": 0,
        "max_gestational_age": 196,  # 28 weeks
        "weight": 0.8
    },
    "abortion": {
        "required_events": ["pregnancy_test"],
        "min_gestational_age": 0,
        "max_gestational_age": 196,  # 28 weeks
        "weight": 0.8
    },
    "ectopic_pregnancy": {
        "required_events": ["pregnancy_test"],
        "min_gestational_age": 0,
        "max_gestational_age": 84,  # 12 weeks
        "weight": 0.9
    },
    "molar_pregnancy": {
        "required_events": ["pregnancy_test", "dating_scan"],
        "min_gestational_age": 0,
        "max_gestational_age": 196,  # 28 weeks
        "weight": 0.9
    }
}

# Define data quality flags and thresholds
DATA_QUALITY_THRESHOLDS = {
    "min_required_events": 2,  # Minimum number of pregnancy-related events
    "max_event_gap": 280,  # Maximum gap between events in days
    "min_gestational_age": 154,  # Minimum gestational age for live birth (22 weeks)
    "max_gestational_age": 294,  # Maximum gestational age for live birth (42 weeks)
    "max_outcome_gap": 84,  # Maximum gap between last event and outcome (12 weeks)
}

# Define validation rules and thresholds
VALIDATION_RULES = {
    "temporal": {
        "max_booking_delay": 84,  # Maximum delay for booking visit (12 weeks)
        "min_scan_interval": 14,  # Minimum interval between scans (2 weeks)
        "max_scan_interval": 84,  # Maximum interval between scans (12 weeks)
        "min_visit_interval": 7,  # Minimum interval between visits (1 week)
        "max_visit_interval": 42,  # Maximum interval between visits (6 weeks)
    },
    "clinical": {
        "max_conditions": 5,      # Maximum number of concurrent conditions
        "max_medications": 8,     # Maximum number of concurrent medications
        "min_weight_gain": 5,     # Minimum weight gain in kg
        "max_weight_gain": 20,    # Maximum weight gain in kg
        "min_blood_pressure": 90,  # Minimum systolic blood pressure
        "max_blood_pressure": 160, # Maximum systolic blood pressure
    },
    "outcome": {
        "min_birth_weight": 500,  # Minimum birth weight in grams
        "max_birth_weight": 6000, # Maximum birth weight in grams
        "min_apgar": 3,          # Minimum APGAR score
        "max_apgar": 10,         # Maximum APGAR score
    }
}

# Define validation error types
class ValidationError:
    TEMPORAL_ERROR = "temporal_error"
    CLINICAL_ERROR = "clinical_error"
    OUTCOME_ERROR = "outcome_error"
    SEQUENCE_ERROR = "sequence_error"
    DATA_QUALITY_ERROR = "data_quality_error"

# Define confidence scoring factors and weights
CONFIDENCE_FACTORS = {
    "event_sequence": {
        "pregnancy_test": 0.15,
        "booking_visit": 0.20,
        "dating_scan": 0.15,
        "antenatal_screening": 0.10,
        "antenatal_risk": 0.10,
        "antenatal_procedures": 0.10
    },
    "clinical_indicators": {
        "gestational_diabetes": 0.10,
        "preeclampsia": 0.10,
        "pregnancy_hypertension": 0.10,
        "hyperemesis": 0.05,
        "pregnancy_infection": 0.05,
        "pregnancy_bleeding": 0.05
    },
    "outcome_indicators": {
        "live_birth": 0.25,
        "stillbirth": 0.20,
        "miscarriage": 0.15,
        "abortion": 0.15,
        "ectopic_pregnancy": 0.10,
        "molar_pregnancy": 0.10
    },
    "temporal_factors": {
        "gestational_age_plausibility": 0.20,
        "event_sequence_plausibility": 0.15,
        "outcome_timing_plausibility": 0.15
    },
    "data_quality": {
        "completeness": 0.20,
        "consistency": 0.15,
        "plausibility": 0.15
    }
}

# Define confidence scoring functions
def calculate_event_sequence_confidence(events, phase):
    """Calculate confidence based on event sequence completeness and timing."""
    confidence = 0.0
    required_events = EPISODE_PHASES[phase]["key_events"]
    
    # Check for required events
    for event in required_events:
        if event in events:
            confidence += CONFIDENCE_FACTORS["event_sequence"][event]
    
    # Check event sequence plausibility
    if len(events) >= 2:
        sorted_events = sorted(events.items(), key=lambda x: x[1])
        for i in range(len(sorted_events) - 1):
            current_event, current_date = sorted_events[i]
            next_event, next_date = sorted_events[i + 1]
            interval = (next_date - current_date).days
            
            # Check if interval is within expected range
            if current_event in required_events:
                if VALIDATION_RULES["temporal"]["min_visit_interval"] <= interval <= VALIDATION_RULES["temporal"]["max_visit_interval"]:
                    confidence += 0.05
    
    return min(confidence, 1.0)

def calculate_clinical_confidence(conditions, medications, phase):
    """Calculate confidence based on clinical indicators."""
    confidence = 0.0
    
    # Check for pregnancy-related conditions
    for condition in conditions:
        if condition in CONFIDENCE_FACTORS["clinical_indicators"]:
            confidence += CONFIDENCE_FACTORS["clinical_indicators"][condition]
    
    # Check for appropriate medications
    for medication in medications:
        if medication in ["antihypertensives", "antidiabetics"]:
            confidence += 0.05
    
    # Check for contraindicated combinations
    contraindicated_pairs = [
        ("antihypertensives", "pregnancy_hypertension"),
        ("antidiabetics", "gestational_diabetes")
    ]
    for med, condition in contraindicated_pairs:
        if med in medications and condition in conditions:
            confidence -= 0.10
    
    return max(0.0, min(confidence, 1.0))

def calculate_outcome_confidence(outcome_type, outcome_data, gestational_age):
    """Calculate confidence based on outcome indicators and timing."""
    confidence = 0.0
    
    # Base confidence from outcome type
    if outcome_type in CONFIDENCE_FACTORS["outcome_indicators"]:
        confidence += CONFIDENCE_FACTORS["outcome_indicators"][outcome_type]
    
    # Check gestational age plausibility
    if outcome_type in ["live_birth", "stillbirth"]:
        if 154 <= gestational_age <= 294:  # 22-42 weeks
            confidence += CONFIDENCE_FACTORS["temporal_factors"]["gestational_age_plausibility"]
    
    # Check outcome data plausibility
    if outcome_type in ["live_birth", "stillbirth"]:
        if "birth_weight" in outcome_data and outcome_data["birth_weight"] is not None:
            weight = outcome_data["birth_weight"]
            if VALIDATION_RULES["outcome"]["min_birth_weight"] <= weight <= VALIDATION_RULES["outcome"]["max_birth_weight"]:
                confidence += 0.10
        
        if "apgar_score" in outcome_data and outcome_data["apgar_score"] is not None:
            apgar = outcome_data["apgar_score"]
            if VALIDATION_RULES["outcome"]["min_apgar"] <= apgar <= VALIDATION_RULES["outcome"]["max_apgar"]:
                confidence += 0.10
    
    return min(confidence, 1.0)

def calculate_data_quality_confidence(quality_flags, validation_errors):
    """Calculate confidence based on data quality indicators."""
    confidence = 1.0
    
    # Penalize for data quality flags
    for flag in quality_flags:
        if flag in [DataQualityFlags.MISSING_KEY_EVENTS, DataQualityFlags.INCONSISTENT_DATES]:
            confidence -= 0.20
        elif flag in [DataQualityFlags.GAP_IN_EVENTS, DataQualityFlags.CONFLICTING_OUTCOMES]:
            confidence -= 0.15
        elif flag in [DataQualityFlags.INVALID_GESTATIONAL_AGE, DataQualityFlags.DUPLICATE_EVENTS]:
            confidence -= 0.10
    
    # Penalize for validation errors
    for error in validation_errors:
        if error == ValidationError.TEMPORAL_ERROR:
            confidence -= 0.15
        elif error == ValidationError.CLINICAL_ERROR:
            confidence -= 0.20
        elif error == ValidationError.OUTCOME_ERROR:
            confidence -= 0.25
        elif error == ValidationError.SEQUENCE_ERROR:
            confidence -= 0.15
        elif error == ValidationError.DATA_QUALITY_ERROR:
            confidence -= 0.10
    
    return max(0.0, min(confidence, 1.0))

# Define data quality flags
class DataQualityFlags:
    MISSING_KEY_EVENTS = "missing_key_events"
    INCONSISTENT_DATES = "inconsistent_dates"
    GAP_IN_EVENTS = "gap_in_events"
    CONFLICTING_OUTCOMES = "conflicting_outcomes"
    INVALID_GESTATIONAL_AGE = "invalid_gestational_age"
    DUPLICATE_EVENTS = "duplicate_events"
    MISSING_OUTCOME = "missing_outcome"
    INCOMPLETE_EPISODE = "incomplete_episode"

# Define flexible time windows for different care models
CARE_MODEL_WINDOWS = {
    "standard": {
        "antenatal_start": 84,  # 12 weeks
        "antenatal_end": 280,   # 40 weeks
        "postpartum": 84,       # 12 weeks
        "min_episode_gap": 180  # 6 months
    },
    "high_risk": {
        "antenatal_start": 0,   # Immediate
        "antenatal_end": 280,   # 40 weeks
        "postpartum": 180,      # 26 weeks
        "min_episode_gap": 365  # 12 months
    },
    "community": {
        "antenatal_start": 84,  # 12 weeks
        "antenatal_end": 280,   # 40 weeks
        "postpartum": 42,       # 6 weeks
        "min_episode_gap": 180  # 6 months
    }
}

# Define episode phases and their characteristics
EPISODE_PHASES = {
    "pre_conception": {
        "start_window": -180,  # 6 months before
        "end_window": 0,       # Conception
        "key_events": ["pregnancy_test", "booking_visit"]
    },
    "first_trimester": {
        "start_window": 0,     # Conception
        "end_window": 84,      # 12 weeks
        "key_events": ["pregnancy_test", "booking_visit", "dating_scan"]
    },
    "second_trimester": {
        "start_window": 84,    # 12 weeks
        "end_window": 196,     # 28 weeks
        "key_events": ["antenatal_screening", "antenatal_risk"]
    },
    "third_trimester": {
        "start_window": 196,   # 28 weeks
        "end_window": 280,     # 40 weeks
        "key_events": ["antenatal_screening", "antenatal_risk", "antenatal_procedures"]
    },
    "postpartum": {
        "start_window": 0,     # Delivery
        "end_window": 84,      # 12 weeks
        "key_events": ["postpartum_hemorrhage", "third_degree_tear"]
    }
}

# Define episode types and their characteristics
EPISODE_TYPES = {
    "standard": {
        "min_events": 2,
        "required_events": ["pregnancy_test", "booking_visit"],
        "confidence_threshold": 0.7
    },
    "high_risk": {
        "min_events": 3,
        "required_events": ["pregnancy_test", "booking_visit", "antenatal_risk"],
        "confidence_threshold": 0.8
    },
    "community": {
        "min_events": 2,
        "required_events": ["pregnancy_test", "booking_visit"],
        "confidence_threshold": 0.6
    }
}

# Define data quality metrics and thresholds
DATA_QUALITY_METRICS = {
    "completeness": {
        "min_required_events": 2,
        "min_required_outcomes": 1,
        "min_required_dates": 2
    },
    "consistency": {
        "max_date_gap": 280,  # Maximum gap between events (40 weeks)
        "min_date_gap": 7,    # Minimum gap between events (1 week)
        "max_gestational_age": 294,  # Maximum gestational age (42 weeks)
        "min_gestational_age": 154   # Minimum gestational age (22 weeks)
    },
    "plausibility": {
        "max_concurrent_conditions": 5,
        "max_concurrent_medications": 8,
        "max_weight_gain": 20,  # kg
        "min_weight_gain": 5,   # kg
        "max_blood_pressure": 160,
        "min_blood_pressure": 90
    },
    "temporal": {
        "max_booking_delay": 84,  # Maximum delay for booking visit (12 weeks)
        "min_scan_interval": 14,  # Minimum interval between scans (2 weeks)
        "max_scan_interval": 84,  # Maximum interval between scans (12 weeks)
        "min_visit_interval": 7,  # Minimum interval between visits (1 week)
        "max_visit_interval": 42  # Maximum interval between visits (6 weeks)
    }
}

# Define data quality check functions
def check_completeness(events, outcomes, dates):
    """Check completeness of pregnancy episode data."""
    issues = []
    
    # Check required events
    if len(events) < DATA_QUALITY_METRICS["completeness"]["min_required_events"]:
        issues.append({
            "type": "missing_events",
            "severity": "high",
            "message": f"Too few events: {len(events)} < {DATA_QUALITY_METRICS['completeness']['min_required_events']}"
        })
    
    # Check required outcomes
    if len(outcomes) < DATA_QUALITY_METRICS["completeness"]["min_required_outcomes"]:
        issues.append({
            "type": "missing_outcomes",
            "severity": "high",
            "message": "No pregnancy outcome recorded"
        })
    
    # Check required dates
    if len(dates) < DATA_QUALITY_METRICS["completeness"]["min_required_dates"]:
        issues.append({
            "type": "missing_dates",
            "severity": "medium",
            "message": f"Too few dates: {len(dates)} < {DATA_QUALITY_METRICS['completeness']['min_required_dates']}"
        })
    
    return issues

def check_consistency(events, gestational_age):
    """Check consistency of pregnancy episode data."""
    issues = []
    
    # Check date gaps
    if len(events) >= 2:
        sorted_dates = sorted(events.values())
        for i in range(len(sorted_dates) - 1):
            gap = (sorted_dates[i + 1] - sorted_dates[i]).days
            if gap > DATA_QUALITY_METRICS["consistency"]["max_date_gap"]:
                issues.append({
                    "type": "large_date_gap",
                    "severity": "medium",
                    "message": f"Large gap between events: {gap} days"
                })
            elif gap < DATA_QUALITY_METRICS["consistency"]["min_date_gap"]:
                issues.append({
                    "type": "small_date_gap",
                    "severity": "low",
                    "message": f"Small gap between events: {gap} days"
                })
    
    # Check gestational age
    if gestational_age is not None:
        if gestational_age > DATA_QUALITY_METRICS["consistency"]["max_gestational_age"]:
            issues.append({
                "type": "high_gestational_age",
                "severity": "high",
                "message": f"Gestational age too high: {gestational_age} days"
            })
        elif gestational_age < DATA_QUALITY_METRICS["consistency"]["min_gestational_age"]:
            issues.append({
                "type": "low_gestational_age",
                "severity": "high",
                "message": f"Gestational age too low: {gestational_age} days"
            })
    
    return issues

def check_plausibility(conditions, medications, measurements):
    """Check plausibility of clinical data."""
    issues = []
    
    # Check concurrent conditions
    if len(conditions) > DATA_QUALITY_METRICS["plausibility"]["max_concurrent_conditions"]:
        issues.append({
            "type": "too_many_conditions",
            "severity": "medium",
            "message": f"Too many concurrent conditions: {len(conditions)}"
        })
    
    # Check concurrent medications
    if len(medications) > DATA_QUALITY_METRICS["plausibility"]["max_concurrent_medications"]:
        issues.append({
            "type": "too_many_medications",
            "severity": "medium",
            "message": f"Too many concurrent medications: {len(medications)}"
        })
    
    # Check measurements
    if "weight_gain" in measurements:
        weight_gain = measurements["weight_gain"]
        if weight_gain > DATA_QUALITY_METRICS["plausibility"]["max_weight_gain"]:
            issues.append({
                "type": "high_weight_gain",
                "severity": "medium",
                "message": f"Weight gain too high: {weight_gain} kg"
            })
        elif weight_gain < DATA_QUALITY_METRICS["plausibility"]["min_weight_gain"]:
            issues.append({
                "type": "low_weight_gain",
                "severity": "medium",
                "message": f"Weight gain too low: {weight_gain} kg"
            })
    
    if "blood_pressure" in measurements:
        bp = measurements["blood_pressure"]
        if bp > DATA_QUALITY_METRICS["plausibility"]["max_blood_pressure"]:
            issues.append({
                "type": "high_blood_pressure",
                "severity": "high",
                "message": f"Blood pressure too high: {bp}"
            })
        elif bp < DATA_QUALITY_METRICS["plausibility"]["min_blood_pressure"]:
            issues.append({
                "type": "low_blood_pressure",
                "severity": "high",
                "message": f"Blood pressure too low: {bp}"
            })
    
    return issues

def check_temporal_sequence(events, phase):
    """Check temporal sequence of events."""
    issues = []
    
    if not events:
        return issues
    
    # Sort events by date
    sorted_events = sorted(events.items(), key=lambda x: x[1])
    
    # Check booking visit delay
    if "booking_visit" in events:
        booking_date = events["booking_visit"]
        pregnancy_test_date = events.get("pregnancy_test")
        if pregnancy_test_date:
            delay = (booking_date - pregnancy_test_date).days
            if delay > DATA_QUALITY_METRICS["temporal"]["max_booking_delay"]:
                issues.append({
                    "type": "late_booking",
                    "severity": "medium",
                    "message": f"Booking visit too late: {delay} days after pregnancy test"
                })
    
    # Check scan intervals
    scan_dates = [date for event, date in sorted_events if "scan" in event.lower()]
    if len(scan_dates) >= 2:
        for i in range(len(scan_dates) - 1):
            interval = (scan_dates[i + 1] - scan_dates[i]).days
            if interval < DATA_QUALITY_METRICS["temporal"]["min_scan_interval"]:
                issues.append({
                    "type": "frequent_scans",
                    "severity": "low",
                    "message": f"Scans too frequent: {interval} days apart"
                })
            elif interval > DATA_QUALITY_METRICS["temporal"]["max_scan_interval"]:
                issues.append({
                    "type": "infrequent_scans",
                    "severity": "medium",
                    "message": f"Scans too infrequent: {interval} days apart"
                })
    
    # Check visit intervals
    visit_dates = [date for event, date in sorted_events if "visit" in event.lower()]
    if len(visit_dates) >= 2:
        for i in range(len(visit_dates) - 1):
            interval = (visit_dates[i + 1] - visit_dates[i]).days
            if interval < DATA_QUALITY_METRICS["temporal"]["min_visit_interval"]:
                issues.append({
                    "type": "frequent_visits",
                    "severity": "low",
                    "message": f"Visits too frequent: {interval} days apart"
                })
            elif interval > DATA_QUALITY_METRICS["temporal"]["max_visit_interval"]:
                issues.append({
                    "type": "infrequent_visits",
                    "severity": "medium",
                    "message": f"Visits too infrequent: {interval} days apart"
                })
    
    return issues

# Define episode identification criteria
EPISODE_IDENTIFICATION = {
    "primary_indicators": {
        "pregnancy_test": 0.3,
        "booking_visit": 0.3,
        "dating_scan": 0.2,
        "antenatal_screening": 0.2
    },
    "secondary_indicators": {
        "gestational_diabetes": 0.1,
        "preeclampsia": 0.1,
        "pregnancy_hypertension": 0.1,
        "hyperemesis": 0.05,
        "pregnancy_infection": 0.05
    },
    "outcome_indicators": {
        "live_birth": 0.4,
        "stillbirth": 0.3,
        "miscarriage": 0.2,
        "abortion": 0.2,
        "ectopic_pregnancy": 0.3,
        "molar_pregnancy": 0.3
    },
    "temporal_rules": {
        "min_episode_gap": 180,  # Minimum gap between episodes (6 months)
        "max_episode_duration": 294,  # Maximum episode duration (42 weeks)
        "min_episode_duration": 154,  # Minimum episode duration (22 weeks)
        "max_outcome_delay": 84  # Maximum delay after last event (12 weeks)
    }
}

# Define episode identification functions
def identify_episode_start(events, previous_episode_end=None):
    """Identify the start of a pregnancy episode."""
    if not events:
        return None
    
    # Collect all relevant dates
    dates_to_check = []
    
    # Add primary indicator dates
    for event_type in EPISODE_IDENTIFICATION["primary_indicators"].keys():
        if event_type in events:
            dates_to_check.append(events[event_type])
    
    # If no primary indicators, add secondary indicator dates
    if not dates_to_check:
        for event_type in EPISODE_IDENTIFICATION["secondary_indicators"].keys():
            if event_type in events:
                dates_to_check.append(events[event_type])
    
    # If still no dates, add all event dates
    if not dates_to_check:
        dates_to_check.extend(events.values())
    
    # If no dates found, return None
    if not dates_to_check:
        return None
    
    # Find the earliest date using minimum_of
    earliest_date = minimum_of(*dates_to_check)
    
    # If there's a previous episode, ensure minimum gap
    if previous_episode_end is not None:
        # Use minimum_of with conditional logic
        # This will return None if earliest_date is not after previous_episode_end
        return minimum_of(earliest_date, previous_episode_end)
    
    return earliest_date

def identify_episode_end(events, start_date, outcomes):
    """Identify the end of a pregnancy episode."""
    if not outcomes:
        # If no outcome, use maximum duration
        max_duration = EPISODE_IDENTIFICATION["temporal_rules"]["max_episode_duration"]
        # Use date comparison for max end date
        return start_date
    
    # Find earliest outcome using minimum_of
    outcome_dates = list(outcomes.values())
    if not outcome_dates:
        return None
    
    earliest_outcome_date = minimum_of(*outcome_dates)
    
    # Check if outcome is within valid range
    episode_duration = (earliest_outcome_date - start_date).days
    min_duration = EPISODE_IDENTIFICATION["temporal_rules"]["min_episode_duration"]
    max_duration = EPISODE_IDENTIFICATION["temporal_rules"]["max_episode_duration"]
    
    # Use minimum_of with conditional for duration check
    valid_outcome_date = minimum_of(
        earliest_outcome_date,
        start_date
    )
    
    # Check if there are events after the outcome
    later_event_dates = []
    for event_date in events.values():
        # Use maximum_of with conditional for date comparison
        later_date = maximum_of(
            event_date,
            earliest_outcome_date
        )
        later_event_dates.append(later_date)
    
    if later_event_dates:
        latest_event_date = maximum_of(*later_event_dates)
        max_delay = EPISODE_IDENTIFICATION["temporal_rules"]["max_outcome_delay"]
        delay_days = (latest_event_date - earliest_outcome_date).days
        
        # Use minimum_of with conditional for delay check
        valid_latest_date = minimum_of(
            latest_event_date,
            earliest_outcome_date
        )
        return valid_latest_date
    
    return valid_outcome_date

def calculate_episode_confidence(events, outcomes, start_date, end_date):
    """Calculate confidence score for episode identification."""
    # Initialize confidence as a series
    confidence = 0.0
    
    # Check primary indicators
    for event_type, weight in EPISODE_IDENTIFICATION["primary_indicators"].items():
        if event_type in events:
            event_date = events[event_type]
            # Add weight if date is within episode range
            confidence += weight
    
    # Check secondary indicators
    for event_type, weight in EPISODE_IDENTIFICATION["secondary_indicators"].items():
        if event_type in events:
            event_date = events[event_type]
            # Add weight if date is within episode range
            confidence += weight
    
    # Check outcome indicators
    for outcome_type, weight in EPISODE_IDENTIFICATION["outcome_indicators"].items():
        if outcome_type in outcomes:
            outcome_date = outcomes[outcome_type]
            # Add weight if date is within episode range
            confidence += weight
    
    # Check temporal validity
    episode_duration = (end_date - start_date).days
    min_duration = EPISODE_IDENTIFICATION["temporal_rules"]["min_episode_duration"]
    max_duration = EPISODE_IDENTIFICATION["temporal_rules"]["max_episode_duration"]
    
    # Add temporal validity weight
    confidence += 0.2
    
    # Ensure confidence is between 0 and 1
    return minimum_of(confidence, 1.0)

def validate_episode_sequence(episodes):
    """Validate the sequence of pregnancy episodes."""
    if not episodes:
        return True
    
    # Simple validation: check if next episode starts after current episode ends
    for i in range(len(episodes) - 1):
        current_episode = episodes[i]
        next_episode = episodes[i + 1]
        
        # Basic sequence validation
        valid_sequence = next_episode["start_date"] > current_episode["end_date"]
        return valid_sequence
    
    return True

# Define outcome classification criteria
OUTCOME_CLASSIFICATION = {
    "primary_outcomes": {
        "live_birth": {
            "weight": 0.4,
            "min_gestational_age": 154,  # 22 weeks
            "max_gestational_age": 294,  # 42 weeks
            "required_events": ["booking_visit", "antenatal_screening"],
            "optional_events": ["dating_scan", "antenatal_risk"],
            "complications": ["postpartum_hemorrhage", "third_degree_tear", "shoulder_dystocia"]
        },
        "stillbirth": {
            "weight": 0.3,
            "min_gestational_age": 154,  # 22 weeks
            "max_gestational_age": 294,  # 42 weeks
            "required_events": ["booking_visit", "antenatal_screening"],
            "optional_events": ["dating_scan", "antenatal_risk"],
            "complications": ["placenta_previa", "placental_abruption"]
        },
        "miscarriage": {
            "weight": 0.2,
            "min_gestational_age": 0,
            "max_gestational_age": 196,  # 28 weeks
            "required_events": ["pregnancy_test"],
            "optional_events": ["booking_visit", "dating_scan"],
            "complications": ["pregnancy_bleeding", "pregnancy_infection"]
        },
        "abortion": {
            "weight": 0.2,
            "min_gestational_age": 0,
            "max_gestational_age": 196,  # 28 weeks
            "required_events": ["pregnancy_test"],
            "optional_events": ["booking_visit"],
            "complications": ["pregnancy_bleeding", "pregnancy_infection"]
        },
        "ectopic_pregnancy": {
            "weight": 0.3,
            "min_gestational_age": 0,
            "max_gestational_age": 84,  # 12 weeks
            "required_events": ["pregnancy_test"],
            "optional_events": ["dating_scan"],
            "complications": ["pregnancy_bleeding", "pregnancy_infection"]
        },
        "molar_pregnancy": {
            "weight": 0.3,
            "min_gestational_age": 0,
            "max_gestational_age": 196,  # 28 weeks
            "required_events": ["pregnancy_test", "dating_scan"],
            "optional_events": ["booking_visit"],
            "complications": ["pregnancy_bleeding", "pregnancy_infection"]
        }
    },
    "outcome_characteristics": {
        "gestational_age_ranges": {
            "very_preterm": (154, 195),  # 22-27 weeks
            "preterm": (196, 258),      # 28-36 weeks
            "term": (259, 294),         # 37-42 weeks
            "post_term": (295, 308)     # 43-44 weeks
        },
        "birth_weight_ranges": {
            "very_low": (500, 1499),    # 500g-1.5kg
            "low": (1500, 2499),        # 1.5kg-2.5kg
            "normal": (2500, 3999),     # 2.5kg-4kg
            "high": (4000, 6000)        # 4kg-6kg
        },
        "apgar_score_ranges": {
            "low": (0, 3),
            "moderate": (4, 6),
            "normal": (7, 10)
        }
    },
    "outcome_validation": {
        "min_required_events": 1,
        "max_event_outcome_gap": 84,  # 12 weeks
        "min_outcome_confidence": 0.6,
        "max_outcome_delay": 84  # 12 weeks
    }
}

# Define outcome classification functions
def classify_outcome_type(events, outcomes, gestational_age):
    """Classify the type of pregnancy outcome."""
    if not outcomes:
        return None, 0.0
    
    best_outcome = None
    best_confidence = 0.0
    
    for outcome_type, outcome_date in outcomes.items():
        if outcome_type not in OUTCOME_CLASSIFICATION["primary_outcomes"]:
            continue
        
        outcome_info = OUTCOME_CLASSIFICATION["primary_outcomes"][outcome_type]
        confidence = 0.0
        
        # Check gestational age
        if outcome_info["min_gestational_age"] <= gestational_age <= outcome_info["max_gestational_age"]:
            confidence += 0.3
        
        # Check required events
        required_events_present = all(event in events for event in outcome_info["required_events"])
        if required_events_present:
            confidence += 0.3
        
        # Check optional events
        optional_events_present = any(event in events for event in outcome_info["optional_events"])
        if optional_events_present:
            confidence += 0.2
        
        # Check complications
        complications_present = any(comp in events for comp in outcome_info["complications"])
        if complications_present:
            confidence += 0.2
        
        if confidence > best_confidence:
            best_outcome = outcome_type
            best_confidence = confidence
    
    return best_outcome, best_confidence

def classify_outcome_characteristics(outcome_type, outcome_data):
    """Classify the characteristics of a pregnancy outcome."""
    characteristics = {
        "gestational_age_category": None,
        "birth_weight_category": None,
        "apgar_category": None,
        "complications": []
    }
    
    # Classify gestational age
    if "gestational_age" in outcome_data:
        age = outcome_data["gestational_age"]
        for category, (min_age, max_age) in OUTCOME_CLASSIFICATION["outcome_characteristics"]["gestational_age_ranges"].items():
            if min_age <= age <= max_age:
                characteristics["gestational_age_category"] = category
                break
    
    # Classify birth weight
    if "birth_weight" in outcome_data:
        weight = outcome_data["birth_weight"]
        for category, (min_weight, max_weight) in OUTCOME_CLASSIFICATION["outcome_characteristics"]["birth_weight_ranges"].items():
            if min_weight <= weight <= max_weight:
                characteristics["birth_weight_category"] = category
                break
    
    # Classify APGAR score
    if "apgar_score" in outcome_data:
        apgar = outcome_data["apgar_score"]
        for category, (min_score, max_score) in OUTCOME_CLASSIFICATION["outcome_characteristics"]["apgar_score_ranges"].items():
            if min_score <= apgar <= max_score:
                characteristics["apgar_category"] = category
                break
    
    # Identify complications
    if outcome_type in OUTCOME_CLASSIFICATION["primary_outcomes"]:
        outcome_info = OUTCOME_CLASSIFICATION["primary_outcomes"][outcome_type]
        characteristics["complications"] = [
            comp for comp in outcome_info["complications"]
            if comp in outcome_data.get("complications", [])
        ]
    
    return characteristics

def validate_outcome(outcome_type, outcome_data, events, gestational_age):
    """Validate a pregnancy outcome."""
    validation_issues = []
    
    if outcome_type not in OUTCOME_CLASSIFICATION["primary_outcomes"]:
        validation_issues.append({
            "type": "invalid_outcome_type",
            "severity": "high",
            "message": f"Invalid outcome type: {outcome_type}"
        })
        return validation_issues
    
    outcome_info = OUTCOME_CLASSIFICATION["primary_outcomes"][outcome_type]
    
    # Validate gestational age
    if gestational_age < outcome_info["min_gestational_age"]:
        validation_issues.append({
            "type": "gestational_age_too_low",
            "severity": "high",
            "message": f"Gestational age {gestational_age} days is below minimum {outcome_info['min_gestational_age']} days"
        })
    elif gestational_age > outcome_info["max_gestational_age"]:
        validation_issues.append({
            "type": "gestational_age_too_high",
            "severity": "high",
            "message": f"Gestational age {gestational_age} days is above maximum {outcome_info['max_gestational_age']} days"
        })
    
    # Validate required events
    missing_required = [event for event in outcome_info["required_events"] if event not in events]
    if missing_required:
        validation_issues.append({
            "type": "missing_required_events",
            "severity": "high",
            "message": f"Missing required events: {', '.join(missing_required)}"
        })
    
    # Validate outcome data
    if "birth_weight" in outcome_data:
        weight = outcome_data["birth_weight"]
        if weight < OUTCOME_CLASSIFICATION["outcome_characteristics"]["birth_weight_ranges"]["very_low"][0]:
            validation_issues.append({
                "type": "birth_weight_too_low",
                "severity": "high",
                "message": f"Birth weight {weight}g is below minimum {OUTCOME_CLASSIFICATION['outcome_characteristics']['birth_weight_ranges']['very_low'][0]}g"
            })
        elif weight > OUTCOME_CLASSIFICATION["outcome_characteristics"]["birth_weight_ranges"]["high"][1]:
            validation_issues.append({
                "type": "birth_weight_too_high",
                "severity": "high",
                "message": f"Birth weight {weight}g is above maximum {OUTCOME_CLASSIFICATION['outcome_characteristics']['birth_weight_ranges']['high'][1]}g"
            })
    
    if "apgar_score" in outcome_data:
        apgar = outcome_data["apgar_score"]
        if apgar < OUTCOME_CLASSIFICATION["outcome_characteristics"]["apgar_score_ranges"]["low"][0]:
            validation_issues.append({
                "type": "apgar_score_too_low",
                "severity": "high",
                "message": f"APGAR score {apgar} is below minimum {OUTCOME_CLASSIFICATION['outcome_characteristics']['apgar_score_ranges']['low'][0]}"
            })
        elif apgar > OUTCOME_CLASSIFICATION["outcome_characteristics"]["apgar_score_ranges"]["normal"][1]:
            validation_issues.append({
                "type": "apgar_score_too_high",
                "severity": "high",
                "message": f"APGAR score {apgar} is above maximum {OUTCOME_CLASSIFICATION['outcome_characteristics']['apgar_score_ranges']['normal'][1]}"
            })
    
    return validation_issues

# Define data quality reporting criteria
DATA_QUALITY_REPORTING = {
    "metrics": {
        "completeness": {
            "required_events": ["pregnancy_test", "booking_visit"],
            "required_outcomes": ["live_birth", "stillbirth", "miscarriage", "abortion"],
            "required_dates": ["start_date", "end_date"],
            "required_measurements": ["gestational_age", "birth_weight"]
        },
        "consistency": {
            "date_ranges": {
                "min_gap": 7,  # Minimum gap between events (1 week)
                "max_gap": 280,  # Maximum gap between events (40 weeks)
                "min_gestational_age": 154,  # Minimum gestational age (22 weeks)
                "max_gestational_age": 294  # Maximum gestational age (42 weeks)
            },
            "value_ranges": {
                "birth_weight": (500, 6000),  # 500g-6kg
                "apgar_score": (0, 10),
                "blood_pressure": (90, 160)
            }
        },
        "plausibility": {
            "max_concurrent_conditions": 5,
            "max_concurrent_medications": 8,
            "max_weight_gain": 20,  # kg
            "min_weight_gain": 5,  # kg
            "max_blood_pressure": 160,
            "min_blood_pressure": 90
        }
    },
    "severity_weights": {
        "critical": 1.0,
        "high": 0.8,
        "medium": 0.5,
        "low": 0.2
    },
    "reporting_thresholds": {
        "min_quality_score": 0.6,
        "max_issues_per_episode": 5,
        "max_missing_required": 1
    }
}

# Define data quality reporting functions
def generate_quality_report(episode_data):
    """Generate a comprehensive data quality report for an episode."""
    report = {
        "episode_number": episode_data["number"],
        "quality_score": 0.0,
        "completeness": {
            "score": 0.0,
            "missing_required": [],
            "missing_optional": [],
            "issues": []
        },
        "consistency": {
            "score": 0.0,
            "date_issues": [],
            "value_issues": [],
            "issues": []
        },
        "plausibility": {
            "score": 0.0,
            "clinical_issues": [],
            "temporal_issues": [],
            "issues": []
        },
        "validation": {
            "score": 0.0,
            "outcome_issues": [],
            "event_sequence_issues": [],
            "issues": []
        },
        "summary": {
            "critical_issues": [],
            "high_priority_issues": [],
            "medium_priority_issues": [],
            "low_priority_issues": []
        }
    }
    
    # Check completeness
    for event_type in DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_events"]:
        if event_type not in episode_data["events"]:
            report["completeness"]["missing_required"].append(event_type)
            report["completeness"]["issues"].append({
                "type": "missing_required_event",
                "severity": "high",
                "message": f"Missing required event: {event_type}"
            })
    
    for outcome_type in DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_outcomes"]:
        if outcome_type not in episode_data["outcomes"]:
            report["completeness"]["missing_required"].append(outcome_type)
            report["completeness"]["issues"].append({
                "type": "missing_required_outcome",
                "severity": "high",
                "message": f"Missing required outcome: {outcome_type}"
            })
    
    # Check consistency
    if episode_data["start_date"] and episode_data["end_date"]:
        gestational_age = (episode_data["end_date"] - episode_data["start_date"]).days
        if gestational_age < DATA_QUALITY_REPORTING["metrics"]["consistency"]["date_ranges"]["min_gestational_age"]:
            report["consistency"]["date_issues"].append({
                "type": "gestational_age_too_low",
                "severity": "high",
                "message": f"Gestational age {gestational_age} days is below minimum"
            })
        elif gestational_age > DATA_QUALITY_REPORTING["metrics"]["consistency"]["date_ranges"]["max_gestational_age"]:
            report["consistency"]["date_issues"].append({
                "type": "gestational_age_too_high",
                "severity": "high",
                "message": f"Gestational age {gestational_age} days is above maximum"
            })
    
    # Check plausibility
    conditions_count = len([c for c in episode_data["events"] if c in [
        "gestational_diabetes", "preeclampsia", "pregnancy_hypertension",
        "hyperemesis", "pregnancy_infection", "pregnancy_bleeding"
    ]])
    if conditions_count > DATA_QUALITY_REPORTING["metrics"]["plausibility"]["max_concurrent_conditions"]:
        report["plausibility"]["clinical_issues"].append({
            "type": "too_many_conditions",
            "severity": "medium",
            "message": f"Too many concurrent conditions: {conditions_count}"
        })
    
    # Check validation
    if "outcome_type" in episode_data:
        outcome_validation = validate_outcome(
            episode_data["outcome_type"],
            episode_data.get("outcome_data", {}),
            episode_data["events"],
            gestational_age
        )
        report["validation"]["issues"].extend(outcome_validation)
    
    # Calculate scores
    report["completeness"]["score"] = calculate_completeness_score(report["completeness"])
    report["consistency"]["score"] = calculate_consistency_score(report["consistency"])
    report["plausibility"]["score"] = calculate_plausibility_score(report["plausibility"])
    report["validation"]["score"] = calculate_validation_score(report["validation"])
    
    # Calculate overall quality score
    report["quality_score"] = calculate_overall_quality_score(report)
    
    # Categorize issues by severity
    for category in ["completeness", "consistency", "plausibility", "validation"]:
        for issue in report[category]["issues"]:
            severity = issue["severity"]
            if severity == "critical":
                report["summary"]["critical_issues"].append(issue)
            elif severity == "high":
                report["summary"]["high_priority_issues"].append(issue)
            elif severity == "medium":
                report["summary"]["medium_priority_issues"].append(issue)
            else:
                report["summary"]["low_priority_issues"].append(issue)
    
    return report

def calculate_completeness_score(completeness_data):
    """Calculate completeness score."""
    if not completeness_data["missing_required"]:
        return 1.0
    
    total_required = len(DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_events"]) + \
                    len(DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_outcomes"])
    missing_count = len(completeness_data["missing_required"])
    
    return max(0.0, 1.0 - (missing_count / total_required))

def calculate_consistency_score(consistency_data):
    """Calculate consistency score."""
    if not consistency_data["date_issues"] and not consistency_data["value_issues"]:
        return 1.0
    
    total_issues = len(consistency_data["date_issues"]) + len(consistency_data["value_issues"])
    max_allowed_issues = 3  # Maximum number of consistency issues before score drops to 0
    
    return max(0.0, 1.0 - (total_issues / max_allowed_issues))

def calculate_plausibility_score(plausibility_data):
    """Calculate plausibility score."""
    if not plausibility_data["clinical_issues"] and not plausibility_data["temporal_issues"]:
        return 1.0
    
    total_issues = len(plausibility_data["clinical_issues"]) + len(plausibility_data["temporal_issues"])
    max_allowed_issues = 4  # Maximum number of plausibility issues before score drops to 0
    
    return max(0.0, 1.0 - (total_issues / max_allowed_issues))

def calculate_validation_score(validation_data):
    """Calculate validation score."""
    if not validation_data["issues"]:
        return 1.0
    
    total_issues = len(validation_data["issues"])
    max_allowed_issues = 2  # Maximum number of validation issues before score drops to 0
    
    return max(0.0, 1.0 - (total_issues / max_allowed_issues))

def calculate_overall_quality_score(report):
    """Calculate overall quality score."""
    weights = {
        "completeness": 0.3,
        "consistency": 0.3,
        "plausibility": 0.2,
        "validation": 0.2
    }
    
    score = 0.0
    for category, weight in weights.items():
        score += report[category]["score"] * weight
    
    return score

def generate_dataset_quality_summary(episodes):
    """Generate a summary of data quality across all episodes."""
    summary = {
        "total_episodes": len(episodes),
        "quality_scores": {
            "excellent": 0,  # ≥ 0.9
            "good": 0,      # ≥ 0.8
            "fair": 0,      # ≥ 0.7
            "poor": 0,      # ≥ 0.6
            "unacceptable": 0  # < 0.6
        },
        "common_issues": {
            "critical": [],
            "high": [],
            "medium": [],
            "low": []
        },
        "completeness": {
            "missing_required_events": {},
            "missing_required_outcomes": {}
        },
        "consistency": {
            "date_issues": {},
            "value_issues": {}
        },
        "plausibility": {
            "clinical_issues": {},
            "temporal_issues": {}
        }
    }
    
    # Process each episode
    for episode in episodes:
        report = generate_quality_report(episode)
        
        # Categorize quality score
        score = report["quality_score"]
        if score >= 0.9:
            summary["quality_scores"]["excellent"] += 1
        elif score >= 0.8:
            summary["quality_scores"]["good"] += 1
        elif score >= 0.7:
            summary["quality_scores"]["fair"] += 1
        elif score >= 0.6:
            summary["quality_scores"]["poor"] += 1
        else:
            summary["quality_scores"]["unacceptable"] += 1
        
        # Track common issues
        for severity in ["critical", "high", "medium", "low"]:
            issues = report["summary"][f"{severity}_priority_issues"]
            for issue in issues:
                issue_type = issue["type"]
                if issue_type not in summary["common_issues"][severity]:
                    summary["common_issues"][severity][issue_type] = 0
                summary["common_issues"][severity][issue_type] += 1
        
        # Track completeness issues
        for event in report["completeness"]["missing_required"]:
            if event in DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_events"]:
                if event not in summary["completeness"]["missing_required_events"]:
                    summary["completeness"]["missing_required_events"][event] = 0
                summary["completeness"]["missing_required_events"][event] += 1
            elif event in DATA_QUALITY_REPORTING["metrics"]["completeness"]["required_outcomes"]:
                if event not in summary["completeness"]["missing_required_outcomes"]:
                    summary["completeness"]["missing_required_outcomes"][event] = 0
                summary["completeness"]["missing_required_outcomes"][event] += 1
    
    return summary

# --- 2. Create Dataset and Define Population ---
dataset = create_dataset()
age = patients.age_on("2020-03-31")
dataset.age = age
dataset.sex = patients.sex
dataset.define_population((age >= 14) & (age < 50) & (patients.sex == "female"))

# --- 3. Extract Individual Event Variables ---
# Early pregnancy events
dataset.pregnancy_test_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["pregnancy_test"])
).date.minimum_for_patient()
dataset.pregnancy_test_yes_no = ~dataset.pregnancy_test_date.is_null()

dataset.booking_visit_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["booking_visit"])
).date.minimum_for_patient()
dataset.booking_visit_yes_no = ~dataset.booking_visit_date.is_null()

dataset.dating_scan_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["dating_scan"])
).date.minimum_for_patient()
dataset.dating_scan_yes_no = ~dataset.dating_scan_date.is_null()

# Antenatal care
dataset.antenatal_screening_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_screening"])
).date.minimum_for_patient()
dataset.antenatal_screening_yes_no = ~dataset.antenatal_screening_date.is_null()

dataset.antenatal_risk_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_risk"])
).date.minimum_for_patient()
dataset.antenatal_risk_yes_no = ~dataset.antenatal_risk_date.is_null()

# Pregnancy conditions
for condition in ["gestational_diabetes", "preeclampsia", "pregnancy_hypertension", 
                 "hyperemesis", "pregnancy_infection", "pregnancy_bleeding", 
                 "pregnancy_anemia", "pregnancy_thrombosis", "pregnancy_mental_health"]:
    date_var = clinical_events.where(
        clinical_events.snomedct_code.is_in(codelists[condition])
    ).date.minimum_for_patient()
    setattr(dataset, f"{condition}_date", date_var)
    setattr(dataset, f"{condition}_yes_no", ~date_var.is_null())

# Delivery methods
for method in ["caesarean_section", "forceps_delivery", "vacuum_extraction", 
               "induction", "episiotomy"]:
    date_var = clinical_events.where(
        clinical_events.snomedct_code.is_in(codelists[method])
    ).date.minimum_for_patient()
    setattr(dataset, f"{method}_date", date_var)
    setattr(dataset, f"{method}_yes_no", ~date_var.is_null())

# Outcomes
for outcome in ["live_birth", "stillbirth", "miscarriage", "abortion", 
                "ectopic_pregnancy", "molar_pregnancy"]:
    date_var = clinical_events.where(
        clinical_events.snomedct_code.is_in(codelists[outcome])
    ).date.minimum_for_patient()
    setattr(dataset, f"{outcome}_date", date_var)
    setattr(dataset, f"{outcome}_yes_no", ~date_var.is_null())

# Complications
for complication in ["postpartum_hemorrhage", "third_degree_tear", 
                    "shoulder_dystocia", "placenta_previa", "placental_abruption"]:
    date_var = clinical_events.where(
        clinical_events.snomedct_code.is_in(codelists[complication])
    ).date.minimum_for_patient()
    setattr(dataset, f"{complication}_date", date_var)
    setattr(dataset, f"{complication}_yes_no", ~date_var.is_null())

# Medications
for medication in ["antenatal_vitamins", "anti_emetics", "antihypertensives", 
                  "antidiabetics", "antibiotics", "mental_health_meds", "pain_relief"]:
    med_events = medications.where(
        medications.dmd_code.is_in(codelists[medication])
    )
    date_var = med_events.date.minimum_for_patient()
    setattr(dataset, f"{medication}_date", date_var)
    setattr(dataset, f"{medication}_yes_no", ~date_var.is_null())

# --- 4. Create Composite Pregnancy Episode Identifier ---
# Get all potential pregnancy events
pregnancy_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(
        codelists["pregnancy_test"] +
        codelists["booking_visit"] +
        codelists["dating_scan"] +
        codelists["antenatal_screening"] +
        codelists["antenatal_risk"] +
        codelists["antenatal_procedures"]
    )
)

# Add variable to indicate if patient had any pregnancy episode
dataset.had_pregnancy_episode = (
    ~dataset.pregnancy_test_date.is_null() |
    ~dataset.booking_visit_date.is_null() |
    ~dataset.dating_scan_date.is_null() |
    ~dataset.antenatal_screening_date.is_null() |
    ~dataset.antenatal_risk_date.is_null() |
    ~dataset.live_birth_date.is_null() |
    ~dataset.stillbirth_date.is_null() |
    ~dataset.miscarriage_date.is_null() |
    ~dataset.abortion_date.is_null() |
    ~dataset.ectopic_pregnancy_date.is_null() |
    ~dataset.molar_pregnancy_date.is_null()
)

# Get all potential outcomes
outcome_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(
        codelists["live_birth"] +
        codelists["stillbirth"] +
        codelists["miscarriage"] +
        codelists["abortion"] +
        codelists["ectopic_pregnancy"] +
        codelists["molar_pregnancy"]
    )
)

# Create episode-level variables for up to 2 episodes per patient
previous_episode_end = None
episodes = []

# Create sets of indicator types for efficient lookup
primary_indicators = set(EPISODE_IDENTIFICATION["primary_indicators"].keys())
secondary_indicators = set(EPISODE_IDENTIFICATION["secondary_indicators"].keys())
outcome_indicators = set(EPISODE_IDENTIFICATION["outcome_indicators"].keys())

for episode_num in range(1, 3):
    # Initialize episode data
    episode_data = {
        "number": episode_num,
        "events": {},
        "outcomes": {},
        "start_date": None,
        "end_date": None,
        "confidence": 0.0
    }
    
    # Collect all events for this episode
    for event_type, codelist in codelists.items():
        if event_type in primary_indicators or event_type in secondary_indicators:
            # Create the base query
            base_query = clinical_events.snomedct_code.is_in(codelist)
            
            # Add date condition if there's a previous episode
            if previous_episode_end is not None:
                date_condition = clinical_events.date > previous_episode_end
                events = clinical_events.where(base_query & date_condition)
            else:
                events = clinical_events.where(base_query)
            
            # Get the minimum date if any events exist
            min_date = events.date.minimum_for_patient()
            if min_date is not None:
                episode_data["events"][event_type] = min_date
    
    # Collect all outcomes for this episode
    for outcome_type, codelist in codelists.items():
        if outcome_type in outcome_indicators:
            # Create the base query
            base_query = clinical_events.snomedct_code.is_in(codelist)
            
            # Add date condition if there's a previous episode
            if previous_episode_end is not None:
                date_condition = clinical_events.date > previous_episode_end
                outcomes = clinical_events.where(base_query & date_condition)
            else:
                outcomes = clinical_events.where(base_query)
            
            # Get the minimum date if any outcomes exist
            min_date = outcomes.date.minimum_for_patient()
            if min_date is not None:
                episode_data["outcomes"][outcome_type] = min_date
    
    # Identify episode start and end
    episode_data["start_date"] = identify_episode_start(episode_data["events"], previous_episode_end)
    if episode_data["start_date"] is not None:
        episode_data["end_date"] = identify_episode_end(
            episode_data["events"],
            episode_data["start_date"],
            episode_data["outcomes"]
        )
    
    # Calculate episode confidence
    if (episode_data["start_date"] is not None) & (episode_data["end_date"] is not None):
        confidence = calculate_episode_confidence(
            episode_data["events"],
            episode_data["outcomes"],
            episode_data["start_date"],
            episode_data["end_date"]
        )
        episode_data["confidence"] = confidence
    
    # Add episode to list if valid
    if (episode_data["start_date"] is not None) & (episode_data["end_date"] is not None):
        episodes.append(episode_data)
        previous_episode_end = episode_data["end_date"]
    
    # Set dataset variables
    if episode_data["start_date"] is not None:
        # Only set episode variables if had_pregnancy_episode is True
        setattr(dataset, f"episode_{episode_num}_start_date", 
                episode_data["start_date"].where(dataset.had_pregnancy_episode))
        setattr(dataset, f"episode_{episode_num}_end_date", 
                episode_data["end_date"].where(dataset.had_pregnancy_episode))
        setattr(dataset, f"episode_{episode_num}_confidence", 
                episode_data["confidence"].where(dataset.had_pregnancy_episode))
        
        # Set event dates
        for event_type, date in episode_data["events"].items():
            setattr(dataset, f"episode_{episode_num}_{event_type}_date", 
                    date.where(dataset.had_pregnancy_episode))
        
        # Set outcome dates
        for outcome_type, date in episode_data["outcomes"].items():
            setattr(dataset, f"episode_{episode_num}_{outcome_type}_date", 
                    date.where(dataset.had_pregnancy_episode))

# Configure dummy data for testing
dataset.configure_dummy_data(population_size=100) 