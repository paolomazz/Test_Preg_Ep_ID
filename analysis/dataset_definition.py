from ehrql import create_dataset, codelist_from_csv, minimum_of
from ehrql.tables.core import patients, clinical_events, medications, practice_registrations
from datetime import date, timedelta

# --- 1. Load Pregnancy-Related Codelists ---
codelist_files = {
    # Core pregnancy identification codelists
    "antenatal_screening": "codelists/local/A1_antenatal_screening.csv",
    "antenatal_risk": "codelists/local/A2_risk_assessment.csv",
    "antenatal_procedures": "codelists/local/A3_antenatal_procedures.csv",
    "live_birth": "codelists/local/B1_live_birth.csv",
    "stillbirth": "codelists/local/B2_stillbirth.csv",
    
    # Pregnancy-related conditions (used for episode identification)
    "gestational_diabetes": "codelists/local/C1_gestational_diabetes.csv",
    "pregnancy_hypertension": "codelists/local/C2_pregnancy_hypertension.csv",
    "preeclampsia": "codelists/local/C3_preeclampsia.csv",
    "hyperemesis": "codelists/local/C4_hyperemesis.csv",
    "pregnancy_infection": "codelists/local/C5_pregnancy_infection.csv",
    "pregnancy_bleeding": "codelists/local/C6_pregnancy_bleeding.csv",
    "pregnancy_anemia": "codelists/local/C7_pregnancy_anemia.csv",
    "pregnancy_thrombosis": "codelists/local/C8_pregnancy_thrombosis.csv",
    "pregnancy_mental_health": "codelists/local/C9_pregnancy_mental_health.csv",
    "pregnancy_thyroid": "codelists/local/C10_pregnancy_thyroid.csv",
    "pregnancy_asthma": "codelists/local/C11_pregnancy_asthma.csv",
    "pregnancy_epilepsy": "codelists/local/C12_pregnancy_epilepsy.csv",
    "pregnancy_heart_disease": "codelists/local/C13_pregnancy_heart_disease.csv",
    "pregnancy_kidney_disease": "codelists/local/C14_pregnancy_kidney_disease.csv",
    "pregnancy_liver_disease": "codelists/local/C15_pregnancy_liver_disease.csv",
    "pregnancy_autoimmune": "codelists/local/C16_pregnancy_autoimmune.csv",
    "pregnancy_obesity": "codelists/local/C17_pregnancy_obesity.csv",
    "pregnancy_smoking": "codelists/local/C18_pregnancy_smoking.csv",
    "pregnancy_alcohol": "codelists/local/C19_pregnancy_alcohol.csv",
    "pregnancy_drug_use": "codelists/local/C20_pregnancy_drug_use.csv",
    
    # Detailed pregnancy outcome codelists
    "incomplete_abortion": "codelists/local/F1_incomplete_abortion.csv",
    "threatened_abortion": "codelists/local/F2_threatened_abortion.csv",
    "complete_miscarriage": "codelists/local/F3_complete_miscarriage.csv",
    "blighted_ovum": "codelists/local/F4_blighted_ovum.csv",
    "chemical_pregnancy": "codelists/local/F5_chemical_pregnancy.csv",
    "missed_miscarriage": "codelists/local/F6_missed_miscarriage.csv",
    "inevitable_miscarriage": "codelists/local/F7_inevitable_miscarriage.csv",
    "recurrent_miscarriage": "codelists/local/F8_recurrent_miscarriage.csv",
    "ectopic_pregnancy": "codelists/local/F9_ectopic_pregnancy.csv",
    "molar_pregnancy": "codelists/local/F10_molar_pregnancy.csv",
    "early_miscarriage": "codelists/local/F11_early_miscarriage.csv",
    "late_miscarriage": "codelists/local/F12_late_miscarriage.csv",
    "missed_silent_miscarriage": "codelists/local/F13_missed_silent_miscarriage.csv",
}

codelists = {k: codelist_from_csv(v, column="code") for k, v in codelist_files.items()}

# --- 2. Create Dataset and Define Population ---
dataset = create_dataset()
age = patients.age_on("2020-03-31")
dataset.age = age
dataset.sex = patients.sex
dataset.define_population((age >= 14) & (age < 50) & (patients.sex == "female"))

# --- 3. Extract Sociodemographic Information ---
# Basic demographics (these are point-in-time values)
dataset.ethnicity = patients.ethnicity
dataset.imd_quintile = patients.imd_quintile
dataset.rural_urban = patients.rural_urban_classification

# Practice information
dataset.practice_region = practice_registrations.region
dataset.practice_urban_rural = practice_registrations.urban_rural_classification

# --- 4. Define Time Windows for Conditions and Medications ---
# Define lookback periods for pre-existing conditions
LOOKBACK_PERIODS = {
    "short_term": 90,  # 3 months
    "medium_term": 365,  # 1 year
    "long_term": 730,  # 2 years
}

# --- 5. Pregnancy Episode Identification ---
# Define pregnancy outcome windows with specific gestational age ranges
PREGNANCY_WINDOWS = {
    # Live births
    "term_birth": 280,  # 40 weeks
    "preterm_birth": 259,  # 37 weeks
    "very_preterm_birth": 224,  # 32 weeks
    
    # Miscarriages and abortions
    "chemical_pregnancy": 42,  # 6 weeks
    "early_miscarriage": 84,  # 12 weeks
    "blighted_ovum": 84,  # 12 weeks
    "incomplete_abortion": 84,  # 12 weeks
    "threatened_abortion": 84,  # 12 weeks
    "complete_miscarriage": 84,  # 12 weeks
    "missed_miscarriage": 84,  # 12 weeks
    "inevitable_miscarriage": 84,  # 12 weeks
    "missed_silent_miscarriage": 84,  # 12 weeks
    "late_miscarriage": 196,  # 28 weeks
    
    # Special cases
    "ectopic_pregnancy": 84,  # 12 weeks
    "molar_pregnancy": 84,  # 12 weeks
    "recurrent_miscarriage": 84,  # 12 weeks
    "stillbirth": 280,  # 40 weeks
}

# Define weights for different types of pregnancy indicators
PREGNANCY_INDICATOR_WEIGHTS = {
    # High confidence indicators (weight = 1.0)
    "antenatal_screening": 1.0,
    "antenatal_risk": 1.0,
    "antenatal_procedures": 1.0,
    
    # Moderate confidence indicators (weight = 0.8)
    "gestational_diabetes": 0.8,
    "preeclampsia": 0.8,
    "hyperemesis": 0.8,
    "pregnancy_hypertension": 0.8,
    
    # Lower confidence indicators (weight = 0.6)
    "pregnancy_infection": 0.6,
    "pregnancy_bleeding": 0.6,
    "pregnancy_anemia": 0.6,
    "pregnancy_thrombosis": 0.6,
    "pregnancy_mental_health": 0.6,
    "pregnancy_thyroid": 0.6,
    "pregnancy_asthma": 0.6,
    "pregnancy_epilepsy": 0.6,
    "pregnancy_heart_disease": 0.6,
    "pregnancy_kidney_disease": 0.6,
    "pregnancy_liver_disease": 0.6,
    "pregnancy_autoimmune": 0.6,
    
    # Lifestyle indicators (weight = 0.4)
    "pregnancy_obesity": 0.4,
    "pregnancy_smoking": 0.4,
    "pregnancy_alcohol": 0.4,
    "pregnancy_drug_use": 0.4
}

# Get all potential pregnancy indicator events
antenatal_codes = (
    codelists["antenatal_screening"] +
    codelists["antenatal_risk"] +
    codelists["antenatal_procedures"]
)

pregnancy_condition_codes = (
    codelists["gestational_diabetes"] +
    codelists["pregnancy_hypertension"] +
    codelists["preeclampsia"] +
    codelists["hyperemesis"] +
    codelists["pregnancy_infection"] +
    codelists["pregnancy_bleeding"] +
    codelists["pregnancy_anemia"] +
    codelists["pregnancy_thrombosis"] +
    codelists["pregnancy_mental_health"] +
    codelists["pregnancy_thyroid"] +
    codelists["pregnancy_asthma"] +
    codelists["pregnancy_epilepsy"] +
    codelists["pregnancy_heart_disease"] +
    codelists["pregnancy_kidney_disease"] +
    codelists["pregnancy_liver_disease"] +
    codelists["pregnancy_autoimmune"] +
    codelists["pregnancy_obesity"] +
    codelists["pregnancy_smoking"] +
    codelists["pregnancy_alcohol"] +
    codelists["pregnancy_drug_use"]
)

# Get all events for each type
antenatal_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(antenatal_codes)
)

pregnancy_condition_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(pregnancy_condition_codes)
)

# Create episode-level variables for up to 5 episodes per patient
for episode_num in range(1, 6):
    # For first episode, use earliest of antenatal date or pregnancy condition date
    if episode_num == 1:
        antenatal_start = antenatal_events.date.minimum_for_patient()
        condition_start = pregnancy_condition_events.date.minimum_for_patient()
        
        # Determine which source to use based on weights
        if antenatal_start is not None and condition_start is not None:
            # If both exist, use the one with higher weight
            antenatal_weight = PREGNANCY_INDICATOR_WEIGHTS["antenatal_screening"]
            condition_weight = PREGNANCY_INDICATOR_WEIGHTS["gestational_diabetes"]  # Default to moderate weight
            episode_start = antenatal_start if antenatal_weight >= condition_weight else condition_start
            setattr(dataset, f"episode_{episode_num}_identification_source", 
                   "antenatal" if antenatal_weight >= condition_weight else "condition")
        else:
            # If only one exists, use that
            episode_start = antenatal_start if antenatal_start is not None else condition_start
            setattr(dataset, f"episode_{episode_num}_identification_source", 
                   "antenatal" if antenatal_start is not None else "condition")
    else:
        # For subsequent episodes, find first event after previous episode's end
        prev_episode_end = getattr(dataset, f"episode_{episode_num-1}_end_date")
        later_antenatal = antenatal_events.where(
            antenatal_events.date > prev_episode_end
        )
        later_condition = pregnancy_condition_events.where(
            pregnancy_condition_events.date > prev_episode_end
        )
        antenatal_start = later_antenatal.date.minimum_for_patient()
        condition_start = later_condition.date.minimum_for_patient()
        
        # Determine which source to use based on weights
        if antenatal_start is not None and condition_start is not None:
            antenatal_weight = PREGNANCY_INDICATOR_WEIGHTS["antenatal_screening"]
            condition_weight = PREGNANCY_INDICATOR_WEIGHTS["gestational_diabetes"]
            episode_start = antenatal_start if antenatal_weight >= condition_weight else condition_start
            setattr(dataset, f"episode_{episode_num}_identification_source", 
                   "antenatal" if antenatal_weight >= condition_weight else "condition")
        else:
            episode_start = antenatal_start if antenatal_start is not None else condition_start
            setattr(dataset, f"episode_{episode_num}_identification_source", 
                   "antenatal" if antenatal_start is not None else "condition")
    
    setattr(dataset, f"episode_{episode_num}_start_date", episode_start)
    
    # Track which conditions were present during this episode
    for condition_name in PREGNANCY_INDICATOR_WEIGHTS.keys():
        if condition_name in codelists:
            condition_events = clinical_events.where(
                (clinical_events.snomedct_code.is_in(codelists[condition_name])) &
                (clinical_events.date >= episode_start) &
                (clinical_events.date <= getattr(dataset, f"episode_{episode_num}_end_date"))
            )
            setattr(dataset, f"episode_{episode_num}_{condition_name}_present", 
                   condition_events.count_for_patient() > 0)
            setattr(dataset, f"episode_{episode_num}_{condition_name}_count", 
                   condition_events.count_for_patient())
            setattr(dataset, f"episode_{episode_num}_{condition_name}_first_date", 
                   condition_events.date.minimum_for_patient())
    
    # Find first outcome after this episode's start
    episode_outcomes = {}
    for outcome_type, outcome_codelist in outcome_codes.items():
        outcomes = clinical_events.where(
            (clinical_events.snomedct_code.is_in(outcome_codelist)) &
            (clinical_events.date > episode_start)
        )
        episode_outcomes[outcome_type] = outcomes.date.minimum_for_patient()
    
    # Determine the earliest outcome and its type
    earliest_outcome_date = None
    earliest_outcome_type = None
    
    for outcome_type, outcome_date in episode_outcomes.items():
        if outcome_date is not None:
            if earliest_outcome_date is None or outcome_date < earliest_outcome_date:
                earliest_outcome_date = outcome_date
                earliest_outcome_type = outcome_type
    
    # Set episode end date and type
    if earliest_outcome_date is not None:
        setattr(dataset, f"episode_{episode_num}_end_date", earliest_outcome_date)
        setattr(dataset, f"episode_{episode_num}_outcome_type", earliest_outcome_type)
        
        # Calculate gestational age at outcome
        gestational_age = (earliest_outcome_date - episode_start).days
        setattr(dataset, f"episode_{episode_num}_gestational_age", gestational_age)
        
        # Flag if outcome occurred before expected window
        expected_window = PREGNANCY_WINDOWS.get(earliest_outcome_type, 280)
        setattr(dataset, f"episode_{episode_num}_premature_outcome", 
                gestational_age < expected_window)
        
        # Add specific flags for certain outcomes
        if earliest_outcome_type in ["threatened_abortion", "inevitable_miscarriage"]:
            setattr(dataset, f"episode_{episode_num}_pregnancy_at_risk", True)
        if earliest_outcome_type == "recurrent_miscarriage":
            setattr(dataset, f"episode_{episode_num}_recurrent_miscarriage", True)
            
        # Impute conception date based on gestational age
        estimated_conception = earliest_outcome_date - timedelta(days=gestational_age + 14)  # Add 2 weeks
        setattr(dataset, f"episode_{episode_num}_estimated_conception_date", estimated_conception)
        setattr(dataset, f"episode_{episode_num}_conception_date_source", "gestational_age")
        
    else:
        # If no outcome found, use maximum window
        max_window = max(PREGNANCY_WINDOWS.values())
        setattr(dataset, f"episode_{episode_num}_end_date", 
                episode_start + timedelta(days=max_window))
        setattr(dataset, f"episode_{episode_num}_outcome_type", "unknown")
        setattr(dataset, f"episode_{episode_num}_gestational_age", max_window)
        setattr(dataset, f"episode_{episode_num}_premature_outcome", False)
        setattr(dataset, f"episode_{episode_num}_pregnancy_at_risk", False)
        setattr(dataset, f"episode_{episode_num}_recurrent_miscarriage", False)
        
        # Impute conception date based on maximum window
        estimated_conception = episode_start - timedelta(days=14)  # Subtract 2 weeks from start date
        setattr(dataset, f"episode_{episode_num}_estimated_conception_date", estimated_conception)
        setattr(dataset, f"episode_{episode_num}_conception_date_source", "episode_start")

# --- 6. Add Clinical History and Medications to Each Episode ---
for episode_num in range(1, 6):
    episode_start = getattr(dataset, f"episode_{episode_num}_start_date")
    episode_end = getattr(dataset, f"episode_{episode_num}_end_date")
    
    # For each lookback period
    for period_name, days in LOOKBACK_PERIODS.items():
        lookback_start = episode_start - timedelta(days=days)
        
        # Track conditions in lookback period
        for condition_name, codelist in codelists.items():
            if condition_name in ["cardiovascular", "respiratory", "mental_health", 
                                "neurological", "endocrine", "gastrointestinal", 
                                "musculoskeletal", "skin", "cancer"]:
                # Pre-existing conditions in lookback period
                conditions = clinical_events.where(
                    (clinical_events.snomedct_code.is_in(codelist)) &
                    (clinical_events.date >= lookback_start) &
                    (clinical_events.date < episode_start)
                )
                setattr(dataset, f"episode_{episode_num}_{period_name}_{condition_name}_count",
                        conditions.count_for_patient())
                setattr(dataset, f"episode_{episode_num}_{period_name}_{condition_name}_list",
                        conditions.snomedct_code)
                
                # Current conditions during episode
                current_conditions = clinical_events.where(
                    (clinical_events.snomedct_code.is_in(codelist)) &
                    (clinical_events.date >= episode_start) &
                    (clinical_events.date <= episode_end)
                )
                setattr(dataset, f"episode_{episode_num}_current_{condition_name}_count",
                        current_conditions.count_for_patient())
                setattr(dataset, f"episode_{episode_num}_current_{condition_name}_list",
                        current_conditions.snomedct_code)
    
    # Track medications during episode
    for med_name, codelist in codelists.items():
        if med_name in ["antihypertensives", "antidiabetics", "anticoagulants", 
                       "antidepressants", "antipsychotics", "antiepileptics", 
                       "steroids", "immunosuppressants"]:
            # Medications during episode
            episode_meds = medications.where(
                (medications.dmd_code.is_in(codelist)) &
                (medications.date >= episode_start) &
                (medications.date <= episode_end)
            )
            setattr(dataset, f"episode_{episode_num}_{med_name}_count",
                    episode_meds.count_for_patient())
            setattr(dataset, f"episode_{episode_num}_{med_name}_list",
                    episode_meds.dmd_code)
            setattr(dataset, f"episode_{episode_num}_{med_name}_dates",
                    episode_meds.date)
            setattr(dataset, f"episode_{episode_num}_{med_name}_doses",
                    episode_meds.quantity)
            setattr(dataset, f"episode_{episode_num}_{med_name}_units",
                    episode_meds.unit)
            
            # Check for medication combinations
            for other_med_name, other_codelist in codelists.items():
                if other_med_name in ["antihypertensives", "antidiabetics", "anticoagulants", 
                                    "antidepressants", "antipsychotics", "antiepileptics", 
                                    "steroids", "immunosuppressants"] and other_med_name != med_name:
                    other_meds = medications.where(
                        (medications.dmd_code.is_in(other_codelist)) &
                        (medications.date >= episode_start) &
                        (medications.date <= episode_end)
                    )
                    # Check for overlapping dates
                    combination_flag = (
                        episode_meds.date.minimum_for_patient() <= other_meds.date.maximum_for_patient() &
                        episode_meds.date.maximum_for_patient() >= other_meds.date.minimum_for_patient()
                    )
                    setattr(dataset, f"episode_{episode_num}_{med_name}_with_{other_med_name}",
                            combination_flag)

# --- 7. Configure Dummy Data for Testing ---
dataset.configure_dummy_data(population_size=2000)