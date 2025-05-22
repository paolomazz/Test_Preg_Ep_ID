from ehrql import create_dataset, codelist_from_csv, minimum_of
from ehrql.tables.core import patients, clinical_events, medications, practice_registrations
from datetime import date, timedelta

# --- 1. Load Pregnancy-Related Codelists ---
codelist_files = {
    # Core pregnancy identification codelists
    "pregnancy_test": "codelists/local/A1_pregnancy_test.csv",
    "booking_visit": "codelists/local/A2_booking_visit.csv",
    "dating_scan": "codelists/local/A3_dating_scan.csv",
    "antenatal_screening": "codelists/local/A4_antenatal_screening.csv",
    "antenatal_risk": "codelists/local/A5_risk_assessment.csv",
    "antenatal_procedures": "codelists/local/A6_antenatal_procedures.csv",
    
    # Pregnancy outcomes
    "live_birth": "codelists/local/B1_live_birth.csv",
    "stillbirth": "codelists/local/B2_stillbirth.csv",
    "miscarriage": "codelists/local/B3_miscarriage.csv",
    "abortion": "codelists/local/B4_abortion.csv",
    "ectopic_pregnancy": "codelists/local/B5_ectopic_pregnancy.csv",
    "molar_pregnancy": "codelists/local/B6_molar_pregnancy.csv",
    
    # Delivery methods
    "caesarean_section": "codelists/local/C1_caesarean_section.csv",
    "forceps_delivery": "codelists/local/C2_forceps_delivery.csv",
    "vacuum_extraction": "codelists/local/C3_vacuum_extraction.csv",
    "induction": "codelists/local/C4_induction.csv",
    "episiotomy": "codelists/local/C5_episiotomy.csv",
    
    # Pregnancy conditions
    "gestational_diabetes": "codelists/local/D1_gestational_diabetes.csv",
    "preeclampsia": "codelists/local/D2_preeclampsia.csv",
    "pregnancy_hypertension": "codelists/local/D3_pregnancy_hypertension.csv",
    "hyperemesis": "codelists/local/D4_hyperemesis.csv",
    "pregnancy_infection": "codelists/local/D5_pregnancy_infection.csv",
    "pregnancy_bleeding": "codelists/local/D6_pregnancy_bleeding.csv",
    "pregnancy_anemia": "codelists/local/D7_pregnancy_anemia.csv",
    "pregnancy_thrombosis": "codelists/local/D8_pregnancy_thrombosis.csv",
    "pregnancy_mental_health": "codelists/local/D9_pregnancy_mental_health.csv",
    
    # Pregnancy medications
    "antenatal_vitamins": "codelists/local/E1_antenatal_vitamins.csv",
    "anti_emetics": "codelists/local/E2_anti_emetics.csv",
    "antihypertensives": "codelists/local/E3_antihypertensives.csv",
    "antidiabetics": "codelists/local/E4_antidiabetics.csv",
    "antibiotics": "codelists/local/E5_antibiotics.csv",
    "mental_health_meds": "codelists/local/E6_mental_health_meds.csv",
    "pain_relief": "codelists/local/E7_pain_relief.csv",
    
    # Complications
    "postpartum_hemorrhage": "codelists/local/F1_postpartum_hemorrhage.csv",
    "third_degree_tear": "codelists/local/F2_third_degree_tear.csv",
    "shoulder_dystocia": "codelists/local/F3_shoulder_dystocia.csv",
    "placenta_previa": "codelists/local/F4_placenta_previa.csv",
    "placental_abruption": "codelists/local/F5_placental_abruption.csv",
}

codelists = {k: codelist_from_csv(v, column="code") for k, v in codelist_files.items()}

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

dataset.booking_visit_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["booking_visit"])
).date.minimum_for_patient()

dataset.dating_scan_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["dating_scan"])
).date.minimum_for_patient()

# Antenatal care
dataset.antenatal_screening_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_screening"])
).date.minimum_for_patient()

dataset.antenatal_risk_date = clinical_events.where(
    clinical_events.snomedct_code.is_in(codelists["antenatal_risk"])
).date.minimum_for_patient()

# Pregnancy conditions
for condition in ["gestational_diabetes", "preeclampsia", "pregnancy_hypertension", 
                 "hyperemesis", "pregnancy_infection", "pregnancy_bleeding", 
                 "pregnancy_anemia", "pregnancy_thrombosis", "pregnancy_mental_health"]:
    setattr(dataset, f"{condition}_date", 
            clinical_events.where(
                clinical_events.snomedct_code.is_in(codelists[condition])
            ).date.minimum_for_patient())

# Delivery methods
for method in ["caesarean_section", "forceps_delivery", "vacuum_extraction", 
               "induction", "episiotomy"]:
    setattr(dataset, f"{method}_date", 
            clinical_events.where(
                clinical_events.snomedct_code.is_in(codelists[method])
            ).date.minimum_for_patient())

# Outcomes
for outcome in ["live_birth", "stillbirth", "miscarriage", "abortion", 
                "ectopic_pregnancy", "molar_pregnancy"]:
    setattr(dataset, f"{outcome}_date", 
            clinical_events.where(
                clinical_events.snomedct_code.is_in(codelists[outcome])
            ).date.minimum_for_patient())

# Complications
for complication in ["postpartum_hemorrhage", "third_degree_tear", 
                    "shoulder_dystocia", "placenta_previa", "placental_abruption"]:
    setattr(dataset, f"{complication}_date", 
            clinical_events.where(
                clinical_events.snomedct_code.is_in(codelists[complication])
            ).date.minimum_for_patient())

# Medications
for medication in ["antenatal_vitamins", "anti_emetics", "antihypertensives", 
                  "antidiabetics", "antibiotics", "mental_health_meds", "pain_relief"]:
    med_events = medications.where(
        medications.dmd_code.is_in(codelists[medication])
    )
    setattr(dataset, f"{medication}_date", med_events.date.minimum_for_patient())
    setattr(dataset, f"{medication}_dose", med_events.quantity)
    setattr(dataset, f"{medication}_unit", med_events.unit)

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

# Create episode-level variables for up to 5 episodes per patient
for episode_num in range(1, 6):
    # For first episode, use earliest pregnancy event
    if episode_num == 1:
        episode_start = pregnancy_events.date.minimum_for_patient()
    else:
        # For subsequent episodes, find first event after previous episode's end
        prev_episode_end = getattr(dataset, f"episode_{episode_num-1}_end_date")
        min_gap_date = prev_episode_end + timedelta(days=EVENT_WINDOWS["min_episode_gap"])
        later_events = pregnancy_events.where(
            pregnancy_events.date > min_gap_date
        )
        episode_start = later_events.date.minimum_for_patient()
    
    setattr(dataset, f"episode_{episode_num}_start_date", episode_start)
    
    # Find first outcome after this episode's start
    episode_outcomes = {}
    for outcome_type, outcome_codelist in codelists.items():
        if outcome_type in ["live_birth", "stillbirth", "miscarriage", "abortion", 
                          "ectopic_pregnancy", "molar_pregnancy"]:
            outcomes = clinical_events.where(
                (clinical_events.snomedct_code.is_in(outcome_codelist)) &
                (clinical_events.date > episode_start)
            )
            episode_outcomes[outcome_type] = outcomes.date.minimum_for_patient()
    
    # Determine the earliest outcome and its type
    earliest_outcome_date = None
    earliest_outcome_type = None
    earliest_outcome_confidence = 0.0
    
    for outcome_type, outcome_date in episode_outcomes.items():
        if outcome_date is not None:
            # Calculate gestational age for this outcome
            gestational_age = (outcome_date - episode_start).days
            
            # Get outcome-specific criteria
            criteria = OUTCOME_SPECIFIC_CRITERIA[outcome_type]
            
            # Calculate confidence score for this outcome
            outcome_confidence = criteria["weight"]
            
            # Check required events
            for required_event in criteria["required_events"]:
                event_date = getattr(dataset, f"{required_event}_date")
                if event_date is not None and episode_start <= event_date <= outcome_date:
                    outcome_confidence += EVENT_SEQUENCE_WEIGHTS[required_event]
            
            # Validate gestational age
            if criteria["min_gestational_age"] <= gestational_age <= criteria["max_gestational_age"]:
                outcome_confidence += 0.2
            
            # Check for conflicting outcomes
            for other_type, other_date in episode_outcomes.items():
                if other_type != outcome_type and other_date is not None:
                    if abs((other_date - outcome_date).days) < 7:  # Within 7 days
                        outcome_confidence -= 0.3  # Penalty for conflicting outcomes
            
            if earliest_outcome_date is None or outcome_confidence > earliest_outcome_confidence:
                earliest_outcome_date = outcome_date
                earliest_outcome_type = outcome_type
                earliest_outcome_confidence = outcome_confidence
    
    # Set episode end date and type
    if earliest_outcome_date is not None:
        setattr(dataset, f"episode_{episode_num}_end_date", earliest_outcome_date)
        setattr(dataset, f"episode_{episode_num}_outcome_type", earliest_outcome_type)
        
        # Calculate gestational age at outcome
        gestational_age = (earliest_outcome_date - episode_start).days
        setattr(dataset, f"episode_{episode_num}_gestational_age", gestational_age)
        
        # Create composite pregnancy episode identifier
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_id", 
                f"PREG_{episode_num}_{episode_start.strftime('%Y%m%d')}")
        
        # Set episode confidence and type
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_confidence", 
                min(earliest_outcome_confidence, 1.0))
        
        # Determine episode type based on confidence and gestational age
        if earliest_outcome_confidence >= 0.8 and gestational_age >= 154:  # 22 weeks
            episode_type = "confirmed"
        elif earliest_outcome_confidence >= 0.5:
            episode_type = "probable"
        else:
            episode_type = "possible"
        
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_type", episode_type)
        
        # Track identification sources
        identification_sources = []
        for event_type, weight in EVENT_SEQUENCE_WEIGHTS.items():
            event_date = getattr(dataset, f"{event_type}_date")
            if event_date is not None and episode_start <= event_date <= earliest_outcome_date:
                identification_sources.append(event_type)
        
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_identification_source", 
                identification_sources)
        
        # Track events within the episode
        episode_end = earliest_outcome_date + timedelta(days=EVENT_WINDOWS["post_outcome_window"])
        
        # Track which events occurred during this episode
        for event_type in codelists.keys():
            events = clinical_events.where(
                (clinical_events.snomedct_code.is_in(codelists[event_type])) &
                (clinical_events.date >= episode_start) &
                (clinical_events.date <= episode_end)
            )
            
            # Set binary flag for event presence
            setattr(dataset, f"episode_{episode_num}_had_{event_type}", 
                   events.count_for_patient() > 0)
            
            # Set count of events
            setattr(dataset, f"episode_{episode_num}_{event_type}_count", 
                   events.count_for_patient())
            
            # Set first and last dates
            if events.count_for_patient() > 0:
                setattr(dataset, f"episode_{episode_num}_{event_type}_first_date", 
                       events.date.minimum_for_patient())
                setattr(dataset, f"episode_{episode_num}_{event_type}_last_date", 
                       events.date.maximum_for_patient())
                
                # Calculate timing relative to outcome
                first_date = events.date.minimum_for_patient()
                last_date = events.date.maximum_for_patient()
                days_before_outcome = (earliest_outcome_date - first_date).days
                days_after_outcome = (last_date - earliest_outcome_date).days
                
                setattr(dataset, f"episode_{episode_num}_{event_type}_days_before_outcome", 
                       days_before_outcome)
                setattr(dataset, f"episode_{episode_num}_{event_type}_days_after_outcome", 
                       days_after_outcome)
                
                # Track event frequency
                if days_before_outcome > 0:
                    frequency = events.count_for_patient() / (days_before_outcome / 30)  # events per month
                    setattr(dataset, f"episode_{episode_num}_{event_type}_frequency", frequency)
            
            # For medications, track additional details
            if event_type in ["antenatal_vitamins", "anti_emetics", "antihypertensives", 
                            "antidiabetics", "antibiotics", "mental_health_meds", "pain_relief"]:
                med_events = medications.where(
                    (medications.dmd_code.is_in(codelists[event_type])) &
                    (medications.date >= episode_start) &
                    (medications.date <= episode_end)
                )
                
                # Track medication details
                setattr(dataset, f"episode_{episode_num}_{event_type}_doses", 
                       med_events.quantity)
                setattr(dataset, f"episode_{episode_num}_{event_type}_units", 
                       med_events.unit)
                
                # Track medication timing
                if med_events.count_for_patient() > 0:
                    first_med_date = med_events.date.minimum_for_patient()
                    last_med_date = med_events.date.maximum_for_patient()
                    
                    setattr(dataset, f"episode_{episode_num}_{event_type}_first_dose_date", 
                           first_med_date)
                    setattr(dataset, f"episode_{episode_num}_{event_type}_last_dose_date", 
                           last_med_date)
                    
                    # Calculate medication duration
                    med_duration = (last_med_date - first_med_date).days
                    setattr(dataset, f"episode_{episode_num}_{event_type}_duration_days", 
                           med_duration)
                    
                    # Calculate total medication quantity
                    total_quantity = med_events.quantity.sum_for_patient()
                    setattr(dataset, f"episode_{episode_num}_{event_type}_total_quantity", 
                           total_quantity)
        
        # Track event relationships and patterns
        # 1. Track conditions that occurred during the episode
        conditions_during_episode = []
        for condition in ["gestational_diabetes", "preeclampsia", "pregnancy_hypertension", 
                         "hyperemesis", "pregnancy_infection", "pregnancy_bleeding", 
                         "pregnancy_anemia", "pregnancy_thrombosis", "pregnancy_mental_health"]:
            if getattr(dataset, f"episode_{episode_num}_had_{condition}"):
                conditions_during_episode.append(condition)
        
        setattr(dataset, f"episode_{episode_num}_conditions_during_episode", 
                conditions_during_episode)
        
        # 2. Track complications that occurred during the episode
        complications_during_episode = []
        for complication in ["postpartum_hemorrhage", "third_degree_tear", 
                           "shoulder_dystocia", "placenta_previa", "placental_abruption"]:
            if getattr(dataset, f"episode_{episode_num}_had_{complication}"):
                complications_during_episode.append(complication)
        
        setattr(dataset, f"episode_{episode_num}_complications_during_episode", 
                complications_during_episode)
        
        # 3. Track medications taken during the episode
        medications_during_episode = []
        for medication in ["antenatal_vitamins", "anti_emetics", "antihypertensives", 
                          "antidiabetics", "antibiotics", "mental_health_meds", "pain_relief"]:
            if getattr(dataset, f"episode_{episode_num}_had_{medication}"):
                medications_during_episode.append(medication)
        
        setattr(dataset, f"episode_{episode_num}_medications_during_episode", 
                medications_during_episode)
        
        # 4. Track delivery methods used
        delivery_methods = []
        for method in ["caesarean_section", "forceps_delivery", "vacuum_extraction", 
                      "induction", "episiotomy"]:
            if getattr(dataset, f"episode_{episode_num}_had_{method}"):
                delivery_methods.append(method)
        
        setattr(dataset, f"episode_{episode_num}_delivery_methods", delivery_methods)
        
        # Add validation checks for event sequences
        # 1. Check for logical event ordering
        event_sequence_validations = []
        
        # Check booking visit follows pregnancy test
        if (getattr(dataset, f"episode_{episode_num}_had_pregnancy_test") and 
            getattr(dataset, f"episode_{episode_num}_had_booking_visit")):
            test_date = getattr(dataset, f"episode_{episode_num}_pregnancy_test_first_date")
            booking_date = getattr(dataset, f"episode_{episode_num}_booking_visit_first_date")
            if booking_date < test_date:
                event_sequence_validations.append("booking_visit_before_pregnancy_test")
        
        # Check dating scan follows booking visit
        if (getattr(dataset, f"episode_{episode_num}_had_booking_visit") and 
            getattr(dataset, f"episode_{episode_num}_had_dating_scan")):
            booking_date = getattr(dataset, f"episode_{episode_num}_booking_visit_first_date")
            scan_date = getattr(dataset, f"episode_{episode_num}_dating_scan_first_date")
            if scan_date < booking_date:
                event_sequence_validations.append("dating_scan_before_booking")
        
        # Check antenatal screening follows booking visit
        if (getattr(dataset, f"episode_{episode_num}_had_booking_visit") and 
            getattr(dataset, f"episode_{episode_num}_had_antenatal_screening")):
            booking_date = getattr(dataset, f"episode_{episode_num}_booking_visit_first_date")
            screening_date = getattr(dataset, f"episode_{episode_num}_antenatal_screening_first_date")
            if screening_date < booking_date:
                event_sequence_validations.append("screening_before_booking")
        
        # Check delivery methods are after booking visit
        for method in ["caesarean_section", "forceps_delivery", "vacuum_extraction", 
                      "induction", "episiotomy"]:
            if (getattr(dataset, f"episode_{episode_num}_had_{method}") and 
                getattr(dataset, f"episode_{episode_num}_had_booking_visit")):
                booking_date = getattr(dataset, f"episode_{episode_num}_booking_visit_first_date")
                method_date = getattr(dataset, f"episode_{episode_num}_{method}_first_date")
                if method_date < booking_date:
                    event_sequence_validations.append(f"{method}_before_booking")
        
        setattr(dataset, f"episode_{episode_num}_event_sequence_validations", 
                event_sequence_validations)
        
        # 2. Check for clinical plausibility
        clinical_validations = []
        
        # Check gestational age at first antenatal visit
        if getattr(dataset, f"episode_{episode_num}_had_booking_visit"):
            booking_date = getattr(dataset, f"episode_{episode_num}_booking_visit_first_date")
            gestational_age = (booking_date - episode_start).days
            if gestational_age > 84:  # More than 12 weeks
                clinical_validations.append("late_booking_visit")
        
        # Check for appropriate medication timing
        for medication in ["antihypertensives", "antidiabetics"]:
            if getattr(dataset, f"episode_{episode_num}_had_{medication}"):
                med_date = getattr(dataset, f"episode_{episode_num}_{medication}_first_date")
                gestational_age = (med_date - episode_start).days
                if gestational_age < 84:  # Before 12 weeks
                    clinical_validations.append(f"early_{medication}")
        
        # Check for appropriate outcome timing
        if earliest_outcome_type in ["live_birth", "stillbirth"]:
            if gestational_age < 154:  # Before 22 weeks
                clinical_validations.append("very_preterm_outcome")
            elif gestational_age > 294:  # After 42 weeks
                clinical_validations.append("post_term_outcome")
        
        setattr(dataset, f"episode_{episode_num}_clinical_validations", 
                clinical_validations)
        
        # 3. Check for data completeness
        completeness_validations = []
        
        # Check for missing key events
        if not getattr(dataset, f"episode_{episode_num}_had_booking_visit"):
            completeness_validations.append("missing_booking_visit")
        if not getattr(dataset, f"episode_{episode_num}_had_dating_scan"):
            completeness_validations.append("missing_dating_scan")
        if not getattr(dataset, f"episode_{episode_num}_had_antenatal_screening"):
            completeness_validations.append("missing_antenatal_screening")
        
        # Check for missing outcome information
        if earliest_outcome_type in ["live_birth", "stillbirth"]:
            if not any(getattr(dataset, f"episode_{episode_num}_had_{method}") 
                      for method in ["caesarean_section", "forceps_delivery", "vacuum_extraction"]):
                completeness_validations.append("missing_delivery_method")
        
        setattr(dataset, f"episode_{episode_num}_completeness_validations", 
                completeness_validations)
        
        # --- 5. Track Episode Relationships and Outcomes ---
        # 1. Track relationships between episodes
        if episode_num > 1:
            prev_episode_end = getattr(dataset, f"episode_{episode_num-1}_end_date")
            gap_days = (episode_start - prev_episode_end).days
            
            setattr(dataset, f"episode_{episode_num}_days_since_previous_episode", 
                   gap_days)
            
            # Check for short inter-pregnancy interval
            if gap_days < 180:  # Less than 6 months
                setattr(dataset, f"episode_{episode_num}_short_inter_pregnancy_interval", 
                       True)
            else:
                setattr(dataset, f"episode_{episode_num}_short_inter_pregnancy_interval", 
                       False)
        
        # 2. Track pregnancy outcomes
        if earliest_outcome_type in ["live_birth", "stillbirth"]:
            # Track delivery details
            delivery_methods = getattr(dataset, f"episode_{episode_num}_delivery_methods")
            setattr(dataset, f"episode_{episode_num}_delivery_method", 
                   delivery_methods[0] if delivery_methods else "unknown")
            
            # Track complications
            complications = getattr(dataset, f"episode_{episode_num}_complications_during_episode")
            setattr(dataset, f"episode_{episode_num}_delivery_complications", 
                   complications)
            
            # Track gestational age at delivery
            setattr(dataset, f"episode_{episode_num}_gestational_age_at_delivery", 
                   gestational_age)
            
            # Classify delivery timing
            if gestational_age < 259:  # Before 37 weeks
                delivery_timing = "preterm"
            elif gestational_age > 294:  # After 42 weeks
                delivery_timing = "post_term"
            else:
                delivery_timing = "term"
            
            setattr(dataset, f"episode_{episode_num}_delivery_timing", 
                   delivery_timing)
        
        # 3. Track pregnancy conditions
        conditions = getattr(dataset, f"episode_{episode_num}_conditions_during_episode")
        setattr(dataset, f"episode_{episode_num}_pregnancy_conditions", 
               conditions)
        
        # 4. Track medication patterns
        medications = getattr(dataset, f"episode_{episode_num}_medications_during_episode")
        setattr(dataset, f"episode_{episode_num}_medication_pattern", 
               medications)
        
        # 5. Calculate episode summary statistics
        setattr(dataset, f"episode_{episode_num}_total_antenatal_visits", 
               sum(1 for event in ["booking_visit", "antenatal_screening", "antenatal_risk"] 
                   if getattr(dataset, f"episode_{episode_num}_had_{event}")))
        
        setattr(dataset, f"episode_{episode_num}_total_conditions", 
               len(conditions))
        
        setattr(dataset, f"episode_{episode_num}_total_complications", 
               len(complications_during_episode))
        
        setattr(dataset, f"episode_{episode_num}_total_medications", 
               len(medications_during_episode))
        
    else:
        # If no outcome found, use maximum window
        max_window = max(PREGNANCY_WINDOWS.values())
        setattr(dataset, f"episode_{episode_num}_end_date", 
                episode_start + timedelta(days=max_window))
        setattr(dataset, f"episode_{episode_num}_outcome_type", "unknown")
        setattr(dataset, f"episode_{episode_num}_gestational_age", max_window)
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_id", 
                f"PREG_{episode_num}_{episode_start.strftime('%Y%m%d')}")
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_confidence", 0.0)
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_type", "possible")
        setattr(dataset, f"episode_{episode_num}_pregnancy_episode_identification_source", [])

# --- 6. Configure Dummy Data for Testing ---
dataset.configure_dummy_data(population_size=2000)