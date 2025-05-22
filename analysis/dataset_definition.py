from ehrql import create_dataset, codelist_from_csv, minimum_of
from ehrql.tables.core import patients, clinical_events
from datetime import date, timedelta

# --- 1. Load Subcategorized Codelists ---
codelist_files = {
    "antenatal_screening": "codelists/local/A1_antenatal_screening.csv",
    "antenatal_risk": "codelists/local/A2_risk_assessment.csv",
    "antenatal_procedures": "codelists/local/A3_antenatal_procedures.csv",
    "live_birth": "codelists/local/B1_live_birth.csv",
    "stillbirth": "codelists/local/B2_stillbirth.csv",
    "neonatal_complications": "codelists/local/B3_neonatal_complications.csv",
    "htn": "codelists/local/C1_hypertension.csv",
    "diabetes": "codelists/local/C2_diabetes.csv",
    "infection": "codelists/local/C3_infections.csv",
    "preeclampsia": "codelists/local/C4_preeclampsia.csv",
    "other_complications": "codelists/local/C5_other.csv",
    "maternal_recovery": "codelists/local/D1_maternal_recovery.csv",
    "neonatal_care": "codelists/local/D2_neonatal_care.csv",
    "mode_delivery": "codelists/local/E1_mode_of_delivery.csv",
    "delivery_complications": "codelists/local/E2_delivery_complications.csv",
    # Add more as needed
}
codelists = {k: codelist_from_csv(v, column="code") for k, v in codelist_files.items()}

# --- 2. Create Dataset and Define Population ---
dataset = create_dataset()
age = patients.age_on("2020-03-31")
dataset.age = age
dataset.sex = patients.sex
dataset.define_population((age >= 14) & (age < 50) & (patients.sex == "female"))

# --- 3. Extract and Summarize Events ---
event_types = {
    "antenatal_screening": codelists["antenatal_screening"],
    "antenatal_risk": codelists["antenatal_risk"],
    "antenatal_procedures": codelists["antenatal_procedures"],
    "live_birth": codelists["live_birth"],
    "stillbirth": codelists["stillbirth"],
    "neonatal_complications": codelists["neonatal_complications"],
    "htn": codelists["htn"],
    "diabetes": codelists["diabetes"],
    "infection": codelists["infection"],
    "preeclampsia": codelists["preeclampsia"],
    "other_complications": codelists["other_complications"],
    "maternal_recovery": codelists["maternal_recovery"],
    "neonatal_care": codelists["neonatal_care"],
    "mode_delivery": codelists["mode_delivery"],
    "delivery_complications": codelists["delivery_complications"],
    # Add more as needed
}

# For each event type, extract first/last date, count, and flag
for event_name, codelist in event_types.items():
    events = clinical_events.where(clinical_events.snomedct_code.is_in(codelist))
    setattr(dataset, f"{event_name}_first_date", events.date.minimum_for_patient())
    setattr(dataset, f"{event_name}_last_date", events.date.maximum_for_patient())
    setattr(dataset, f"{event_name}_count", events.count_for_patient())
    setattr(dataset, f"{event_name}_flag", events.exists_for_patient())

# --- 4. Pregnancy Episode Identification ---
# Define pregnancy episode window (typical pregnancy duration is ~280 days)
PREGNANCY_WINDOW = 280  # days

# Get all antenatal and outcome dates
antenatal_codes = (
    codelists["antenatal_screening"] +
    codelists["antenatal_risk"] +
    codelists["antenatal_procedures"]
)

outcome_codes = (
    codelists["live_birth"] +
    codelists["stillbirth"]
)

# Get all events for each type
antenatal_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(antenatal_codes)
)

outcome_events = clinical_events.where(
    clinical_events.snomedct_code.is_in(outcome_codes)
)

# Create episode-level variables for up to 5 episodes per patient
for episode_num in range(1, 6):
    # For first episode, use earliest antenatal date
    if episode_num == 1:
        episode_start = antenatal_events.date.minimum_for_patient()
    else:
        # For subsequent episodes, find first antenatal date after previous episode's end
        prev_episode_end = getattr(dataset, f"episode_{episode_num-1}_end_date")
        later_antenatal = antenatal_events.where(
            antenatal_events.date > prev_episode_end
        )
        episode_start = later_antenatal.date.minimum_for_patient()
    
    setattr(dataset, f"episode_{episode_num}_start_date", episode_start)
    
    # Find first outcome after this episode's start
    episode_outcomes = outcome_events.where(
        outcome_events.date > episode_start
    )
    setattr(dataset, f"episode_{episode_num}_end_date",
            episode_outcomes.date.minimum_for_patient())
    
    # Track events within this episode
    episode_start = getattr(dataset, f"episode_{episode_num}_start_date")
    episode_end = getattr(dataset, f"episode_{episode_num}_end_date")
    
    for event_name, codelist in event_types.items():
        events = clinical_events.where(
            (clinical_events.snomedct_code.is_in(codelist)) &
            (clinical_events.date >= episode_start) &
            (clinical_events.date <= episode_end)
        )
        
        setattr(dataset, f"episode_{episode_num}_{event_name}_count", 
                events.count_for_patient())
        setattr(dataset, f"episode_{episode_num}_{event_name}_flag", 
                events.exists_for_patient())
        setattr(dataset, f"episode_{episode_num}_{event_name}_first_date", 
                events.date.minimum_for_patient())
        setattr(dataset, f"episode_{episode_num}_{event_name}_last_date", 
                events.date.maximum_for_patient())

# --- 5. Configure Dummy Data for Testing ---
dataset.configure_dummy_data(population_size=2000)