from ehrql import create_dataset, codelist_from_csv, minimum_of
from ehrql.tables.core import patients, clinical_events
from datetime import date

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

# --- 4. Key Pregnancy Dates and Intervals ---
# Earliest antenatal event (across all antenatal types)
from ehrql import minimum_of
antenatal_dates = [
    dataset.antenatal_screening_first_date,
    dataset.antenatal_risk_first_date,
    dataset.antenatal_procedures_first_date,
]
dataset.earliest_antenatal_date = minimum_of(*antenatal_dates)

# Earliest pregnancy outcome (live birth or stillbirth)
dataset.first_live_birth_date = dataset.live_birth_first_date
dataset.first_stillbirth_date = dataset.stillbirth_first_date
dataset.pregnancy_outcome_date = minimum_of(dataset.first_live_birth_date, dataset.first_stillbirth_date)

# Time from first antenatal to outcome
dataset.time_antenatal_to_outcome = (dataset.pregnancy_outcome_date - dataset.earliest_antenatal_date).days

# --- 5. Flags for Missing/Inconsistent Data ---
dataset.has_antenatal = dataset.antenatal_screening_flag | dataset.antenatal_risk_flag | dataset.antenatal_procedures_flag
dataset.has_outcome = dataset.live_birth_flag | dataset.stillbirth_flag
dataset.antenatal_no_outcome = dataset.has_antenatal & (~dataset.has_outcome)
dataset.outcome_no_antenatal = dataset.has_outcome & (~dataset.has_antenatal)

# --- 6. Export All Event Dates for Post-Processing ---
# (All *_first_date, *_last_date, and counts are available for export)

# --- 7. Configure Dummy Data for Testing ---
dataset.configure_dummy_data(population_size=2000)