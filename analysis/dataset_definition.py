from ehrql import create_dataset, codelist_from_csv, minimum_of, maximum_of, when
from ehrql.tables.core import patients, clinical_events, medications, practice_registrations
from datetime import date, timedelta

# Create the dataset
dataset = create_dataset()

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

# --- 2. Create Dataset and Define Population ---
dataset.age = patients.age_on("2020-03-31")
dataset.sex = patients.sex
dataset.define_population((dataset.age >= 14) & (dataset.age < 50) & (dataset.sex == "female"))

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

# Configure dummy data for testing
dataset.configure_dummy_data(
    population_size=100
) 