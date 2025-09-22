from pyairtable import Table
import os
import json
from pyairtable import Api
from dotenv import load_dotenv

# -----------------------------
# 1. Setup
# -----------------------------
load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_ID = os.getenv("BASE_ID")

APPLICANTS_TABLE = "Applicants"
SHORTLIST_TABLE = "Shortlisted Leads"

api = Api(API_KEY)
applicants_table = api.table(BASE_ID, APPLICANTS_TABLE)
shortlist_table = api.table(BASE_ID, SHORTLIST_TABLE)


# Tier-1 companies
TIER1_COMPANIES = ["Google", "Meta", "OpenAI"]

# Allowed locations
ALLOWED_LOCATIONS = ["US", "Canada", "UK", "Germany", "India"]

# -----------------------------
# 2. Loop through all applicants
# -----------------------------
applicant_records = applicants_table.all()

for app_rec in applicant_records:
    app_id = app_rec["id"]
    json_str = app_rec["fields"].get("Compressed JSON")
    if not json_str:
        continue  # skip if no JSON
    
    data = json.loads(json_str)
    
    # -----------------------------
    # 3. Evaluate rules
    # -----------------------------
    experience_list = data.get("experience", [])
    salary_data = data.get("salary", {})
    personal_data = data.get("personal", {})

    # Rule 1: Experience ≥ 4 years OR worked at Tier-1
    total_years = 0
    tier1 = False
    for exp in experience_list:
        start = exp.get("start")
        end = exp.get("end")
        company = exp.get("company", "")
        if start and end:
            try:
                total_years += int(end) - int(start)
            except:
                pass
        if company in TIER1_COMPANIES:
            tier1 = True

    experience_ok = total_years >= 4 or tier1

    # Rule 2: Compensation
    rate = salary_data.get("rate", 0)
    availability = salary_data.get("availability", 0)
    compensation_ok = rate <= 100 and availability >= 20

    # Rule 3: Location
    location = personal_data.get("Location", "")
    location_ok = location in ALLOWED_LOCATIONS

    # -----------------------------
    # 4. If all criteria met, create Shortlisted Leads row
    # -----------------------------
    if experience_ok and compensation_ok and location_ok:
        score_reason = f"Experience: {total_years} yrs, Tier1: {tier1}, Rate: {rate}, Availability: {availability}, Location: {location}"
        
        # Check if already shortlisted
        existing = shortlist_table.all(formula=f"{{Linked Applicant}} = '{app_id}'")
        if not existing:
            row_data = {
                "Linked Applicant": [app_id],
                "Compressed JSON": json_str,
                "Score Reason": score_reason
            }
            shortlist_table.create(row_data)
            print(f"Applicant {app_id} shortlisted.")
