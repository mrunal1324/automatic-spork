from pyairtable import Api
import json
from collections import defaultdict
from dotenv import load_dotenv
import os

# -----------------------------
# 1. Setup
# -----------------------------
load_dotenv()
TOKEN = os.getenv("API_KEY")
BASE_ID = os.getenv("BASE_ID")

# Table names
APPLICANTS_TABLE = "Applicants"
CHILD_TABLES = ["Personal Details", "Work Experience", "Salary Preferences"]

# Initialize API with token
api = Api(TOKEN)

# Initialize tables
applicants_table = api.table(BASE_ID, APPLICANTS_TABLE)
personal_table = api.table(BASE_ID, "Personal Details")
work_table = api.table(BASE_ID, "Work Experience")
salary_table = api.table(BASE_ID, "Salary Preferences")

# -----------------------------
# 2. Fetch all records
# -----------------------------
personal_records = personal_table.all()
work_records = work_table.all()
salary_records = salary_table.all()

# -----------------------------
# 3. Group child records by Applicant ID
# -----------------------------
personal_by_applicant = {}
work_by_applicant = defaultdict(list)
salary_by_applicant = {}

for rec in personal_records:
    linked_applicant = rec["fields"].get("Applicant", [None])[0]
    if linked_applicant:
        personal_by_applicant[linked_applicant] = rec["fields"]

for rec in work_records:
    linked_applicant = rec["fields"].get("Applicant", [None])[0]
    if linked_applicant:
        work_by_applicant[linked_applicant].append({
            "company": rec["fields"].get("Company"),
            "title": rec["fields"].get("Title"),
            "start": rec["fields"].get("Start"),
            "end": rec["fields"].get("End"),
            "technologies": rec["fields"].get("Technologies")
        })

for rec in salary_records:
    linked_applicant = rec["fields"].get("Applicant", [None])[0]
    if linked_applicant:
        salary_by_applicant[linked_applicant] = rec["fields"]

# -----------------------------
# 4. Build compressed JSON per applicant
# -----------------------------
applicant_records = applicants_table.all()

for app_rec in applicant_records:
    app_id = app_rec["id"]
    
    compressed_json = {
        "personal": personal_by_applicant.get(app_id, {}),
        "experience": work_by_applicant.get(app_id, []),
        "salary": salary_by_applicant.get(app_id, {})
    }
    
    json_str = json.dumps(compressed_json)
    
    # Update Applicants table
    applicants_table.update(app_id, {"Compressed JSON": json_str})
    print(f"Updated applicant {app_id} with compressed JSON.")
