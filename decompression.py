from pyairtable import Api
import os
import json
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

API_KEY = os.getenv("API_KEY")
BASE_ID = os.getenv("BASE_ID")

APPLICANTS_TABLE = "Applicants"
PERSONAL_TABLE = "Personal Details"
WORK_TABLE = "Work Experience"
SALARY_TABLE = "Salary Preferences"

api = Api(API_KEY)
applicants_table = api.table(BASE_ID, APPLICANTS_TABLE)
personal_table = api.table(BASE_ID, PERSONAL_TABLE)
work_table = api.table(BASE_ID, WORK_TABLE)
salary_table = api.table(BASE_ID, SALARY_TABLE)

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
    # 3. Upsert Personal Details (one-to-one)
    # -----------------------------
    personal_fields = data.get("personal", {})
    
    existing_personal = personal_table.all(formula=f"{{Applicant}} = '{app_id}'")
    if existing_personal:
        personal_table.update(existing_personal[0]["id"], personal_fields)
    else:
        personal_fields["Applicant"] = [app_id]
        personal_table.create(personal_fields)
    
    # -----------------------------
    # 4. Upsert Work Experience (one-to-many)
    # -----------------------------
    work_list = data.get("experience", [])
    
    # Delete existing work rows
    existing_work = work_table.all(formula=f"{{Applicant}} = '{app_id}'")
    for w in existing_work:
        work_table.delete(w["id"])
    
    # Map your JSON keys to Airtable field names
    for w in work_list:
        w_mapped = {
    "Applicant": [app_id],
    "Company": w.get("company"),
    "Title": w.get("position"),   # <-- use "Title" exactly
    "Start": w.get("start_date"),
    "End": w.get("end_date")
}

        work_table.create(w_mapped)
    
    # -----------------------------
    # 5. Upsert Salary Preferences (one-to-one)
    # -----------------------------
    salary_fields = data.get("salary", {})
    existing_salary = salary_table.all(formula=f"{{Applicant}} = '{app_id}'")
    if existing_salary:
        salary_table.update(existing_salary[0]["id"], salary_fields)
    else:
        salary_fields["Applicant"] = [app_id]
        salary_table.create(salary_fields)
    
    print(f"Decompressed applicant {app_id} into child tables.")
