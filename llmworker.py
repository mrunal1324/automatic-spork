from pyairtable import Api
import json
import time
import google.generativeai as genai
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

AIRTABLE_KEY = os.getenv("API_KEY")
BASE_ID = os.getenv("BASE_ID")
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")

APPLICANTS_TABLE = "Applicants"
api = Api(AIRTABLE_KEY)
applicants_table = api.table(BASE_ID, APPLICANTS_TABLE)

# Configure Gemini
genai.configure(api_key=GEMINI_API_KEY)

# Create Gemini model instance
model = genai.GenerativeModel("gemini-1.5-pro")  # Or "gemini-1.0-pro" or "gemini-pro" depending on your access

# -----------------------------
# 2. Loop through applicants
# -----------------------------
applicant_records = applicants_table.all()

for app_rec in applicant_records:
    app_id = app_rec["id"]
    json_str = app_rec["fields"].get("Compressed JSON")
    if not json_str:
        continue

    if app_rec["fields"].get("LLM Summary"):
        continue

    data = json.loads(json_str)

    prompt = f"""
You are a recruiting analyst. Given this JSON applicant profile, do four things:
1. Provide a concise summary (≤ 75 words).
2. Assign a quality score 1-10 (higher is better).
3. List any missing or contradictory fields.
4. Suggest up to three follow-up questions.

Return exactly in this format:
Summary: <text>
Score: <integer>
Issues: <comma-separated list or 'None'>
Follow-Ups: <bullet list>

Applicant JSON:
{json.dumps(data, indent=2)}
"""

    max_retries = 3
    retry_delay = 1
    for attempt in range(max_retries):
        try:
            response = model.generate_content(prompt)
            text = response.text.strip()
            break
        except Exception as e:
            print(f"API call failed: {e}, retrying...")
            time.sleep(retry_delay)
            retry_delay *= 2
    else:
        print(f"Failed to process applicant {app_id} after retries.")
        continue

    summary = score = issues = followups = ""
    for line in text.splitlines():
        if line.startswith("Summary:"):
            summary = line.replace("Summary:", "").strip()
        elif line.startswith("Score:"):
            score = line.replace("Score:", "").strip()
        elif line.startswith("Issues:"):
            issues = line.replace("Issues:", "").strip()
        elif line.startswith("Follow-Ups:"):
            followups = line.replace("Follow-Ups:", "").strip()

    applicants_table.update(app_id, {
        "LLM Summary": summary,
        "LLM Score": int(score) if score.isdigit() else None,
        "LLM Follow-Ups": followups
    })

    print(f"Processed LLM for applicant {app_id}")
