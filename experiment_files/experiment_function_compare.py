'''
Structured-Normal
       580-737
       457-552
       316-238
       406-402
       438-437
       568-624
       477-794
       671-691
=====================
      3913-4475

We will use structured or function calling for json formatted response.
'''

import os
import google.generativeai as genai
from dotenv import load_dotenv

load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

model = genai.GenerativeModel("gemini-2.5-flash")

# PROMPT ENGINEERING
# We have to manually describe the schema in English.
prompt = """
Analyze the following product review and output the result strictly as a valid JSON object.

The JSON object must have these exact keys:
- sentiment_score: a float between 0 and 1
- is_positive: boolean
- primary_complaint: string
- feature_tags: list of strings
- summary: string

Review: "I hated the battery life on this phone, but the camera was amazing. It felt cheap to hold though."
"""

response = model.generate_content(prompt)

usage = response.usage_metadata
print("\n--- NORMAL PROMPT RESULTS ---")
print(f"Input Tokens:  {usage.prompt_token_count}")
print(f"Output Tokens: {usage.candidates_token_count}")
print(f"Total Tokens:  {usage.total_token_count}")
print(f"Response:\n{response.text}")