import os
import google.generativeai as genai
import typing_extensions as typing
from dotenv import load_dotenv

# 1. SETUP
load_dotenv()
# Make sure your .env file has GOOGLE_API_KEY=AIza...
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))

# 2. DEFINE OUTPUT STRUCTURE
class ReviewAnalysis(typing.TypedDict):
    sentiment_score: float
    is_positive: bool
    primary_complaint: str
    feature_tags: list[str]
    summary: str

# 3. CONFIGURE MODEL
model = genai.GenerativeModel("gemini-2.5-flash")

# 4. EXECUTE
# We pass the schema directly to the model configuration
response = model.generate_content(
    "I hated the battery life on this phone, but the camera was amazing. It felt cheap to hold though.",
    generation_config=genai.GenerationConfig(
        response_mime_type="application/json", 
        response_schema=ReviewAnalysis
    )
)

# 5. PRINT TOKEN USAGE
usage = response.usage_metadata
print("--- STRUCTURED OUTPUT (FUNCTION CALLING) RESULTS ---")
print(f"Input Tokens:  {usage.prompt_token_count}")
print(f"Output Tokens: {usage.candidates_token_count}")
print(f"Total Tokens:  {usage.total_token_count}")
print(f"Response:\n{response.text}")