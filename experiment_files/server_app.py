# Run the server using: uvicorn server_app:app --reload
import os
import asyncio
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
import google.generativeai as genai

app = FastAPI(title="Automated Resume Screener API")
load_dotenv()

# --- Fallback API Key Setup ---
API_KEYS = []
for i in range(1, 11):
    key_name = "GOOGLE_API_KEY" if i == 1 else f"GOOGLE_API_KEY{i}"
    key_val = os.getenv(key_name)
    if key_val:
        API_KEYS.append(key_val)

if not API_KEYS:
    raise ValueError("No GOOGLE_API_KEY variables found in the environment.")

current_key_index = 0
key_lock = asyncio.Lock()  # Ensures thread-safe key rotation during concurrent requests
# ------------------------------


class ParameterMatch(BaseModel):
    status: str = Field(description="Match, Partial Match, or No Match")
    summary: str = Field(
        description="A 1-line summary indicating if and why it matches the JD"
    )


class ResumeJDMatch(BaseModel):
    overall_match_score: int = Field(
        description="Overall match score strictly on a scale of 0 to 100"
    )
    experience: ParameterMatch
    education: ParameterMatch
    location: ParameterMatch
    project_history_relevance: ParameterMatch
    tools_used: ParameterMatch
    certifications: ParameterMatch


class PersonalInfo(BaseModel):
    full_name: str
    location: str
    email: str
    phone: str


class Employment(BaseModel):
    current_job_title: str
    current_organization: str


class CareerMetrics(BaseModel):
    total_experience_in_years: float
    total_jobs: int


class Socials(BaseModel):
    linkedin: str
    github: str
    portfolio: str


class Education(BaseModel):
    degree: str
    institution: str
    graduation_year: str


class ResumeDataExtraction(BaseModel):
    personal_information: PersonalInfo
    professional_summary: str
    current_employment: Employment
    career_metrics: CareerMetrics
    social_profiles: Socials
    education_history: list[Education]


class ComprehensiveResumeAnalysis(BaseModel):
    function_1_resume_jd_matching: ResumeJDMatch
    function_2_resume_data_extraction: ResumeDataExtraction


class AnalysisRequest(BaseModel):
    resume_text: str
    jd_text: str


@app.get("/api/v1/health")
async def health_check():
    """Health check endpoint returning key pool status."""
    return {
        "status": "ok",
        "total_keys": len(API_KEYS),
        "active_key_index": current_key_index,
    }


@app.post("/api/v1/analyze-resume")
async def analyze_resume(request: AnalysisRequest):
    global current_key_index

    prompt = f"""
    You are an expert technical recruiter and resume parser.
    Analyze the provided Resume and Job Description (JD).
    
    Perform two functions and return the result strictly in the requested JSON schema:
    1. Extract structured data from the resume.
    2. Evaluate the resume against the JD across key parameters to generate an overall match score (0-100) and provide a concise 1-line summary for each parameter's match status.
    
    CRITICAL SCORING RULE: 
    - If the JD does NOT mention any required certifications, but the candidate HAS certifications on their resume, you MUST mark the certifications status as a "Match" (treat it as a small bonus/plus point) and increase the score by 2-3 points. Do NOT mark it as "No Match".
    - If certifications are neither requested in the JD nor provided in the resume, evaluate the certifications status as a "Partial Match" to ensure the absence of an irrelevant requirement does not artificially lower the candidate's overall score.
    - If the JD explicitly requires or mentions certifications, but the candidate's resume lacks them entirely, you must mark the certifications status as a "No Match".
    - If the candidate's total experience exceeds the JD's required experience by more than 5 years (indicating they are highly overqualified), mark the experience status as a "Partial Match" rather than a full match.
    - Location Scoring Logic:
        If the candidate is in the exact same city as the JD requirement, mark it as a "Match".
        If the candidate is in the same state but a different city, mark it as a "Partial Match".
        If the candidate is in a completely different state, mark it as a "No Match".
        If the candidate's location is absent from the resume, default to a "Partial Match".

    Resume Text:
    {request.resume_text}
    
    Job Description Text:
    {request.jd_text}
    """

    # Try available keys until one works or all are exhausted
    for attempt in range(len(API_KEYS)):
        # Safely get the active key
        async with key_lock:
            key_to_use = API_KEYS[current_key_index]

        genai.configure(api_key=key_to_use)
        model = genai.GenerativeModel("gemini-2.5-flash")

        try:
            response = model.generate_content(
                prompt,
                generation_config=genai.GenerationConfig(
                    response_mime_type="application/json",
                    response_schema=ComprehensiveResumeAnalysis,
                    temperature=0.1,
                ),
            )

            usage = response.usage_metadata
            print(f"\n--- SUCCESS WITH KEY INDEX {current_key_index} ---")
            print(f"Input Tokens:  {usage.prompt_token_count}")
            print(f"Output Tokens: {usage.candidates_token_count}")
            print(f"Total Tokens:  {usage.total_token_count}")
            print(f"Response:\n{response.text}")

            return Response(content=response.text, media_type="application/json")

        except Exception as e:
            error_msg = str(e).lower()

            # Check if the error is related to quota, rate limits, or invalid keys
            rate_limit_keywords = [
                "429",
                "quota",
                "exhausted",
                "rate limit",
                "invalid",
                "api_key",
            ]
            if any(keyword in error_msg for keyword in rate_limit_keywords):
                print(
                    f"⚠️ API Key index {current_key_index} failed (Quota/Limit). Rotating..."
                )

                # Safely increment to the next key
                async with key_lock:
                    # Check if another thread already rotated the key while this one was failing
                    if API_KEYS[current_key_index] == key_to_use:
                        current_key_index = (current_key_index + 1) % len(API_KEYS)

                continue  # Retry with the next key

            else:
                # If it's a different error (e.g., bad schema, safety block), raise immediately
                raise HTTPException(status_code=500, detail=str(e))

    # If the loop finishes without returning, all keys failed
    print("❌ All API keys have been exhausted.")
    raise HTTPException(
        status_code=429,
        detail="API Quota Exceeded. All fallback keys are currently exhausted.",
    )
