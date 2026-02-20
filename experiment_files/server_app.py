import os
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
import google.generativeai as genai

app = FastAPI(title="Automated Resume Screener API")
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
model = genai.GenerativeModel('gemini-2.5-flash')

class ParameterMatch(BaseModel):
    status: str = Field(description="Match, Partial Match, or No Match")
    summary: str = Field(description="A 1-line summary indicating if and why it matches the JD")

class ResumeJDMatch(BaseModel):
    overall_match_score: int = Field(description="Overall match score strictly on a scale of 0 to 100")
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

@app.post("/api/v1/analyze-resume")
async def analyze_resume(request: AnalysisRequest):
    prompt = f"""
    You are an expert technical recruiter and resume parser.
    Analyze the provided Resume and Job Description (JD).
    
    Perform two functions and return the result strictly in the requested JSON schema:
    1. Extract structured data from the resume.
    2. Evaluate the resume against the JD across key parameters to generate an overall match score (0-100) and provide a concise 1-line summary for each parameter's match status.
    
    CRITICAL SCORING RULE: 
    - If the JD does NOT mention any required certifications, but the candidate HAS certifications on their resume, you MUST mark the certifications status as a "Match" (treat it as a small bonus/plus point) and increase the score by 2-3 points. Do NOT mark it as "No Match".

    Resume Text:
    {request.resume_text}
    
    Job Description Text:
    {request.jd_text}
    """
    
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
        print("\n--- NORMAL PROMPT RESULTS ---")
        print(f"Input Tokens:  {usage.prompt_token_count}")
        print(f"Output Tokens: {usage.candidates_token_count}")
        print(f"Total Tokens:  {usage.total_token_count}")
        print(f"Response:\n{response.text}")
        return Response(content=response.text, media_type="application/json")
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Run the server using: uvicorn server_app:app --reload