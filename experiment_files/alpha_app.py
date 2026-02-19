import os
from dotenv import load_dotenv
import json
from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
import google.generativeai as genai

app = FastAPI(title="Automated Resume Screener API")
load_dotenv()
genai.configure(api_key=os.getenv("GOOGLE_API_KEY"))
model = genai.GenerativeModel('gemini-2.5-flash')

class MatchParameter(BaseModel):
    jd_requirement: str = Field(description="The requirement stated in the JD")
    resume_value: str = Field(description="The corresponding value found in the resume")
    status: str = Field(description="Match, Partial Match, or No Match")
    score_contribution: int = Field(description="Points contributed to the overall score")

class SkillMatch(BaseModel):
    matched_skills: list[str]
    missing_skills: list[str]
    status: str
    score_contribution: int

class ToolMatch(BaseModel):
    matched_tools: list[str]
    missing_tools: list[str]
    status: str
    score_contribution: int

class ResumeJDMatch(BaseModel):
    overall_match_score: int
    experience: MatchParameter
    education: MatchParameter
    skillset: SkillMatch
    location: MatchParameter
    project_history_relevance: MatchParameter
    tools_used: ToolMatch
    certifications: MatchParameter

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
    skills_inventory: list[str]
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
    2. Evaluate the resume against the JD across key parameters to generate a match score.
    
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

# Run the server using: uvicorn main:app --reload