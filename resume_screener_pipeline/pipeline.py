import os
import json
import argparse
import pandas as pd
import google.generativeai as genai
from groq import Groq
from dotenv import load_dotenv
from pathlib import Path
from resume_screener_pipeline.old_pipeline import WeightedScorer

# Load environment variables
load_dotenv()

# Configure API Keys
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
GROQ_API_KEY_ALT = os.getenv("GROQ_API_KEY_ALT")

# Setup GenAI
if GOOGLE_API_KEY:
    genai.configure(api_key=GOOGLE_API_KEY)

class GeminiScorer:
    def __init__(self):
        self.fallback_chain = self._build_fallback_chain()

    def _build_fallback_chain(self):
        chain = []
        if GOOGLE_API_KEY:
            chain.append(("Gemini", None, "gemini-2.5-pro"))
        if GROQ_API_KEY:
            chain.append(("Groq-Primary", GROQ_API_KEY, "llama-3.3-70b-versatile"))
        if GROQ_API_KEY_ALT:
            chain.append(("Groq-Alt", GROQ_API_KEY_ALT, "llama-3.3-70b-versatile"))
        # Add more fallbacks if needed (e.g. smaller models)
        return chain

    def _call_gemini(self, prompt, model_name):
        model = genai.GenerativeModel(model_name)
        response = model.generate_content(prompt)
        return response.text

    def _call_groq(self, prompt, api_key, model_name):
        client = Groq(api_key=api_key)
        response = client.chat.completions.create(
            model=model_name,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.1,
        )
        return response.choices[0].message.content

    def generate_score(self, resume_json, jd_json):
        # Convert JSONs to string for prompt
        resume_str = json.dumps(resume_json, indent=2)
        jd_str = json.dumps(jd_json, indent=2)

        prompt = f"""You are an expert resume parser and ranker. Step by step, evaluate the candidate's match against the job description.

CRITICAL INSTRUCTIONS:
1. Return ONLY valid JSON in the exact format shown below.
2. Analyze the RESUME DATA and JOB DESCRIPTION DATA provided in JSON format.
3. specific focus on:
    - Tech Stack match (Languages, Frameworks, Tools)
    - Experience relevance (Years, Complexity, Domain)
    - Project relevance (Similar projects, Scale, Impact)
4. Provide a match score from 0 to 100.

RESUME DATA:
{resume_str}

JOB DESCRIPTION DATA:
{jd_str}

Return JSON in this EXACT format:
{{
    "name": "Candidate Name",
    "match_score": "Match Score (0-100)",
    "reasoning": "Brief explanation of the score"
}}
"""
        
        for label, api_key, model_name in self.fallback_chain:
            try:
                # print(f"Attempting with {label}...")
                if label == "Gemini":
                    response_text = self._call_gemini(prompt, model_name)
                else:
                    response_text = self._call_groq(prompt, api_key, model_name)
                
                # Parse JSON response
                # Clean up markdown if present
                clean_text = response_text.replace("```json", "").replace("```", "").strip()
                start_idx = clean_text.find("{")
                end_idx = clean_text.rfind("}") + 1
                if start_idx != -1 and end_idx != -1:
                    json_str = clean_text[start_idx:end_idx]
                    return json.loads(json_str)
                else:
                    raise ValueError("No JSON found in response")

            except Exception as e:
                print(f"Error with {label}: {e}")
                continue
        
        return {"match_score": 0, "reasoning": "Failed to generate score with all providers"}

def main():
    parser = argparse.ArgumentParser(description="Resume Scoring Pipeline (Gemini vs Project Match)")
    parser.add_argument("--role", required=True, help="Role name (matches folder name in extracted_json_resumes)")
    parser.add_argument("--limit", type=int, default=10, help="Number of resumes to process")
    args = parser.parse_args()

    role_name = args.role
    
    # Paths
    base_dir = Path(os.getcwd()) # Use current working directory
    # If running from within "resume_screener_pipeline", adjust? 
    # Let's start with CWD assumption. 
    # If standard setup: CWD is project root "resumer screener setup"
    
    resumes_dir = base_dir / "extracted_json_resumes" / role_name
    jd_folder = base_dir / "extracted_json_jd"
    output_dir = base_dir / "screening_results"
    output_dir.mkdir(exist_ok=True)
    
    if not resumes_dir.exists():
        # Try full absolute path if relative fails
        base_dir_abs = Path(r"c:\Hiral\Work\Si2 Resume Screener\resumer screener setup")
        resumes_dir = base_dir_abs / "extracted_json_resumes" / role_name
        jd_folder = base_dir_abs / "extracted_json_jd"
        output_dir = base_dir_abs / "screening_results"
        output_dir.mkdir(exist_ok=True)

        if not resumes_dir.exists():
            print(f"Error: Resume directory not found: {resumes_dir}")
            return

    # Locate JD File
    jd_file = None
    
    # 1. Exact match
    candidates = [
        f"JD_{role_name}.json",
        f"JD_{role_name.replace('_', '-')}.json", # kebab-case
    ]
    
    # 2. Strip ID (e.g. "3237_SAP_SD_Consultant" -> "sap-sd-consultant")
    parts = role_name.split('_')
    if len(parts) > 1 and parts[0].isdigit():
        role_without_id = "_".join(parts[1:])
        candidates.append(f"JD_{role_without_id}.json")
        candidates.append(f"JD_{role_without_id.replace('_', '-')}.json") # kebab
        candidates.append(f"JD_{role_without_id.lower().replace('_', '-')}.json") # kebab lower

    print(f"Looking for JD file candidates: {candidates}")

    for filename in candidates:
        fpath = jd_folder / filename
        if fpath.exists():
            jd_file = fpath
            print(f"Found JD file: {jd_file}")
            break
    
    if not jd_file:
        # Fallback: fuzzy match based on text overlap? or just first JD?
        # Let's list available and ask user? No interactive mode.
        # Just pick one that looks closest or fail.
        print(f"Error: JD file not found in {jd_folder}. Candidates checked: {candidates}")
        # List available JDs to help debug
        print("Available JDs:")
        for f in jd_folder.glob("*.json"):
            print(f" - {f.name}")
        return


    # Load JD
    with open(jd_file, "r", encoding="utf-8") as f:
        jd_json = json.load(f)

    # Initialize Scorers
    gemini_scorer = GeminiScorer()
    # WeightedScorer needs a vector store usually, but we can try without it for pure text match logic
    # The old_pipeline.py says "If provided, used as a BOOST... Not required."
    project_scorer = WeightedScorer(vector_store=None) 

    results = []
    
    resume_files = list(resumes_dir.glob("*.json"))[:args.limit]
    print(f"Processing {len(resume_files)} resumes for role: {role_name}")

    for resume_file in resume_files:
        try:
            with open(resume_file, "r", encoding="utf-8") as f:
                resume_json = json.load(f)
            
            # 1. Project Match Score
            # Need 'resume_text' for tf-idf. The json might have it or we reconstruct it.
            # old_pipeline uses 'resume_json_to_text' helper.
            # Let's import it or re-implement simple version.
            # Checking old_pipeline.py again... yes `resume_json_to_text` is global function.
            from resume_screener_pipeline.old_pipeline import resume_json_to_text
            
            resume_text = resume_json_to_text(resume_json)
            
            # Project Scorer expects jd_parsed (which is jd_json structure mostly)
            # The keys might vary. old_pipeline expects "required_skills", "location", etc.
            # gathered from parse_jd output.
            # Our jd_json from extracted_json_jd likely has this structure.
            
            project_score_data = project_scorer.score_candidate(
                resume_json=resume_json,
                resume_text=resume_text,
                jd_parsed=jd_json
            )
            project_score = project_score_data.get("project_relevance_score", 0)

            # 2. Gemini Match Score
            gemini_result = gemini_scorer.generate_score(resume_json, jd_json)
            gemini_score = gemini_result.get("match_score", 0)
            gemini_reason = gemini_result.get("reasoning", "")
            
            # Clean gemini score if it's a string like "85%"
            if isinstance(gemini_score, str):
                gemini_score = "".join(filter(str.isdigit, gemini_score))
                gemini_score = float(gemini_score) if gemini_score else 0

            results.append({
                "Filename": resume_file.name,
                "Candidate Name": resume_json.get("name") or resume_json.get("full_name") or "Unknown",
                "Gemini Score": gemini_score,
                "Project Match Score": project_score,
                "Gemini Reason": gemini_reason
            })
            print(f"Processed {resume_file.name}: Gem={gemini_score}, Proj={project_score}")

        except Exception as e:
            print(f"Error processing {resume_file.name}: {e}")

    # Save Results
    if results:
        df = pd.DataFrame(results)
        # Save as CSV
        output_csv = output_dir / f"pipeline_comparison_{role_name}.csv"
        df.to_csv(output_csv, index=False)
        
        # Save as Excel
        output_excel = output_dir / f"pipeline_comparison_{role_name}.xlsx"
        df.to_excel(output_excel, index=False, engine='openpyxl')
        
        print(f"\nResults saved to:\n - {output_csv}\n - {output_excel}")
        print(df[["Filename", "Gemini Score", "Project Match Score"]])
    else:
        print("No results generated.")

if __name__ == "__main__":
    main()
