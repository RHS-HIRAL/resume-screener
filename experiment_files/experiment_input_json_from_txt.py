import json
import os

def generate_api_payload(resume_path: str, jd_path: str, output_path: str = "payload.json"):
    if not os.path.exists(resume_path) or not os.path.exists(jd_path):
        print("Error: Please ensure both text files exist at the specified paths.")
        return

    with open(resume_path, 'r', encoding='utf-8') as f_resume:
        resume_text = f_resume.read()
        
    with open(jd_path, 'r', encoding='utf-8') as f_jd:
        jd_text = f_jd.read()
        
    payload = {
        "resume_text": resume_text,
        "jd_text": jd_text
    }
    
    with open(output_path, 'w', encoding='utf-8') as f_out:
        json.dump(payload, f_out, indent=2, ensure_ascii=False)
        
    print(f"JSON payload successfully generated and saved to: {output_path}")

if __name__ == "__main__":
    # Replace with the actual paths to your text files
    RESUME_FILE = "../extracted_txt_resumes/5101_Trainee_Accountant_US_Accounting_and_Taxation/Ishan_Thete_5101_2026-02-16.txt"
    JD_FILE = "../extracted_txt_jd/JD_trainee-accountant-us-accounting-and-taxation.txt"
    
    generate_api_payload(RESUME_FILE, JD_FILE)