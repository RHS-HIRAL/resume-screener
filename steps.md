📘 How to Run the Resume Screener Project

This guide explains the exact steps to set up and run the AI-powered resume screener project using a virtual environment.

⸻

✅ Prerequisites
	•	Python 3.11+ installed
	•	Terminal or command prompt access
	•	Files needed:
	•	app.py
	•	requirements.txt
	•	.env with your GOOGLE_API_KEY

⸻

🔧 Step-by-Step Setup Instructions

1. 📁 Create a Project Folder

mkdir resume-screener
cd resume-screener

Move app.py, requirements.txt, and .env into this folder.

2. 🧪 Create a Virtual Environment

python3 -m venv venv

3. ▶️ Activate the Virtual Environment
	•	Mac/Linux:

source venv/bin/activate

	•	Windows (CMD):

venv\Scripts\activate

	•	Windows (PowerShell):

.\venv\Scripts\Activate.ps1

4. ⬆️ Upgrade pip

pip install --upgrade pip

5. 📦 Install Dependencies

pip install -r requirements.txt

6. 🔑 Add Your API Key to .env

Create a file named .env if not already present, and add:

GOOGLE_API_KEY=your_google_api_key_here

⚠️ Keep this key secure and do not share it.

7. 🚀 Run the App

streamlit run app.py

This will open the app in your default browser at:

http://localhost:8501


⸻

🔁 Deactivate the Environment

After you’re done:

deactivate


⸻
