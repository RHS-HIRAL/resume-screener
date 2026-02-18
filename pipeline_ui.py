import streamlit as st
import pandas as pd
from pathlib import Path
import os

# Set page config
st.set_page_config(page_title="Resume Scores Comparison", layout="wide")

# Title
st.title("📊 Resume Scores Comparison")
st.markdown("Compare **Gemini Match Scores** vs **Project Match Scores**")

# Sidebar for File Selection
st.sidebar.header("📁 Select Results File")

# Base logic to find files
# Assumes we are in "resumer screener setup" root or similar
BASE_DIR = Path(os.getcwd())
RESULTS_DIR = BASE_DIR / "screening_results"

if not RESULTS_DIR.exists():
    # Try alternate location if running from subfolder
    RESULTS_DIR = BASE_DIR / "../screening_results"

if not RESULTS_DIR.exists():
    st.error(f"Could not find results directory at {RESULTS_DIR}")
else:
    # List Excel files
    files = list(RESULTS_DIR.glob("pipeline_comparison_*.xlsx"))
    # Also list CSVs if xlsx not present?
    csv_files = list(RESULTS_DIR.glob("pipeline_comparison_*.csv"))
    
    all_files = sorted(files + csv_files, key=lambda x: x.stat().st_mtime, reverse=True)
    
    if not all_files:
        st.warning("No comparison result files found.")
    else:
        file_options = {f.name: f for f in all_files}
        selected_filename = st.sidebar.selectbox("Choose a file:", list(file_options.keys()))
        
        if selected_filename:
            selected_file = file_options[selected_filename]
            st.sidebar.info(f"Loaded: {selected_filename}")
            
            # Load Data
            try:
                if selected_file.suffix == ".xlsx":
                    df = pd.read_excel(selected_file, engine="openpyxl")
                else:
                    df = pd.read_csv(selected_file)
                
                # Display Data
                
                # Metrics
                col1, col2, col3 = st.columns(3)
                avg_gemini = df["Gemini Score"].mean()
                avg_project = df["Project Match Score"].mean()
                
                col1.metric("Avg Gemini Score", f"{avg_gemini:.1f}")
                col2.metric("Avg Project Score", f"{avg_project:.1f}")
                col3.metric("Candidate Count", len(df))
                
                # Dataframe with highlighting
                st.subheader("Detailed Scores")
                
                # Simple color formatting for scores
                def highlight_high_scores(val):
                    if isinstance(val, (int, float)):
                        if val >= 80:
                            return 'background-color: #d4edda; color: #155724' # Greenish
                        elif val >= 50:
                            return 'background-color: #fff3cd; color: #856404' # Yellowish
                        else:
                            return 'background-color: #f8d7da; color: #721c24' # Reddish
                    return ''

                try:
                    st.dataframe(
                        df.style.applymap(highlight_high_scores, subset=["Gemini Score", "Project Match Score"]),
                        use_container_width=True,
                        height=800
                    )
                except Exception:
                    # Fallback if styling fails
                    st.dataframe(df, use_container_width=True)
                
                # Scatter Plot Comparison
                if len(df) > 1:
                    st.subheader("Scatter Plot: Gemini vs Project Score")
                    st.scatter_chart(
                        df,
                        x="Project Match Score",
                        y="Gemini Score",
                        color="Candidate Name", # Or just rely on hover
                    )

            except Exception as e:
                st.error(f"Error reading file: {e}")

