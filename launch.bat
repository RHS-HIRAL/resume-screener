@echo off
setlocal

:: --- Configuration ---
:: Matches the VENV_NAME in your setup.bat
set "VENV_NAME=.venv"
set "SCRIPT_NAME=app.py"

:: --- Check for Virtual Environment ---
if not exist "%VENV_NAME%\Scripts\activate.bat" (
    echo Virtual environment not found.
    echo Running setup.bat to configure the application...
    echo.
    call setup.bat
)

:: --- Final Check ---
if not exist "%VENV_NAME%\Scripts\activate.bat" (
    echo.
    echo Error: Setup could not be completed.
    echo Please check the error messages above.
    pause
    exit /b 1
)

:: --- Launch Application ---
echo.
echo Activating virtual environment...
call "%VENV_NAME%\Scripts\activate.bat"

echo Launching %SCRIPT_NAME%...
streamlit run "%SCRIPT_NAME%"

:: --- Deactivate and Pause ---
call "%VENV_NAME%\Scripts\deactivate.bat"
echo.
echo Application closed.
pause