@echo off
setlocal

:: --- Configuration ---
set "PYTHON_VER=3.10.11"
set "INSTALLER_URL=https://www.python.org/ftp/python/3.10.11/python-3.10.11-amd64.exe"
set "INSTALLER_NAME=python-installer.exe"
set "VENV_NAME=.venv"
set "REQ_FILE=requirements.txt"

:: --- Step 1: Check if Python 3.10.11 is already installed ---
echo Checking for Python %PYTHON_VER%...
python --version 2>NUL | findstr /C:"Python %PYTHON_VER%" >NUL
if %errorlevel%==0 (
    echo Python %PYTHON_VER% is already installed.
    goto :create_venv
)

:: --- Step 2: Download and Install Python if missing ---
echo Python %PYTHON_VER% not found. Downloading installer...
curl -o %INSTALLER_NAME% %INSTALLER_URL%

if not exist %INSTALLER_NAME% (
    echo Error: Failed to download python installer.
    pause
    exit /b 1
)

echo Installing Python %PYTHON_VER% (this may take a few minutes)...
:: /quiet = Silent install
:: InstallAllUsers=1 = Install to Program Files (requires Admin)
:: PrependPath=1 = Add to PATH
start /wait %INSTALLER_NAME% /quiet InstallAllUsers=1 PrependPath=1 Include_test=0

:: Clean up installer
del %INSTALLER_NAME%

:: Refresh PATH for the current session manually so we can use 'python' immediately
:: Standard path for system-wide install:
set "PATH=%ProgramFiles%\Python310\Scripts;%ProgramFiles%\Python310;%PATH%"

:: Verify installation
python --version 2>NUL | findstr /C:"Python %PYTHON_VER%" >NUL
if %errorlevel% neq 0 (
    echo Error: Python installation failed or PATH not updated.
    echo Please restart your computer and run this script again.
    pause
    exit /b 1
)
echo Python %PYTHON_VER% installed successfully.

:create_venv
:: --- Step 3: Create Virtual Environment ---
if not exist "%VENV_NAME%" (
    echo Creating virtual environment '%VENV_NAME%'...
    python -m venv %VENV_NAME%
) else (
    echo Virtual environment '%VENV_NAME%' already exists.
)

:: --- Step 4: Install Requirements ---
if exist "%REQ_FILE%" (
    echo Installing dependencies from %REQ_FILE%...
    call %VENV_NAME%\Scripts\activate.bat
    python -m pip install --upgrade pip
    pip install -r %REQ_FILE%
    call %VENV_NAME%\Scripts\deactivate.bat
) else (
    echo Warning: %REQ_FILE% not found. Skipping requirement installation.
)

:: --- Complete ---
echo.
echo ========================================================
echo Setup Complete!
echo You can activate the environment with: %VENV_NAME%\Scripts\activate
echo ========================================================
pause