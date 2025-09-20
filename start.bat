@echo off
setlocal enabledelayedexpansion

set "ROOT=%~dp0"
if "%ROOT:~-1%"=="\" set "ROOT=%ROOT:~0,-1%"

call :build_ui frontend "Frontend"
call :build_ui AiWord "AiWord"

echo [start] Initializing database and starting Flask...
echo [start] UI: http://localhost:5050/app (legacy: /?ui=legacy)

set PYTHONUNBUFFERED=1

if defined PY_CMD (
    call %PY_CMD% "%ROOT%\app.py"
    goto :eof
)

if defined VIRTUAL_ENV (
    call python "%ROOT%\app.py"
    goto :eof
)

if exist "%ROOT%\.venv\Scripts\python.exe" (
    call "%ROOT%\.venv\Scripts\python.exe" "%ROOT%\app.py"
    goto :eof
)

where poetry >nul 2>nul
if %ERRORLEVEL%==0 (
    poetry run python "%ROOT%\app.py"
    goto :eof
)

where pipenv >nul 2>nul
if %ERRORLEVEL%==0 (
    pipenv run python "%ROOT%\app.py"
    goto :eof
)

where conda >nul 2>nul
if %ERRORLEVEL%==0 (
    for /f "tokens=1" %%i in ('conda env list ^| findstr /I "flask_catalog"') do (
        conda run -n flask_catalog python "%ROOT%\app.py"
        goto :eof
    )
)

python "%ROOT%\app.py"
goto :eof

:build_ui
set "DIR=%~1"
set "LABEL=%~2"
if exist "%ROOT%\%DIR%\dist\index.html" (
    echo [start] %LABEL% build exists.
    goto :eof
)

where npm >nul 2>nul
if %ERRORLEVEL% NEQ 0 (
    echo [start] npm not found; cannot build %LABEL% (dist missing)
    goto :eof
)

pushd "%ROOT%\%DIR%"
if not exist node_modules (
    echo [start] Installing npm dependencies for %LABEL%...
    npm install
)
echo [start] Building %LABEL%...
npm run build
popd
goto :eof
