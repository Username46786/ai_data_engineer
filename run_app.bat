@echo off
setlocal

set "PROJECT_DIR=%~dp0"
set "APP_PY=%PROJECT_DIR%app.py"
set "LOCAL_PACKAGES=%PROJECT_DIR%.python_packages"
set "ARCGIS_PY=C:\Program Files\ArcGIS\Pro\bin\Python\envs\arcgispro-py3\python.exe"
set "ARCGIS_JAVA=C:\Program Files\ArcGIS\Pro\java\runtime\jre"

if exist "%LOCAL_PACKAGES%" set "PYTHONPATH=%LOCAL_PACKAGES%;%PYTHONPATH%"
set "PANDAS_USE_NUMEXPR=0"

if exist "%ARCGIS_JAVA%\bin\java.exe" (
    set "JAVA_HOME=%ARCGIS_JAVA%"
    set "PATH=%ARCGIS_JAVA%\bin;%PATH%"
)

if exist "%ARCGIS_PY%" (
    "%ARCGIS_PY%" -m streamlit run "%APP_PY%"
) else (
    streamlit run "%APP_PY%"
)

endlocal
