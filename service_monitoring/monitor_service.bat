rem This batch file initiates the 3.X installation of Python so that this script can run. 
rem If either the location of the script or the location of the Python 3.X installation changes
rem then these values would need to change.

rem cd %~dp0

set commonConfigurationFile=config.json
set serviceConfigurationFile=service_config.json
set instance=gis.charlottenc.gov

REM @echo off
"C:\Python\python.exe" "%~dp0\GetService_Status.py" %1 %commonConfigurationFile% %serviceConfigurationFile% %instance%
pause