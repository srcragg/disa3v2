@echo off
:start

cd /d "C:/Users/Steve Cragg/Documents/disa3v2"


start cmd /k "python casting_counter_1.0.py"
::if %errorlevel% neq 0 goto restart
timeout /t 10
start "" "cmd.exe" /k "streamlit run streamlit_app_v1.py --server.port=8502"
::if %errorlevel% neq 0 goto restart
timeout /t 10
start cmd /k "python disa_processor_to_mqtt.py"
::if %errorlevel% neq 0 goto restart
::goto end

::restart
::echo Script crashed, restarting...
::timeout /t 5
::goto start

:end
echo All scripts started.