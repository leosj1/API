set ROOT_DIR = C:/API_LIVE/API/
call C:/Users/seun/anaconda3/Scripts/activate.bat C:\Users\seun\anaconda3\
set LOG_DIR = C:API_LIVE\LOGS\
set LOG_DIR = %LOG_DIR%\Logs


set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%



echo ------------------------------------START------------------------------------------ >> C:\API_LIVE\LOGS\Log_Updates_%SUBFILENAME%.log
echo %date%-%time% >> C:\API_LIVE\LOGS\Log_Updates_%SUBFILENAME%.log
"C:\Users\seun\anaconda3\python.exe" "C:\API_LIVE\API\Main.py" echo %DATE% %TIME% >>C:\API_LIVE\LOGS\Log_Updates_%SUBFILENAME%.log 2>&1
echo %date%-%time% >> C:\API_LIVE\LOGS\Log_Updates_%SUBFILENAME%.log
echo ------------------------------------END------------------------------------------ >> C:\API_LIVE\LOGS\Log_Updates_%SUBFILENAME%.log

REM echo ------------------------------------START------------------------------------------ >> C:\Users\e0185872\Documents\DataFactory\Comments_Automation\Comments_extract\Logs\Log_Updates_%SUBFILENAME%.log
REM echo %date%-%time% >> C:\Users\e0185872\Documents\DataFactory\Comments_Automation\Comments_extract\Logs\Log_Updates_%SUBFILENAME%.log
REM "C:\Users\seun\anaconda3\python.exe" "C:\Users\seun\Downloads\BT-PostProcessing\BT-PostProcessing\TableUpdates\blogposts.py" echo %DATE% %TIME% >> C:\Users\e0185872\Documents\DataFactory\Comments_Automation\Comments_extract\Logs\Log_Updates_%SUBFILENAME%.log 2>&1
REM echo %date%-%time% >> C:\Users\e0185872\Documents\DataFactory\Comments_Automation\Comments_extract\Logs\Log_Updates__%SUBFILENAME%.log
REM echo ------------------------------------END------------------------------------------ >> C:\Users\e0185872\Documents\DataFactory\Comments_Automation\Comments_extract\Logs\Log_Updates_%SUBFILENAME%.log
pause