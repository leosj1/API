call C:/Users/seun/anaconda3/Scripts/activate.bat C:\Users\seun\anaconda3\
cd C:\API_LIVE\API\Terms
REM call conda activate base

set CUR_YYYY=%date:~10,4%
set CUR_MM=%date:~4,2%
set CUR_DD=%date:~7,2%
REM set CUR_TT=%time:~0,8%
REM set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%_%CUR_TT:~0,2%_%CUR_TT:~3,2%_%CUR_TT:~6,2%

set SUBFILENAME=%CUR_MM%-%CUR_DD%-%CUR_YYYY%

echo ------------------------------------START------------------------------------------ >> C:\API_LIVE\API\Terms\Logs\Log_Terms_%SUBFILENAME%.log
echo %date%-%time% >> C:\API_LIVE\API\Terms\Logs\Log_Terms_%SUBFILENAME%.log
"C:\Users\seun\anaconda3\python.exe" "C:\API_LIVE\API\Terms\compute_terms.py" echo %DATE% %TIME% >> C:\API_LIVE\API\Terms\Logs\Log_Terms_%SUBFILENAME%.log 2>&1
echo %date%-%time% >> C:\API_LIVE\API\Terms\Logs\Log_Terms_%SUBFILENAME%.log
echo ------------------------------------END------------------------------------------ >> C:\API_LIVE\API\Terms\Logs\Log_Terms_%SUBFILENAME%.log
REM pause