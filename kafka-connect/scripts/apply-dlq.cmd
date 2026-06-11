@echo off
echo Applying DLQ config to all connectors...

REM 
for /f "tokens=*" %%i in ('curl -s http://localhost:8083/connectors') do set CONNECTORS=%%i
echo Current connectors: %CONNECTORS%