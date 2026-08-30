@echo off
REM ===================================================================
REM  Chay toan bo verifier runtime theo lich. Dang ky bang Task Scheduler:
REM    schtasks /Create /TN BDP-verify /TR "D:igdata-platform\scriptserify-scheduled.cmd" /SC HOURLY /F
REM
REM  Vi sao script nam TRONG repo con lich thi khong: logic phai duoc version va
REM  review nhu moi thu khac; rieng viec dang ky la cau hinh cua TUNG MAY nen no
REM  o ngoai. Doi cach kiem = sua file nay + commit, khong phai go lai task.
REM  Xem ADR-0043.
REM ===================================================================
setlocal
cd /d "%~dp0.."
if not exist ".verify" mkdir ".verify"

REM Ghi log day du cua lan chay gan nhat (de doc khi co su co)
python -m dataplatform.cli verify > ".verify\last-run.txt" 2>&1
set RC=%ERRORLEVEL%

REM Phan loai theo ma thoat — KHONG gop lam mot:
REM   0 = dat | 1 = LECH that su, phai sua | 3 = stack chua bat, chua ket luan duoc
if "%RC%"=="0" set STATUS=DAT
if "%RC%"=="1" set STATUS=LECH
if "%RC%"=="3" set STATUS=STACK-TAT
if not defined STATUS set STATUS=LOI-%RC%

echo %DATE% %TIME% ^| %STATUS% ^| exit=%RC% >> ".verify\history.log"

REM Chi ma 1 moi la bao dong that. Stack tat (3) tra 0 de Task Scheduler khong
REM bao that bai lien tuc — bao dong gia lap lai thi ca cong bi bo qua (ADR-0041).
if "%RC%"=="1" exit /b 1
exit /b 0
