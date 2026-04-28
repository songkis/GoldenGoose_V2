@echo off 

:: BatchGotAdmin 
:------------------------------------- 
rem --> Check for permissions 
>nul 2>&1 "%SYSTEMROOT%\system32\cacls.exe" "%SYSTEMROOT%\system32\config\system" 

rem --> If error flag set, we do not have admin. 
if '%errorlevel%' NEQ '0' ( 
	echo Requesting administrative privileges... 
	goto UACPrompt 
) else ( goto gotAdmin ) 

:UACPrompt 
	echo Set UAC = CreateObject^("Shell.Application"^) > "%temp%\getadmin.vbs" 
	set "params=%*"
	echo UAC.ShellExecute "cmd.exe", "/c %~s0 %params%", "", "runas", 1 >> "%temp%\getadmin.vbs" 

	"%temp%\getadmin.vbs" 
	del "%temp%\getadmin.vbs" 
	exit /B 

:gotAdmin 
	pushd "%CD%" 
	CD /D "%~dp0" 
:--------------------------------------

cd /d C:\GoldenGoose_V2\GG_Client
regsvr32 /s C:\LS_SEC\xingAPI\XA_Session.dll
regsvr32 /s C:\LS_SEC\xingAPI\XA_DataSet.dll
:: 32-bit Python environment activation and execution
call C:\GoldenGoose\Python311-32\Scripts\activate.bat &
python GoldenGoose.pyw
