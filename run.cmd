@echo off
rem Lanzador de run.ps1 que funciona aunque Windows PowerShell tenga bloqueada la
rem ejecucion de scripts (politica "Restricted" por defecto). El permiso aplica solo
rem a este proceso: no cambia ninguna configuracion de Windows.
rem Uso:  .\run.cmd pipeline        .\run.cmd pipeline -Mes 2025-05 -Modelo dbt
powershell.exe -NoProfile -ExecutionPolicy Bypass -File "%~dp0run.ps1" %*
exit /b %ERRORLEVEL%
