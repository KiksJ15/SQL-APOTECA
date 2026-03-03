@echo off
REM ============================================================
REM  Installation de la tache planifiee Windows
REM  Execute auto_update.py toutes les heures de 9h a 18h (lun-ven)
REM ============================================================

echo === Configuration de la tache planifiee Apoteca ===
echo.

REM Detecter le chemin du projet
set SCRIPT_DIR=%~dp0
set PROJECT_DIR=%SCRIPT_DIR%..

REM Verifier que Python est installe
where python >nul 2>&1
if %ERRORLEVEL% NEQ 0 (
    echo ERREUR: Python non trouve dans le PATH
    pause
    exit /b 1
)

REM Creer la tache planifiee
echo Creation de la tache "Apoteca_AutoUpdate"...
echo   - Frequence: toutes les heures
echo   - Horaires: 9h00 a 18h00
echo   - Jours: lundi a vendredi
echo.

REM Supprimer la tache si elle existe deja
schtasks /delete /tn "Apoteca_AutoUpdate" /f >nul 2>&1

REM Creer la tache (se lance toutes les heures entre 9h et 18h, lun-ven)
schtasks /create ^
    /tn "Apoteca_AutoUpdate" ^
    /tr "python \"%PROJECT_DIR%\scripts\auto_update.py\"" ^
    /sc hourly ^
    /mo 1 ^
    /st 09:00 ^
    /et 18:00 ^
    /d MON,TUE,WED,THU,FRI ^
    /f

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Tache planifiee creee avec succes!
    echo.
    echo Pour verifier: schtasks /query /tn "Apoteca_AutoUpdate"
    echo Pour supprimer: schtasks /delete /tn "Apoteca_AutoUpdate" /f
    echo Pour lancer manuellement: schtasks /run /tn "Apoteca_AutoUpdate"
) else (
    echo.
    echo ERREUR: Impossible de creer la tache.
    echo Essayez de lancer ce script en tant qu'administrateur.
)

echo.
pause
