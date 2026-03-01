@echo off
cd /d "C:\Users\julie\OneDrive\Bureau\hippique-prediction"
.venv\Scripts\python.exe -c "from src.trading.scheduler import start_scheduler; start_scheduler()"
