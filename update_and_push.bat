@echo off
REM Lottery data update script
REM Run this periodically to fetch new draws and update the website

cd /d D:\ClaudeCode\lottery

echo === %date% %time% ===
echo [1/4] Crawling SSQ incremental...
C:\Users\ben\AppData\Local\Programs\Python\Python312\python.exe -m lottery_scraper crawl ssq --incremental
echo.
echo [2/4] Crawling DLT incremental...
C:\Users\ben\AppData\Local\Programs\Python\Python312\python.exe -m lottery_scraper crawl dlt --incremental
echo.
echo [3/4] Exporting to static JSON...
C:\Users\ben\AppData\Local\Programs\Python\Python312\python.exe export_web.py
echo.
echo [4/4] Pushing to GitHub...
git add docs/data/
git add data/lottery.db
git diff --cached --quiet && (
    echo No new data to push.
) || (
    git commit -m "auto update lottery data %date%"
    git push origin main
    echo Pushed successfully.
)
echo.
echo === Done ===
pause
