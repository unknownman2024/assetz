import os
import shutil
from datetime import datetime, timedelta
import pytz

# ================= CONFIG =================
BASE_PATHS = [
    "advance/data",
    "daily/data"
]

IST = pytz.timezone("Asia/Kolkata")

# 📅 Date range:
# Start = 5 days ago (IST)
# End   = yesterday (IST)
START_DATE = (datetime.now(IST) - timedelta(days=5)).strftime("%Y%m%d")
END_DATE   = (datetime.now(IST) - timedelta(days=1)).strftime("%Y%m%d")

FILES_TO_DELETE = [
    *(f"detailed{i}.json" for i in range(1, 10)),
    *(f"movie_summary{i}.json" for i in range(1, 10)),
]

# ================= HELPERS =================
def daterange(start, end):
    cur = datetime.strptime(start, "%Y%m%d")
    end = datetime.strptime(end, "%Y%m%d")
    while cur <= end:
        yield cur.strftime("%Y%m%d")
        cur += timedelta(days=1)

# ================= CLEANUP =================
deleted_files = 0
deleted_dirs = 0

print(f"🗓 Cleaning shard files from {START_DATE} → {END_DATE} (IST)\n")

for date in daterange(START_DATE, END_DATE):
    for base in BASE_PATHS:
        folder = os.path.join(base, date)
        if not os.path.isdir(folder):
            continue

        # delete file shards
        for fname in FILES_TO_DELETE:
            path = os.path.join(folder, fname)
            if os.path.exists(path):
                os.remove(path)
                deleted_files += 1
                print(f"🗑 Deleted file: {path}")

        # delete `logs` directory entirely
        logs_dir = os.path.join(folder, "logs")
        if os.path.isdir(logs_dir):
            shutil.rmtree(logs_dir)
            deleted_dirs += 1
            print(f"🗑 Deleted directory: {logs_dir}")

print("\n=======================")
print(f" Files removed: {deleted_files}")
print(f" Folders removed: {deleted_dirs}")
print(f" Cleanup complete.")
