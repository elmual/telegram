import os
from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta, time, timezone

app = Flask(__name__)

# --- MongoDB qoşulmalar ---
MONGO_URI = "mongodb+srv://erlams:erlams423@cluster0.wwpua.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)

# telegram_bot_10
db10 = client["telegram_bot_10"]
answers10 = db10["answers"]
students10 = db10["students"]

# telegram_bot_11
db11 = client["telegram_bot_11"]
answers11 = db11["answers"]
students11 = db11["students"]

# --- KÖMƏKÇİ FUNKSİYALAR ---
def _prepare_answers():
    all_answers = []
    for ans in list(answers10.find()) + list(answers11.find()):
        if ans.get("timestamp") and ans["timestamp"].tzinfo is None:
            ans["timestamp"] = ans["timestamp"].replace(tzinfo=timezone.utc)
        all_answers.append(ans)
    return all_answers

def _prepare_students():
    students = {}
    for s in list(students10.find()) + list(students11.find()):
        students[s["user_id"]] = s
    return students

def is_weekday(ts):
    """Yalnız iş günləri (Mon-Fri)"""
    return ts.weekday() < 5

def _generate_report(df, students, limits=None):
    """Report generator"""
    if df.empty:
        report = pd.DataFrame(columns=["user_id","user_name","sual_sayi","duz","sehv","faiz","cavabsiz"])
    else:
        df["correct"] = df["selected_option"] == df["correct_option"]
        report = df.groupby("user_id").agg(
            sual_sayi=("user_id", "count"),
            duz=("correct", "sum")
        ).reset_index()

    report["user_name"] = report["user_id"].apply(lambda uid: students[uid]["full_name"])
    report["sehv"] = report["sual_sayi"] - report["duz"]
    report["faiz"] = ((report["duz"]/report["sual_sayi"])*100).round(0).fillna(0).astype(int)

    if limits:
        report["cavabsiz"] = report.apply(lambda row: max(0, limits.get(row["user_id"], 0) - row["sual_sayi"]), axis=1)
        report["sual_sayi"] = report["sual_sayi"] + report["cavabsiz"]
    else:
        report["cavabsiz"] = 0

    # heç cavab verməyən tələbələr
    for uid, student in students.items():
        if uid not in report["user_id"].values:
            sual_count = limits.get(uid, 0) if limits else 0
            report = pd.concat([report, pd.DataFrame([{
                "user_id": uid,
                "user_name": student.get("full_name"),
                "sual_sayi": sual_count,
                "duz": 0,
                "sehv": 0,
                "faiz": 0,
                "cavabsiz": sual_count
            }])], ignore_index=True)

    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]
    report = report.sort_values(by=["cavabsiz","faiz","duz"], ascending=[True,False,False]).reset_index(drop=True)
    return report

# --- LIMITS ---
def get_daily_limits():
    limits = {}
    for s in list(students10.find()):
        limits[s["user_id"]] = 10
    for s in list(students11.find()):
        limits[s["user_id"]] = 12
    return limits

def get_weekly_limits():
    limits = {}
    for s in list(students10.find()):
        limits[s["user_id"]] = 10 * 5  # iş günləri
    for s in list(students11.find()):
        limits[s["user_id"]] = 12 * 5
    return limits

def get_overall_limits(all_answers):
    """Ümumi limit: polls-da maksimum question_idx + 1"""
    limits = {}
    students = _prepare_students()

    # Bot10
    bot10_polls = db10["polls"].find()
    max_idx_10 = max([p["question_idx"] for p in bot10_polls], default=-1)
    for s in students10.find():
        limits[s["user_id"]] = max_idx_10 + 1

    # Bot11
    bot11_polls = db11["polls"].find()
    max_idx_11 = max([p["question_idx"] for p in bot11_polls], default=-1)
    for s in students11.find():
        limits[s["user_id"]] = max_idx_11 + 1

    return limits

# --- REPORTS ---
def get_daily_report():
    today = datetime.now(timezone.utc).date()
    start = datetime.combine(today, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    all_answers = [a for a in _prepare_answers() if start <= a["timestamp"] < end]
    students = _prepare_students()
    limits = get_daily_limits()
    df = pd.DataFrame(all_answers)
    return _generate_report(df, students, limits=limits)

def get_weekly_report():
    today = datetime.now(timezone.utc)
    start_of_week = today - timedelta(days=today.weekday())
    start = datetime.combine(start_of_week.date(), time.min, tzinfo=timezone.utc)
    end = datetime.combine((start_of_week + timedelta(days=5)).date(), time.max, tzinfo=timezone.utc)

    all_answers = [
        a for a in _prepare_answers()
        if start <= a["timestamp"] <= end and is_weekday(a["timestamp"])
    ]

    students = _prepare_students()
    limits = get_weekly_limits()

    # yalnız bu həftə cavab verən tələbələr üçün limit tətbiq edirik
    df = pd.DataFrame(all_answers)
    used_limits = {uid: limits[uid] for uid in df["user_id"].unique()} if not df.empty else None
    return _generate_report(df, students, limits=used_limits)


def get_overall_report():
    all_answers = _prepare_answers()
    students = _prepare_students()
    limits = get_overall_limits(all_answers)
    df = pd.DataFrame(all_answers)
    return _generate_report(df, students, limits=limits)

# --- FLASK ROUTE ---
@app.route("/")
def index():
    daily = get_daily_report()
    weekly = get_weekly_report()
    overall = get_overall_report()
    return render_template(
        "index.html",
        daily=daily.to_dict(orient="records"),
        weekly=weekly.to_dict(orient="records"),
        overall=overall.to_dict(orient="records")
    )

if __name__ == "__main__":
    port = int(os.environ.get("PORT",5000))
    app.run(host="0.0.0.0", port=port, debug=True)
