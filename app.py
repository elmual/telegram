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
    """Bütün cavabları yığ və timestamp UTC-aware et"""
    all_answers = []

    for ans in list(answers10.find()) + list(answers11.find()):
        if ans.get("timestamp") and ans["timestamp"].tzinfo is None:
            ans["timestamp"] = ans["timestamp"].replace(tzinfo=timezone.utc)
        all_answers.append(ans)

    return all_answers

def _generate_report(df, students, daily_limits=None):
    """DataFrame və students dictionary-dən hesabat hazırla"""
    if df.empty:
        report = pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz", "cavabsiz"])
    else:
        df["correct"] = df["selected_option"] == df["correct_option"]
        report = df.groupby("user_id").agg(
            sual_sayi=("user_id", "count"),
            duz=("correct", "sum"),
            user_name=("user_name", "first")
        ).reset_index()
        report["sehv"] = report["sual_sayi"] - report["duz"]
        report["faiz"] = (report["duz"] / report["sual_sayi"] * 100).round(0).astype(int)

    # bütün tələbələri əlavə et (heç cavab verməyənləri də)
    for uid, student in students.items():
        if uid not in report["user_id"].values:
            report = pd.concat([report, pd.DataFrame([{
                "user_id": uid,
                "user_name": student.get("full_name") or student.get("name") or "Naməlum",
                "sual_sayi": 0,
                "duz": 0,
                "sehv": 0,
                "faiz": 0
            }])], ignore_index=True)

    # Cavabsız suallar
    if daily_limits:
        report["cavabsiz"] = report.apply(lambda row: daily_limits.get(row["user_id"], 0) - row["sual_sayi"], axis=1)
        report["cavabsiz"] = report["cavabsiz"].clip(lower=0)
    else:
        report["cavabsiz"] = 0

    # hidden olanları çıxart
    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]

    # faiz sırasına görə, sonra sual sayına görə düzənlə
    report = report.sort_values(by=["faiz", "sual_sayi"], ascending=[False, False])

    return report

def get_daily_report():
    today = datetime.now(timezone.utc).date()
    start_time = datetime.combine(today, time.min, tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=1)

    all_answers = [a for a in _prepare_answers() if start_time <= a["timestamp"] < end_time]
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    # hər tələbə üçün bot limitləri
    daily_limits = {}
    for s in list(students10.find()):
        daily_limits[s["user_id"]] = 10
    for s in list(students11.find()):
        daily_limits[s["user_id"]] = 12

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students, daily_limits=daily_limits)

def get_weekly_report():
    one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    all_answers = [a for a in _prepare_answers() if a["timestamp"] >= one_week_ago]
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students)

def get_overall_report():
    all_answers = _prepare_answers()
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students)

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
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port, debug=True)
