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
pending10 = db10.get_collection("pending_answers")  # Pending cavablar

# telegram_bot_11
db11 = client["telegram_bot_11"]
answers11 = db11["answers"]
students11 = db11["students"]
pending11 = db11.get_collection("pending_answers")  # Pending cavablar

# ------------------ KÖMƏKÇİ FUNKSİYALAR ------------------

def _prepare_answers():
    """Bütün cavabları (answers + pending) yığ və timestamp UTC-aware et"""
    all_answers = []

    for ans in list(answers10.find()) + list(pending10.find()):
        if ans.get("timestamp") and ans["timestamp"].tzinfo is None:
            ans["timestamp"] = ans["timestamp"].replace(tzinfo=timezone.utc)
        all_answers.append(ans)

    for ans in list(answers11.find()) + list(pending11.find()):
        if ans.get("timestamp") and ans["timestamp"].tzinfo is None:
            ans["timestamp"] = ans["timestamp"].replace(tzinfo=timezone.utc)
        all_answers.append(ans)

    return all_answers

def _generate_report(df, students):
    """DataFrame və students dictionary-dən hesabat hazırla."""
    if df.empty:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df["correct"] = df["selected_option"] == df["correct_option"]

    report = df.groupby("user_id").agg(
        sual_sayi=("user_id", "count"),
        duz=("correct", "sum"),
        user_name=("user_name", "first")
    ).reset_index()

    report["sehv"] = report["sual_sayi"] - report["duz"]
    report["faiz"] = (report["duz"] / report["sual_sayi"] * 100).round(0).astype(int)

    # full_name varsa, göstər
    report["user_name"] = report.apply(
        lambda row: students.get(row["user_id"], {}).get("full_name") or row["user_name"],
        axis=1
    )

    # hidden olanları çıxart
    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]

    # faiz sırasına görə, sonra sual sayına görə düzənlə
    report = report.sort_values(by=["faiz", "sual_sayi"], ascending=[False, False])

    return report

def get_daily_report():
    today = datetime.now(timezone.utc).date()
    start_time = datetime.combine(today, time(hour=0, minute=0, second=0), tzinfo=timezone.utc)
    end_time = start_time + timedelta(days=1)

    all_answers = [a for a in _prepare_answers() if start_time <= a["timestamp"] < end_time]

    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    if not all_answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students)

def get_weekly_report():
    one_week_ago = datetime.now(timezone.utc) - timedelta(days=7)
    all_answers = [a for a in _prepare_answers() if a["timestamp"] >= one_week_ago]

    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    if not all_answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students)

def get_overall_report():
    all_answers = _prepare_answers()
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    if not all_answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(all_answers)
    return _generate_report(df, students)

# ------------------ FLASK ROUTE ------------------

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
