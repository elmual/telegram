import os
from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta, time

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

# ------------------ KÖMƏKÇİ FUNKSİYALAR ------------------

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

    # faiz sırasına görə düzənlə
    report = report.sort_values(by="faiz", ascending=False)

    return report

def get_daily_report():
    today = datetime.now().date()
    start_time = datetime.combine(today, time(hour=0, minute=0, second=0))
    end_time = datetime.combine(today + timedelta(days=1), time(hour=0, minute=0, second=0))

    # hər iki bazadan bu günün cavabları
    answers_10_today = list(answers10.find({"timestamp": {"$gte": start_time, "$lt": end_time}}))
    answers_11_today = list(answers11.find({"timestamp": {"$gte": start_time, "$lt": end_time}}))
    all_answers = answers_10_today + answers_11_today

    # bütün tələbələr
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    df = pd.DataFrame(all_answers)
    if df.empty:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    return _generate_report(df, students)

def get_weekly_report():
    one_week_ago = datetime.now() - timedelta(days=7)
    
    answers_10 = list(answers10.find({"timestamp": {"$gte": one_week_ago}}))
    answers_11 = list(answers11.find({"timestamp": {"$gte": one_week_ago}}))
    all_answers = answers_10 + answers_11

    if not all_answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(all_answers)
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

    return _generate_report(df, students)

def get_overall_report():
    answers_10 = list(answers10.find())
    answers_11 = list(answers11.find())
    all_answers = answers_10 + answers_11

    if not all_answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(all_answers)
    students = {s["user_id"]: s for s in list(students10.find()) + list(students11.find())}

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
