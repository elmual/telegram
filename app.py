from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta

app = Flask(__name__)

# --- MongoDB qoşulma ---
MONGO_URI = "mongodb+srv://erlams:erlams423@cluster0.wwpua.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["telegram_bot_10"]
answers_collection = db["answers"]
students_collection = db["students"]

# ------------------ KÖMƏKÇİ FUNKSİYALAR ------------------

def _generate_report(df):
    """Verilmiş DataFrame-dən hesabat hazırlayır."""
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

    # ---- STUDENTS KOLLEKSİYASI İLƏ ƏLAVƏ EMAL ----
    students = {s["user_id"]: s for s in students_collection.find()}

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


def get_weekly_report():
    one_week_ago = datetime.now() - timedelta(days=7)
    cursor = answers_collection.find({"timestamp": {"$gte": one_week_ago}})
    answers = list(cursor)

    if not answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(answers)
    return _generate_report(df)


def get_overall_report():
    cursor = answers_collection.find()
    answers = list(cursor)

    if not answers:
        return pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz"])

    df = pd.DataFrame(answers)
    return _generate_report(df)

# ------------------ FLASK ROUTE ------------------

@app.route("/")
def index():
    weekly = get_weekly_report()
    overall = get_overall_report()

    return render_template(
        "index.html",
        weekly=weekly.to_dict(orient="records"),
        overall=overall.to_dict(orient="records")
    )

if __name__ == "__main__":
    app.run(debug=True)
