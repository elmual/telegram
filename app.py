import os
from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
from datetime import datetime, timedelta, time
import pytz

app = Flask(__name__)

# --- Bakı vaxt qurşağı ---
BAKU_TZ = pytz.timezone("Asia/Baku")

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
        ts = ans.get("timestamp")

        if ts:
            # Əgər timestamp string-disə, datetime-a çevir
            if isinstance(ts, str):
                try:
                    ts = datetime.fromisoformat(ts)
                except Exception:
                    try:
                        ts = datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        continue  # format tam fərqlidirsə, bu cavabı keç

            # Vaxt zonasını əlavə et
            if ts.tzinfo is None:
                ts = pytz.UTC.localize(ts).astimezone(BAKU_TZ)
            else:
                ts = ts.astimezone(BAKU_TZ)

            ans["timestamp"] = ts

        all_answers.append(ans)
    return all_answers



def _prepare_students():
    students = {}
    for s in list(students10.find({"user_id": {"$exists": True}})) + list(students11.find({"user_id": {"$exists": True}})):
        students[s["user_id"]] = s
    return students


def is_weekday(ts):
    return ts.weekday() < 5


def _generate_report(df, students, limits=None):
    if df.empty:
        report = pd.DataFrame(
            columns=[
                "user_id",
                "user_name",
                "sual_sayi",
                "duz",
                "sehv",
                "faiz",
                "cavabsiz",
            ]
        )
    else:
        df["correct"] = df["selected_option"] == df["correct_option"]
        report = (
            df.groupby("user_id")
            .agg(sual_sayi=("user_id", "count"), duz=("correct", "sum"))
            .reset_index()
        )

    report["user_name"] = report["user_id"].apply(
        lambda uid: students[uid]["full_name"]
    )
    report["sehv"] = report["sual_sayi"] - report["duz"]
    report["faiz"] = (
        ((report["duz"] / report["sual_sayi"]) * 100).round(0).fillna(0).astype(int)
    )

    if limits:
        report["cavabsiz"] = report.apply(
            lambda row: max(0, limits.get(row["user_id"], 0) - row["sual_sayi"]), axis=1
        )
        report["sual_sayi"] = report["sual_sayi"] + report["cavabsiz"]
    else:
        report["cavabsiz"] = 0

    for uid, student in students.items():
        if uid not in report["user_id"].values:
            sual_count = limits.get(uid, 0) if limits else 0
            report = pd.concat(
                [
                    report,
                    pd.DataFrame(
                        [
                            {
                                "user_id": uid,
                                "user_name": student.get("full_name"),
                                "sual_sayi": sual_count,
                                "duz": 0,
                                "sehv": 0,
                                "faiz": 0,
                                "cavabsiz": sual_count,
                            }
                        ]
                    ),
                ],
                ignore_index=True,
            )

    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]
    report = report.sort_values(
        by=["cavabsiz", "faiz", "duz"], ascending=[True, False, False]
    ).reset_index(drop=True)
    return report


# --- LIMITS ---
def get_daily_limits():
    limits = {s["user_id"]: 10 for s in students10.find({"user_id": {"$exists": True}})}
    limits.update({s["user_id"]: 12 for s in students11.find({"user_id": {"$exists": True}})})
    return limits


def get_weekly_limits():
    limits = {s["user_id"]: 10*5 for s in students10.find({"user_id": {"$exists": True}})}
    limits.update({s["user_id"]: 12*5 for s in students11.find({"user_id": {"$exists": True}})})
    return limits


def get_overall_limits(all_answers):
    limits = {}
    students = _prepare_students()

    bot10_polls = db10["polls"].find()
    max_idx_10 = max([p["question_idx"] for p in bot10_polls], default=-1)
    for s in students10.find({"user_id": {"$exists": True}}):
        limits[s["user_id"]] = max_idx_10 + 1

    bot11_polls = db11["polls"].find()
    max_idx_11 = max([p["question_idx"] for p in bot11_polls], default=-1)
    for s in students11.find({"user_id": {"$exists": True}}):
        limits[s["user_id"]] = max_idx_11 + 1

    return limits


# --- REPORTS ---
def get_daily_report():
    now = datetime.now(BAKU_TZ)
    today_start = BAKU_TZ.localize(datetime.combine(now.date(), time(7, 30)))
    if now < today_start:
        today_start -= timedelta(days=1)

    start = today_start
    end = start + timedelta(days=1)

    if start.weekday() >= 5:
        students = _prepare_students()
        data = []
        for s in students.values():
            data.append({
                "user_id": s["user_id"],
                "user_name": s["full_name"],
                "sual_sayi": 0,
                "duz": 0,
                "sehv": 0,
                "faiz": 0,
                "cavabsiz": 0
            })
        return pd.DataFrame(data)

    all_answers = [a for a in _prepare_answers() if start <= a["timestamp"] < end]
    students = _prepare_students()
    limits = get_daily_limits()
    df = pd.DataFrame(all_answers)
    return _generate_report(df, students, limits=limits)


def get_weekly_report():
    today = datetime.now(BAKU_TZ)
    start_of_week = today - timedelta(days=today.weekday())
    start = BAKU_TZ.localize(datetime.combine(start_of_week.date(), time.min))
    end = BAKU_TZ.localize(datetime.combine((start_of_week + timedelta(days=5)).date(), time.max))

    all_answers = [
        a
        for a in _prepare_answers()
        if start <= a["timestamp"] <= end and is_weekday(a["timestamp"])
    ]

    students = _prepare_students()
    limits = get_weekly_limits()
    df = pd.DataFrame(all_answers)
    used_limits = {uid: limits[uid] for uid in df["user_id"].unique()} if not df.empty else None
    return _generate_report(df, students, limits=used_limits)


def get_overall_report():
    all_answers = _prepare_answers()
    students = _prepare_students()
    limits = get_overall_limits(all_answers)
    df = pd.DataFrame(all_answers)
    return _generate_report(df, students, limits=limits)


# --- Excel-dən Quiz nəticələri oxuma ---
def get_quizz_data():
    file_path = os.path.join("static", "data", "quizz.xlsx")
    if not os.path.exists(file_path):
        print(f"Fayl tapılmadı: {file_path}")
        return pd.DataFrame()

    df = pd.read_excel(file_path, header=0)
    df = df.dropna(axis=1, how="all")
    if df.empty or len(df.columns) < 2:
        print("XƏTA: DataFrame boşdur və ya kifayət qədər sütun yoxdur!")
        return pd.DataFrame()

    df = df.rename(columns={df.columns[0]: "Abituriyentlərin ad və soyadı"})
    test_cols = [col for col in df.columns if col != "Abituriyentlərin ad və soyadı" and col != "Ortalama"]
    df[test_cols] = df[test_cols].apply(pd.to_numeric, errors="coerce")
    df["Ortalama imtahan nəticəsi %"] = df[test_cols].mean(axis=1).round(2)
    if "Ortalama" in df.columns:
        df = df.drop(columns=["Ortalama"])
    df.insert(0, "Sıra", range(1, len(df)+1))
    df = df.sort_values(by="Ortalama imtahan nəticəsi %", ascending=False)
    df["Sıra"] = range(1, len(df)+1)
    return df


# --- FLASK ROUTE ---
@app.route("/")
def index():
    daily = get_daily_report()
    weekly = get_weekly_report()
    overall = get_overall_report()
    quizz_data = get_quizz_data()
    return render_template(
        "index.html",
        daily=daily.to_dict(orient="records"),
        weekly=weekly.to_dict(orient="records"),
        overall=overall.to_dict(orient="records"),
        table_data=quizz_data.to_dict(orient="records"),
    )


if __name__ == "__main__":
    app.run(debug=True)
