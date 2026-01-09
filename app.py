import os
from flask import Flask, render_template
from pymongo import MongoClient
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, time
import pytz

app = Flask(__name__)

# --- Bakı vaxt qurşağı ---
BAKU_TZ = pytz.timezone("Asia/Baku")

# --- MongoDB qoşulmalar ---
# MONGO_URI = "mongodb+srv://erlams:erlams423@cluster0.wwpua.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set")

client = MongoClient(MONGO_URI)

db10 = client["telegram_bot_10"]
answers10 = db10["answers"]
students10 = db10["students"]

db11 = client["telegram_bot_11"]
answers11 = db11["answers"]
students11 = db11["students"]

# --- KÖMƏKÇİ FUNKSİYALAR ---

def _prepare_answers(start_date=None, end_date=None):
    all_answers = []
    query = {}
    if start_date and end_date:
        query = {"timestamp": {"$gte": start_date, "$lt": end_date}}

    projection = {"user_id": 1, "selected_option": 1, "correct_option": 1, "timestamp": 1}

    for coll in [answers10, answers11]:
        for ans in coll.find(query, projection):
            ts = ans.get("timestamp")
            if ts:
                if isinstance(ts, str):
                    try: ts = datetime.fromisoformat(ts)
                    except: continue
                # Vaxt zonası yoxdursa, Bakı vaxtına çeviririk
                if ts.tzinfo is None:
                    ts = pytz.UTC.localize(ts).astimezone(BAKU_TZ)
                else:
                    ts = ts.astimezone(BAKU_TZ)
                ans["timestamp"] = ts
            all_answers.append(ans)
    return all_answers

def _prepare_students():
    students = {}
    for coll in [students10, students11]:
        for s in coll.find({"user_id": {"$exists": True}}):
            students[s["user_id"]] = s
    return students

def is_weekday(ts):
    return ts.weekday() < 5

def _generate_report(df, students, limits=None):
    if df.empty:
        report = pd.DataFrame(columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz", "cavabsiz"])
    else:
        # Əgər df-də artıq "duz" sütunu yoxdursa (yəni daily/weekly-dirsə), hesablama apar
        if "duz" not in df.columns:
            df["correct"] = df["selected_option"] == df["correct_option"]
            report = df.groupby("user_id").agg(sual_sayi=("user_id", "count"), duz=("correct", "sum")).reset_index()
        else:
            report = df

    report["user_name"] = report["user_id"].apply(lambda uid: students.get(uid, {}).get("full_name") or students.get(uid, {}).get("name") or "Naməlum")
    report["duz"] = pd.to_numeric(report.get("duz", 0)).fillna(0)
    report["sual_sayi"] = pd.to_numeric(report.get("sual_sayi", 0)).fillna(0)
    report["sehv"] = (report["sual_sayi"] - report["duz"]).clip(lower=0)
    report["faiz"] = np.where(report["sual_sayi"] > 0, ((report["duz"] / report["sual_sayi"]) * 100), 0)
    report["faiz"] = report["faiz"].round(0).astype(int)

    if limits:
        report["cavabsiz"] = report.apply(lambda row: max(0, limits.get(row["user_id"], 0) - row["sual_sayi"]), axis=1)
        report["sual_sayi"] = report["sual_sayi"] + report["cavabsiz"]
    else:
        report["cavabsiz"] = 0

    # Siyahıda olmayan tələbələri əlavə et
    for uid, student in students.items():
        if uid not in report["user_id"].values:
            sual_count = limits.get(uid, 0) if limits else 0
            new_row = pd.DataFrame([{"user_id": uid, "user_name": student.get("full_name") or student.get("name") or "Naməlum", 
                                     "sual_sayi": sual_count, "duz": 0, "sehv": 0, "faiz": 0, "cavabsiz": sual_count}])
            report = pd.concat([report, new_row], ignore_index=True)

    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]
    return report.sort_values(by=["cavabsiz", "faiz", "duz"], ascending=[True, False, False]).reset_index(drop=True)

# --- LIMITS ---
def get_daily_limits():
    limits = {s["user_id"]: 10 for s in students10.find({"user_id": {"$exists": True}})}
    limits.update({s["user_id"]: 12 for s in students11.find({"user_id": {"$exists": True}})})
    return limits

def get_weekly_limits():
    return {k: v * 5 for k, v in get_daily_limits().items()}

def get_overall_limits():
    limits = {}
    for db, s_coll in [(db10, students10), (db11, students11)]:
        max_idx = max([p["question_idx"] for p in db["polls"].find({}, {"question_idx": 1})], default=-1)
        for s in s_coll.find({"user_id": {"$exists": True}}, {"user_id": 1}):
            limits[s["user_id"]] = max_idx + 1
    return limits

# --- REPORTS ---

def get_daily_report():
    now = datetime.now(BAKU_TZ)
    # Sənin orijinal vaxt məntiqin: 07:30-da gün dəyişir
    today_start = BAKU_TZ.localize(datetime.combine(now.date(), time(7, 30)))
    if now < today_start: today_start -= timedelta(days=1)
    
    start, end = today_start, today_start + timedelta(days=1)
    
    # Həftə sonu (Şənbə-Bazar) 0 göstərsin
    if start.weekday() >= 5: 
        return _generate_report(pd.DataFrame(), _prepare_students(), limits=get_daily_limits())
    
    all_answers = _prepare_answers(start, end)
    return _generate_report(pd.DataFrame(all_answers), _prepare_students(), limits=get_daily_limits())

def get_weekly_report():
    today = datetime.now(BAKU_TZ)
    start_of_week = today - timedelta(days=today.weekday())
    start = BAKU_TZ.localize(datetime.combine(start_of_week.date(), time.min))
    # Cümə axşamına qədər olan limit (5 gün)
    end = BAKU_TZ.localize(datetime.combine((start_of_week + timedelta(days=5)).date(), time.max))
    
    # Həftə içi cavablarını çək
    all_answers = [a for a in _prepare_answers(start, end) if is_weekday(a["timestamp"])]
    return _generate_report(pd.DataFrame(all_answers), _prepare_students(), limits=get_weekly_limits())

def get_overall_report():
    # Sürətli MongoDB Aggregation
    pipeline = [
        {"$project": {"user_id": 1, "is_correct": {"$cond": [{"$eq": ["$selected_option", "$correct_option"]}, 1, 0]}}},
        {"$group": {"_id": "$user_id", "sual_sayi": {"$sum": 1}, "duz": {"$sum": "$is_correct"}}}
    ]
    results = []
    for coll in [answers10, answers11]:
        results.extend(list(coll.aggregate(pipeline)))
    
    df_results = pd.DataFrame(results).rename(columns={"_id": "user_id"}) if results else pd.DataFrame()
    return _generate_report(df_results, _prepare_students(), limits=get_overall_limits())

def get_quizz_data():
    file_path = os.path.join("static", "data", "quizz.xlsx")
    if not os.path.exists(file_path): return pd.DataFrame()
    try:
        df = pd.read_excel(file_path, header=0)
        df = df.dropna(axis=1, how="all")
        df = df.rename(columns={df.columns[0]: "Abituriyentlərin ad və soyadı"})
        test_cols = [col for col in df.columns if col not in ["Abituriyentlərin ad və soyadı", "Ortalama"]]
        df[test_cols] = df[test_cols].apply(pd.to_numeric, errors="coerce")
        df["Ortalama imtahan nəticəsi %"] = df[test_cols].mean(axis=1).round(2)
        if "Ortalama" in df.columns: df = df.drop(columns=["Ortalama"])
        df.insert(0, "Sıra", range(1, len(df) + 1))
        return df.sort_values(by="Ortalama imtahan nəticəsi %", ascending=False)
    except: return pd.DataFrame()

@app.route("/")
def index():
    try:
        return render_template(
            "index.html",
            daily=get_daily_report().to_dict(orient="records"),
            weekly=get_weekly_report().to_dict(orient="records"),
            overall=get_overall_report().to_dict(orient="records"),
            table_data=get_quizz_data().to_dict(orient="records")
        )
    except Exception as e:
        print(f"Xəta: {e}")
        return "Sistemdə xəta baş verdi.", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    app.run(host="0.0.0.0", port=port, debug=True)