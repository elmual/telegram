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
MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    raise RuntimeError("MONGO_URI environment variable is not set")

# MONGO_URI = "mongodb+srv://erlams:erlams423@cluster0.wwpua.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
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
def _prepare_answers(start_date=None, end_date=None):
    """
    Optimallaşdırılmış funksiya: start_date və end_date verilərsə, 
    bazadan yalnız həmin aralıqdakı datanı çəkir (RAM-ı qoruyur).
    """
    all_answers = []
    
    # MongoDB Query filtri
    query = {}
    if start_date and end_date:
        query = {"timestamp": {"$gte": start_date, "$lt": end_date}}

    # list(answers10.find()) əvəzinə birbaşa cursor üzərində dövr edirik
    for coll in [answers10, answers11]:
        for ans in coll.find(query):
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
                            continue 

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
    query = {"user_id": {"$exists": True}}
    for coll in [students10, students11]:
        for s in coll.find(query):
            students[s["user_id"]] = s
    return students


def is_weekday(ts):
    return ts.weekday() < 5


def _generate_report(df, students, limits=None):
    if df.empty:
        # Boş sətirləri olan ilkin report yaradırıq ki, təmiz görünsün
        report = pd.DataFrame(
            columns=["user_id", "user_name", "sual_sayi", "duz", "sehv", "faiz", "cavabsiz"]
        )
    else:
        df["correct"] = df["selected_option"] == df["correct_option"]
        report = (
            df.groupby("user_id")
            .agg(sual_sayi=("user_id", "count"), duz=("correct", "sum"))
            .reset_index()
        )

    # 🔹 user_name əlavə et
    report["user_name"] = report["user_id"].apply(
        lambda uid: students.get(uid, {}).get("full_name")
        or students.get(uid, {}).get("name")
        or "Naməlum"
    )

    # 🔹 Rəqəmləri tam numeric-ə çevir
    report["duz"] = pd.to_numeric(report.get("duz", 0), errors="coerce").fillna(0)
    report["sual_sayi"] = pd.to_numeric(
        report.get("sual_sayi", 0), errors="coerce"
    ).fillna(0)

    # 🔹 Səhvlər
    report["sehv"] = (report["sual_sayi"] - report["duz"]).clip(lower=0)

    # 🔹 Faiz hesabla
    report["faiz"] = np.where(
        report["sual_sayi"] > 0, ((report["duz"] / report["sual_sayi"]) * 100), 0
    )
    report["faiz"] = (
        pd.to_numeric(report["faiz"], errors="coerce").fillna(0).round(0).astype(int)
    )

    # 🔹 cavabsiz suallar
    if limits:
        report["cavabsiz"] = report.apply(
            lambda row: max(0, limits.get(row["user_id"], 0) - row["sual_sayi"]), axis=1
        )
        report["sual_sayi"] = report["sual_sayi"] + report["cavabsiz"]
    else:
        report["cavabsiz"] = 0

    # 🔹 cavabsiz tələbələr üçün sətr əlavə et
    for uid, student in students.items():
        if uid not in report["user_id"].values:
            sual_count = limits.get(uid, 0) if limits else 0
            new_row = pd.DataFrame([{
                "user_id": uid,
                "user_name": student.get("full_name") or student.get("name") or "Naməlum",
                "sual_sayi": sual_count,
                "duz": 0, "sehv": 0, "faiz": 0, "cavabsiz": sual_count
            }])
            report = pd.concat([report, new_row], ignore_index=True)

    # 🔹 gizli tələbələri çıxart
    hidden_ids = [uid for uid, st in students.items() if st.get("hidden")]
    report = report[~report["user_id"].isin(hidden_ids)]

    # 🔹 sıralama
    report = report.sort_values(
        by=["cavabsiz", "faiz", "duz"], ascending=[True, False, False]
    ).reset_index(drop=True)

    return report


# --- LIMITS ---
def get_daily_limits():
    limits = {s["user_id"]: 10 for s in students10.find({"user_id": {"$exists": True}})}
    limits.update(
        {s["user_id"]: 12 for s in students11.find({"user_id": {"$exists": True}})}
    )
    return limits


def get_weekly_limits():
    limits = {s["user_id"]: 10 * 5 for s in students10.find({"user_id": {"$exists": True}})}
    limits.update(
        {s["user_id"]: 12 * 5 for s in students11.find({"user_id": {"$exists": True}})}
    )
    return limits


def get_overall_limits():
    limits = {}
    bot10_polls = list(db10["polls"].find())
    max_idx_10 = max([p["question_idx"] for p in bot10_polls], default=-1)
    for s in students10.find({"user_id": {"$exists": True}}):
        limits[s["user_id"]] = max_idx_10 + 1

    bot11_polls = list(db11["polls"].find())
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

    if start.weekday() >= 5: # Şənbə-Bazar
        return pd.DataFrame()

    all_answers = _prepare_answers(start, end)
    students = _prepare_students()
    limits = get_daily_limits()
    return _generate_report(pd.DataFrame(all_answers), students, limits=limits)


def get_weekly_report():
    today = datetime.now(BAKU_TZ)
    start_of_week = today - timedelta(days=today.weekday())
    start = BAKU_TZ.localize(datetime.combine(start_of_week.date(), time.min))
    end = BAKU_TZ.localize(datetime.combine((start_of_week + timedelta(days=5)).date(), time.max))

    all_answers = _prepare_answers(start, end)
    # Həftə içi filtrini saxlayırıq
    all_answers = [a for a in all_answers if is_weekday(a["timestamp"])]

    students = _prepare_students()
    limits = get_weekly_limits()
    return _generate_report(pd.DataFrame(all_answers), students, limits=limits)


def get_overall_report():
    all_answers = _prepare_answers() # Ümumi olduğu üçün hamısı çəkilir
    students = _prepare_students()
    limits = get_overall_limits()
    return _generate_report(pd.DataFrame(all_answers), students, limits=limits)


# --- Excel-dən Quiz nəticələri oxuma ---
def get_quizz_data():
    file_path = os.path.join("static", "data", "quizz.xlsx")
    if not os.path.exists(file_path):
        return pd.DataFrame()

    try:
        df = pd.read_excel(file_path, header=0)
        df = df.dropna(axis=1, how="all")
        if df.empty or len(df.columns) < 2:
            return pd.DataFrame()

        df = df.rename(columns={df.columns[0]: "Abituriyentlərin ad və soyadı"})
        test_cols = [col for col in df.columns if col not in ["Abituriyentlərin ad və soyadı", "Ortalama"]]
        
        df[test_cols] = df[test_cols].apply(pd.to_numeric, errors="coerce")
        df["Ortalama imtahan nəticəsi %"] = df[test_cols].mean(axis=1).round(2)
        
        if "Ortalama" in df.columns:
            df = df.drop(columns=["Ortalama"])
            
        df.insert(0, "Sıra", range(1, len(df) + 1))
        df = df.sort_values(by="Ortalama imtahan nəticəsi %", ascending=False)
        df["Sıra"] = range(1, len(df) + 1)
        return df
    except:
        return pd.DataFrame()


# --- FLASK ROUTE ---
@app.route("/")
def index():
    daily = get_daily_report()
    weekly = get_weekly_report()
    overall = get_overall_report()
    quizz_data = get_quizz_data()
    
    return render_template(
        "index.html",
        daily=daily.to_dict(orient="records") if not daily.empty else [],
        weekly=weekly.to_dict(orient="records") if not weekly.empty else [],
        overall=overall.to_dict(orient="records") if not overall.empty else [],
        table_data=quizz_data.to_dict(orient="records") if not quizz_data.empty else [],
    )


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 10000))
    # debug=True lokalda kəsilmələri görmək üçün qalmalıdır
    app.run(host="0.0.0.0", port=port, debug=True)
