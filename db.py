from utils import *


def init_db():
    conn = sqlite3.connect(DB_FILE)
    cursor = conn.cursor()
    c.executescript('''
        CREATE TABLE IF NOT EXISTS health_stats (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            weight_kg REAL, bodyfat_pct REAL, muscle_pct REAL, notes TEXT
        );
        CREATE TABLE IF NOT EXISTS workout_logs (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            type TEXT, details JSON, duration_min INTEGER, notes TEXT
        );
        CREATE TABLE IF NOT EXISTS nutrition_logs (
            id INTEGER PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            calories INTEGER, protein_g REAL, carbs_g REAL, fat_g REAL, source TEXT, notes TEXT
        );
    ''')
    conn.commit()
    conn.close()


def add_row(table: str, data: dict): 
    init_db()
    conn = sqlite3.connect(DB_FILE)
    cols = ', '.join(data.keys())
    vals = ', '.join(['?'] * len(data))
    conn.execute(f"INSERT INTO {table} ({cols}) VALUES ({vals})", list(data.values()))
    conn.commit()
    conn.close()
    return {"status": "logged", "timestamp": datetime.utcnow().isoformat()}

def get_trends(days: int = 30):
    conn = sqlite3.connect(DB_FILE)
    df_h = pd.read_sql(f"SELECT * FROM health_stats ORDER BY timestamp DESC LIMIT {days*2}", conn)
    df_n = pd.read_sql(f"SELECT * FROM nutrition_logs ORDER BY timestamp DESC LIMIT {days*2}", conn)
    conn.close()
    # simple trends
    return {
        "weight_change_kg": round(df_h['weight_kg'].iloc[0] - df_h['weight_kg'].iloc[-1], 1) if len(df_h)>1 else 0,
        "avg_protein_g": round(df_n['protein_g'].mean(), 1) if not df_n.empty else 0,
        "trend_note": "Strong progress" if df_h['weight_kg'].iloc[0] < df_h['weight_kg'].iloc[-1] else "Monitor"
    }
