import sqlite3
from pathlib import Path

BASE = Path(__file__).resolve().parent
DB_MAIN = BASE / "CR_main.db"
DB_IND = BASE / "CR_individual.db"
DB_NON = BASE / "CR_non_individual.db"


def fetch(db_path, query):
    if not db_path.exists():
        print(f"{db_path.name} NOT FOUND")
        return
    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(query).fetchall()
        if not rows:
            print("(no rows)")
            return
        for r in rows:
            print(dict(r))
    except Exception as e:
        print("ERROR:", e)
    finally:
        try:
            conn.close()
        except:
            pass


print("=== clients_main (last 10) ===")
fetch(DB_MAIN, "SELECT * FROM clients_main ORDER BY client_id DESC LIMIT 10;")

print("\n=== individual_details (last 10) ===")
fetch(DB_IND, "SELECT * FROM individual_details ORDER BY individual_id DESC LIMIT 10;")

print("\n=== kyc_individual (last 10) ===")
fetch(DB_IND, "SELECT * FROM kyc_individual ORDER BY kyc_individual_id DESC LIMIT 10;")

print("\n=== non_individual_details (last 10) ===")
fetch(DB_NON, "SELECT * FROM non_individual_details ORDER BY non_individual_id DESC LIMIT 10;")

print("\n=== kyc_non_individual (last 10) ===")
fetch(DB_NON, "SELECT * FROM kyc_non_individual ORDER BY kyc_non_individual_id DESC LIMIT 10;")
