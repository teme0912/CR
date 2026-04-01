"""
migrate_to_mysql.py

Copy local SQLite DB files to a MySQL database.

Usage example:
  .\.venv\Scripts\python.exe migrate_to_mysql.py \
      --host 127.0.0.1 --port 3306 --user root --database cr_db

If --password is omitted the script will prompt for it. The script will
create the database and tables if they don't exist, then insert rows from
`CR_main.db`, `CR_individual.db` and `CR_non_individual.db`.

CAUTION: Back up your MySQL database before running this on production.
"""

import argparse
import getpass
import os
import sqlite3
from pathlib import Path
import sys

try:
    import pymysql
except Exception as e:
    print("pymysql is required. Install with: pip install pymysql")
    raise


BASE = Path(__file__).resolve().parent
DB_MAIN = BASE / "CR_main.db"
DB_IND = BASE / "CR_individual.db"
DB_NON = BASE / "CR_non_individual.db"


def parse_args():
    p = argparse.ArgumentParser(description="Migrate local CR SQLite DBs to MySQL")
    p.add_argument("--host", default=os.getenv("MYSQL_HOST", "127.0.0.1"))
    p.add_argument("--port", type=int, default=int(os.getenv("MYSQL_PORT", "3306")))
    p.add_argument("--user", default=os.getenv("MYSQL_USER", "root"))
    p.add_argument("--password", default=os.getenv("MYSQL_PASSWORD"))
    p.add_argument("--database", default=os.getenv("MYSQL_DB", "cr_db"))
    p.add_argument("--skip-data", action="store_true", help="Create schema only, do not copy data")
    return p.parse_args()


def prompt_for_password(args):
    if not args.password:
        args.password = getpass.getpass(f"MySQL password for {args.user}@{args.host}:{args.port}: ")


def ensure_database(conn, dbname: str):
    with conn.cursor() as cur:
        cur.execute(f"CREATE DATABASE IF NOT EXISTS `{dbname}` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;")
    conn.commit()
    print(f"Ensured database `{dbname}` exists")


def create_schema(mysql_conn):
    ddls = [
        """
        CREATE TABLE IF NOT EXISTS clients_main (
          client_id INT PRIMARY KEY,
          category VARCHAR(50) NOT NULL,
          client_type VARCHAR(100) NOT NULL,
          risk_level VARCHAR(20) NOT NULL DEFAULT 'Medium',
          status VARCHAR(50) NOT NULL DEFAULT 'Pending Approval',
          approved_by VARCHAR(255),
          approved_at DATETIME NULL,
          created_at DATETIME NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,

        """
        CREATE TABLE IF NOT EXISTS individual_details (
          individual_id INT PRIMARY KEY,
          client_id INT NOT NULL,
          first_name VARCHAR(255) NOT NULL,
          last_name VARCHAR(255) NOT NULL,
          gender VARCHAR(50) NOT NULL,
          date_of_birth DATE NOT NULL,
          age INT NOT NULL,
          phone VARCHAR(50) NOT NULL,
          email VARCHAR(255),
          address TEXT,
          UNIQUE KEY uniq_individual_client (client_id),
          FOREIGN KEY (client_id) REFERENCES clients_main(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,

        """
        CREATE TABLE IF NOT EXISTS kyc_individual (
          kyc_individual_id INT PRIMARY KEY,
          client_id INT NOT NULL,
          id_type VARCHAR(100) NOT NULL,
          id_number VARCHAR(255) NOT NULL,
          occupation VARCHAR(255),
          source_of_funds VARCHAR(255),
          UNIQUE KEY uniq_kyc_individual_client (client_id),
          FOREIGN KEY (client_id) REFERENCES clients_main(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,

        """
        CREATE TABLE IF NOT EXISTS non_individual_details (
          non_individual_id INT PRIMARY KEY,
          client_id INT NOT NULL,
          organization_name VARCHAR(255) NOT NULL,
          registration_number VARCHAR(255) NOT NULL,
          contact_person VARCHAR(255) NOT NULL,
          industry VARCHAR(255),
          phone VARCHAR(50) NOT NULL,
          email VARCHAR(255),
          address TEXT,
          UNIQUE KEY uniq_non_individual_client (client_id),
          FOREIGN KEY (client_id) REFERENCES clients_main(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,

        """
        CREATE TABLE IF NOT EXISTS kyc_non_individual (
          kyc_non_individual_id INT PRIMARY KEY,
          client_id INT NOT NULL,
          tax_id VARCHAR(255) NOT NULL,
          certificate_of_incorporation VARCHAR(255),
          business_license_number VARCHAR(255),
          beneficial_owner VARCHAR(255),
          UNIQUE KEY uniq_kyc_non_individual_client (client_id),
          FOREIGN KEY (client_id) REFERENCES clients_main(client_id) ON DELETE CASCADE
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    ]

    with mysql_conn.cursor() as cur:
        for s in ddls:
            cur.execute(s)
    mysql_conn.commit()
    print("MySQL schema created/ensured")


def copy_rows(sqlite_conn, mysql_conn, select_sql, insert_sql, cols):
    cursor = sqlite_conn.execute(select_sql)
    rows = cursor.fetchall()
    if not rows:
        print(f"No rows returned for: {select_sql}")
        return 0

    data = []
    for r in rows:
        # sqlite3.Row supports mapping access
        data.append(tuple(r[c] for c in cols))

    with mysql_conn.cursor() as cur:
        cur.executemany(insert_sql, data)
    mysql_conn.commit()
    print(f"Inserted {len(data)} rows into target using: {insert_sql.split()[2]}")
    return len(data)


def set_auto_increment(mysql_conn, table: str, id_col: str):
    with mysql_conn.cursor() as cur:
        cur.execute(f"SELECT MAX({id_col}) FROM {table}")
        r = cur.fetchone()
        max_id = r[0] if r and r[0] is not None else 0
        next_id = max_id + 1
        cur.execute(f"ALTER TABLE {table} AUTO_INCREMENT = %s", (next_id,))
    mysql_conn.commit()
    print(f"Set AUTO_INCREMENT for {table} to {next_id}")


def main():
    args = parse_args()
    prompt_for_password(args)

    # connect to server (no database) so we can create DB if needed
    admin_conn = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, charset='utf8mb4', autocommit=True)
    try:
        ensure_database(admin_conn, args.database)
    finally:
        admin_conn.close()

    # connect to the target database
    mysql_conn = pymysql.connect(host=args.host, port=args.port, user=args.user, password=args.password, database=args.database, charset='utf8mb4')

    try:
        create_schema(mysql_conn)

        if args.skip_data:
            print("Schema created. Exiting (skip-data specified)")
            return

        # open sqlite DBs
        if not DB_MAIN.exists():
            print(f"Source database {DB_MAIN} not found. Aborting.")
            return
        s_main = sqlite3.connect(DB_MAIN)
        s_main.row_factory = sqlite3.Row

        s_ind = None
        s_non = None
        if DB_IND.exists():
            s_ind = sqlite3.connect(DB_IND)
            s_ind.row_factory = sqlite3.Row
        if DB_NON.exists():
            s_non = sqlite3.connect(DB_NON)
            s_non.row_factory = sqlite3.Row

        # copy order: clients_main -> individual_details -> kyc_individual -> non_individual -> kyc_non_individual

        # clients_main
        copy_rows(
            s_main,
            mysql_conn,
            "SELECT client_id, category, client_type, risk_level, status, approved_by, approved_at, created_at FROM clients_main ORDER BY client_id ASC",
            "INSERT INTO clients_main (client_id, category, client_type, risk_level, status, approved_by, approved_at, created_at) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
            ["client_id", "category", "client_type", "risk_level", "status", "approved_by", "approved_at", "created_at"],
        )

        if s_ind:
            copy_rows(
                s_ind,
                mysql_conn,
                "SELECT individual_id, client_id, first_name, last_name, gender, date_of_birth, age, phone, email, address FROM individual_details ORDER BY individual_id ASC",
                "INSERT INTO individual_details (individual_id, client_id, first_name, last_name, gender, date_of_birth, age, phone, email, address) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ["individual_id", "client_id", "first_name", "last_name", "gender", "date_of_birth", "age", "phone", "email", "address"],
            )

            copy_rows(
                s_ind,
                mysql_conn,
                "SELECT kyc_individual_id, client_id, id_type, id_number, occupation, source_of_funds FROM kyc_individual ORDER BY kyc_individual_id ASC",
                "INSERT INTO kyc_individual (kyc_individual_id, client_id, id_type, id_number, occupation, source_of_funds) VALUES (%s, %s, %s, %s, %s, %s)",
                ["kyc_individual_id", "client_id", "id_type", "id_number", "occupation", "source_of_funds"],
            )

        if s_non:
            copy_rows(
                s_non,
                mysql_conn,
                "SELECT non_individual_id, client_id, organization_name, registration_number, contact_person, industry, phone, email, address FROM non_individual_details ORDER BY non_individual_id ASC",
                "INSERT INTO non_individual_details (non_individual_id, client_id, organization_name, registration_number, contact_person, industry, phone, email, address) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)",
                ["non_individual_id", "client_id", "organization_name", "registration_number", "contact_person", "industry", "phone", "email", "address"],
            )

            copy_rows(
                s_non,
                mysql_conn,
                "SELECT kyc_non_individual_id, client_id, tax_id, certificate_of_incorporation, business_license_number, beneficial_owner FROM kyc_non_individual ORDER BY kyc_non_individual_id ASC",
                "INSERT INTO kyc_non_individual (kyc_non_individual_id, client_id, tax_id, certificate_of_incorporation, business_license_number, beneficial_owner) VALUES (%s, %s, %s, %s, %s, %s)",
                ["kyc_non_individual_id", "client_id", "tax_id", "certificate_of_incorporation", "business_license_number", "beneficial_owner"],
            )

        # adjust AUTO_INCREMENTs
        set_auto_increment(mysql_conn, "clients_main", "client_id")
        set_auto_increment(mysql_conn, "individual_details", "individual_id")
        set_auto_increment(mysql_conn, "kyc_individual", "kyc_individual_id")
        set_auto_increment(mysql_conn, "non_individual_details", "non_individual_id")
        set_auto_increment(mysql_conn, "kyc_non_individual", "kyc_non_individual_id")

        print("Migration complete.")

    finally:
        try:
            mysql_conn.close()
        except:
            pass


if __name__ == "__main__":
    main()
