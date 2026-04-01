import sqlite3
from datetime import date, datetime
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
MAIN_DB = BASE_DIR / "CR_main.db"
IND_DB = BASE_DIR / "CR_individual.db"
NONIND_DB = BASE_DIR / "CR_non_individual.db"


def get_connection() -> sqlite3.Connection:
    """Return a connection to the main database (clients_main)."""
    conn = sqlite3.connect(MAIN_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_individual_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(IND_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_non_individual_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(NONIND_DB)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def initialize_database() -> None:
    """Create three databases and their tables if they don't exist."""
    main_schema = """
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS clients_main (
        client_id INTEGER PRIMARY KEY AUTOINCREMENT,
        category TEXT NOT NULL CHECK (category IN ('Individual', 'Non-Individual')),
        client_type TEXT NOT NULL,
        risk_level TEXT NOT NULL DEFAULT 'Medium',
        status TEXT NOT NULL DEFAULT 'Pending Approval',
        approved_by TEXT,
        approved_at TEXT,
        created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    );
    """

    ind_schema = """
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS individual_details (
        individual_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL UNIQUE,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT NOT NULL,
        date_of_birth TEXT NOT NULL,
        age INTEGER NOT NULL,
        phone TEXT NOT NULL,
        email TEXT,
        address TEXT
    );

    CREATE TABLE IF NOT EXISTS kyc_individual (
        kyc_individual_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL UNIQUE,
        id_type TEXT NOT NULL,
        id_number TEXT NOT NULL,
        occupation TEXT,
        source_of_funds TEXT
    );
    """

    nonind_schema = """
    PRAGMA foreign_keys = ON;
    CREATE TABLE IF NOT EXISTS non_individual_details (
        non_individual_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL UNIQUE,
        organization_name TEXT NOT NULL,
        registration_number TEXT NOT NULL,
        contact_person TEXT NOT NULL,
        industry TEXT,
        phone TEXT NOT NULL,
        email TEXT,
        address TEXT
    );

    CREATE TABLE IF NOT EXISTS kyc_non_individual (
        kyc_non_individual_id INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id INTEGER NOT NULL UNIQUE,
        tax_id TEXT NOT NULL,
        certificate_of_incorporation TEXT,
        business_license_number TEXT,
        beneficial_owner TEXT
    );
    """

    with get_connection() as conn:
        conn.executescript(main_schema)

    with get_individual_connection() as conn:
        conn.executescript(ind_schema)

    with get_non_individual_connection() as conn:
        conn.executescript(nonind_schema)


def calculate_age(date_of_birth: str) -> int:
    born = datetime.strptime(date_of_birth, "%Y-%m-%d").date()
    today = date.today()
    years = today.year - born.year
    if (today.month, today.day) < (born.month, born.day):
        years -= 1
    return max(years, 0)


def format_account_number(client_id: int, category: str) -> str:
    """Return a 12-digit display account number using client_id as suffix."""
    if category == "Individual":
        prefix = "1000"
    elif category == "Non-Individual":
        prefix = "10000"
    else:
        raise ValueError(f"Unsupported category: {category}")

    suffix_len = 12 - len(prefix)
    return f"{prefix}{str(client_id).zfill(suffix_len)}"


# -----------------------------------------------------------------------------
# Procedure-style functions (API used by app.py). These operate across DBs
# when necessary. Because SQLite doesn't support distributed transactions
# across separate database files, inserts into individual/non-individual DBs
# are committed independently.
# -----------------------------------------------------------------------------


def sp_create_client_main(
    conn: sqlite3.Connection,
    category: str,
    client_type: str,
    risk_level: str,
) -> int:
    """Insert main client record and return client_id."""
    cursor = conn.execute(
        """
        INSERT INTO clients_main (category, client_type, risk_level)
        VALUES (?, ?, ?)
        """,
        (category, client_type, risk_level),
    )
    conn.commit()
    return cursor.lastrowid


def sp_register_individual(conn: sqlite3.Connection, payload: dict) -> int:
    """Register full Individual customer with KYC in one transaction."""
    client_id = sp_create_client_main(
        conn,
        "Individual",
        payload["client_type"],
        payload.get("risk_level", "Medium"),
    )

    age = calculate_age(payload["date_of_birth"])

    # Insert into individual DB
    with get_individual_connection() as ind_conn:
        ind_conn.execute(
            """
            INSERT INTO individual_details (
                client_id, first_name, last_name, gender, date_of_birth, age, phone, email, address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                payload["first_name"],
                payload["last_name"],
                payload["gender"],
                payload["date_of_birth"],
                age,
                payload["phone"],
                payload.get("email", ""),
                payload.get("address", ""),
            ),
        )

        ind_conn.execute(
            """
            INSERT INTO kyc_individual (
                client_id, id_type, id_number, occupation, source_of_funds
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                client_id,
                payload["id_type"],
                payload["id_number"],
                payload.get("occupation", ""),
                payload.get("source_of_funds", ""),
            ),
        )
        ind_conn.commit()

    return client_id


def sp_register_non_individual(conn: sqlite3.Connection, payload: dict) -> int:
    client_id = sp_create_client_main(
        conn,
        "Non-Individual",
        payload["client_type"],
        payload.get("risk_level", "Medium"),
    )

    with get_non_individual_connection() as nconn:
        nconn.execute(
            """
            INSERT INTO non_individual_details (
                client_id, organization_name, registration_number, contact_person,
                industry, phone, email, address
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                payload["organization_name"],
                payload["registration_number"],
                payload["contact_person"],
                payload.get("industry", ""),
                payload["phone"],
                payload.get("email", ""),
                payload.get("address", ""),
            ),
        )

        nconn.execute(
            """
            INSERT INTO kyc_non_individual (
                client_id, tax_id, certificate_of_incorporation,
                business_license_number, beneficial_owner
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                client_id,
                payload["tax_id"],
                payload.get("certificate_of_incorporation", ""),
                payload.get("business_license_number", ""),
                payload.get("beneficial_owner", ""),
            ),
        )
        nconn.commit()

    return client_id


def sp_individual_phone_exists(conn: sqlite3.Connection, phone: str) -> bool:
    with get_individual_connection() as ind_conn:
        row = ind_conn.execute(
            "SELECT 1 FROM individual_details WHERE phone = ?",
            (phone,),
        ).fetchone()
    return bool(row)


def sp_non_individual_registration_exists(conn: sqlite3.Connection, registration_number: str) -> bool:
    with get_non_individual_connection() as nconn:
        row = nconn.execute(
            "SELECT 1 FROM non_individual_details WHERE registration_number = ?",
            (registration_number,),
        ).fetchone()
    return bool(row)


def sp_find_existing_customer_by_phone(conn: sqlite3.Connection, phone: str) -> dict | None:
    # Check individual DB first
    with get_individual_connection() as ind_conn:
        row = ind_conn.execute(
            "SELECT client_id, first_name, last_name, phone, email FROM individual_details WHERE phone = ? ORDER BY individual_id DESC LIMIT 1",
            (phone,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Individual"),
                "category": "Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": f"{client['first_name']} {client['last_name']}",
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    # Check non-individual DB
    with get_non_individual_connection() as nconn:
        row = nconn.execute(
            "SELECT client_id, organization_name, phone, email FROM non_individual_details WHERE phone = ? ORDER BY non_individual_id DESC LIMIT 1",
            (phone,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Non-Individual"),
                "category": "Non-Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": client.get("organization_name", "-"),
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    return None


def sp_find_existing_customer_by_email(conn: sqlite3.Connection, email: str) -> dict | None:
    normalized = email.strip().lower()
    if not normalized:
        return None

    with get_individual_connection() as ind_conn:
        row = ind_conn.execute(
            "SELECT client_id, first_name, last_name, phone, email FROM individual_details WHERE lower(coalesce(email,'')) = ? ORDER BY individual_id DESC LIMIT 1",
            (normalized,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Individual"),
                "category": "Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": f"{client['first_name']} {client['last_name']}",
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    with get_non_individual_connection() as nconn:
        row = nconn.execute(
            "SELECT client_id, organization_name, phone, email FROM non_individual_details WHERE lower(coalesce(email,'')) = ? ORDER BY non_individual_id DESC LIMIT 1",
            (normalized,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Non-Individual"),
                "category": "Non-Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": client.get("organization_name", "-"),
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    return None


def sp_find_existing_customer_by_identity(conn: sqlite3.Connection, identity_value: str) -> dict | None:
    if not identity_value:
        return None

    with get_individual_connection() as ind_conn:
        row = ind_conn.execute(
            "SELECT d.client_id, d.first_name, d.last_name, d.phone, d.email FROM individual_details d JOIN kyc_individual k ON k.client_id = d.client_id WHERE k.id_number = ? ORDER BY d.individual_id DESC LIMIT 1",
            (identity_value,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Individual"),
                "category": "Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": f"{client['first_name']} {client['last_name']}",
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    with get_non_individual_connection() as nconn:
        row = nconn.execute(
            "SELECT n.client_id, n.organization_name, n.phone, n.email FROM non_individual_details n JOIN kyc_non_individual k ON k.client_id = n.client_id WHERE k.tax_id = ? ORDER BY n.non_individual_id DESC LIMIT 1",
            (identity_value,),
        ).fetchone()
        if row:
            client = dict(row)
            with get_connection() as mconn:
                m = mconn.execute("SELECT status, created_at FROM clients_main WHERE client_id = ?", (client["client_id"],)).fetchone()
            return {
                "client_id": client["client_id"],
                "account_number": format_account_number(client["client_id"], "Non-Individual"),
                "category": "Non-Individual",
                "status": m["status"] if m else None,
                "created_at": m["created_at"] if m else None,
                "customer_name": client.get("organization_name", "-"),
                "phone": client["phone"],
                "email": client.get("email", ""),
            }

    return None


def sp_get_registration_status(conn: sqlite3.Connection, client_id: int) -> dict | None:
    with get_connection() as mconn:
        m = mconn.execute(
            "SELECT client_id, category, client_type, risk_level, status, approved_by, approved_at, created_at FROM clients_main WHERE client_id = ?",
            (client_id,),
        ).fetchone()
        if not m:
            return None
        m = dict(m)

    # Try individual
    with get_individual_connection() as ind_conn:
        d = ind_conn.execute("SELECT first_name, last_name, phone FROM individual_details WHERE client_id = ?", (client_id,)).fetchone()
        if d:
            d = dict(d)
            return {
                "client_id": m["client_id"],
                "account_number": format_account_number(m["client_id"], m["category"]),
                "category": m["category"],
                "client_type": m["client_type"],
                "risk_level": m["risk_level"],
                "status": m["status"],
                "approved_by": m.get("approved_by"),
                "approved_at": m.get("approved_at"),
                "created_at": m.get("created_at"),
                "first_name": d.get("first_name"),
                "last_name": d.get("last_name"),
                "organization_name": None,
                "phone": d.get("phone"),
            }


def sp_get_registration_status_by_phone(conn: sqlite3.Connection, phone: str) -> dict | None:
        # try individual
        with get_individual_connection() as ind_conn:
            row = ind_conn.execute("SELECT client_id FROM individual_details WHERE phone = ? ORDER BY individual_id DESC LIMIT 1", (phone,)).fetchone()
            if row:
                return sp_get_registration_status(conn, row[0])

        with get_non_individual_connection() as nconn:
            row = nconn.execute("SELECT client_id FROM non_individual_details WHERE phone = ? ORDER BY non_individual_id DESC LIMIT 1", (phone,)).fetchone()
            if row:
                return sp_get_registration_status(conn, row[0])

        return None


def sp_get_registration_status_by_name_and_phone(
    conn: sqlite3.Connection,
    full_name: str,
    phone: str,
) -> dict | None:
        normalized_full_name = " ".join(full_name.split()).lower()
        with get_individual_connection() as ind_conn:
            rows = ind_conn.execute(
                "SELECT client_id, lower(trim(first_name || ' ' || last_name)) as full_name FROM individual_details WHERE phone = ? ORDER BY individual_id DESC",
                (phone,),
            ).fetchall()
            for r in rows:
                if r["full_name"] == normalized_full_name:
                    return sp_get_registration_status(conn, r["client_id"])
        return None


def sp_update_client_status(
    conn: sqlite3.Connection,
    client_id: int,
    new_status: str,
    approved_by: str | None = None,
) -> bool:
        with get_connection() as mconn:
            if new_status == "Approved":
                cursor = mconn.execute(
                    """
                    UPDATE clients_main
                    SET status = ?, approved_by = ?, approved_at = CURRENT_TIMESTAMP
                    WHERE client_id = ?
                    """,
                    (new_status, approved_by or "Checker", client_id),
                )
            else:
                cursor = mconn.execute(
                    """
                    UPDATE clients_main
                    SET status = ?
                    WHERE client_id = ?
                    """,
                    (new_status, client_id),
                )
            mconn.commit()
            return cursor.rowcount > 0


def sp_get_admin_clients(
    conn: sqlite3.Connection,
    status_filter: str = "",
    search_query: str = "",
) -> list[dict]:
        # Fetch main rows then enrich with display name from other DBs.
        sql = "SELECT client_id, category, client_type, risk_level, status, approved_by, created_at FROM clients_main WHERE 1=1"
        params: list = []
        if status_filter:
            sql += " AND status = ?"
            params.append(status_filter)

        sql += " ORDER BY client_id DESC"

        results: list[dict] = []
        with get_connection() as mconn:
            rows = mconn.execute(sql, params).fetchall()
            for r in rows:
                r = dict(r)
                client_id = r["client_id"]
                display_name = "-"
                if r["category"] == "Individual":
                    with get_individual_connection() as ind_conn:
                        d = ind_conn.execute("SELECT first_name, last_name FROM individual_details WHERE client_id = ?", (client_id,)).fetchone()
                        if d:
                            display_name = f"{d['first_name']} {d['last_name']}"
                else:
                    with get_non_individual_connection() as nconn:
                        n = nconn.execute("SELECT organization_name FROM non_individual_details WHERE client_id = ?", (client_id,)).fetchone()
                        if n:
                            display_name = n["organization_name"]

                r["display_name"] = display_name
                r["account_number"] = format_account_number(client_id, r["category"])
                results.append(r)
        # If search_query provided, filter results in Python for name or client_id match
        if search_query:
            q = search_query.lower()
            filtered = []
            for r in results:
                if q in str(r["client_id"]) or q in r["display_name"].lower():
                    filtered.append(r)
            return filtered

        return results


def sp_get_admin_summary(conn: sqlite3.Connection) -> dict[str, int]:
    with get_connection() as mconn:
        total = mconn.execute("SELECT COUNT(*) FROM clients_main").fetchone()[0]
        pending = mconn.execute("SELECT COUNT(*) FROM clients_main WHERE status = 'Pending Approval'").fetchone()[0]
        approved = mconn.execute("SELECT COUNT(*) FROM clients_main WHERE status = 'Approved'").fetchone()[0]
        rejected = mconn.execute("SELECT COUNT(*) FROM clients_main WHERE status = 'Rejected'").fetchone()[0]
        blocked = mconn.execute("SELECT COUNT(*) FROM clients_main WHERE status = 'Blocked'").fetchone()[0]
    return {"total": total, "pending": pending, "approved": approved, "rejected": rejected, "blocked": blocked}


def sp_delete_client(conn: sqlite3.Connection, client_id: int) -> bool:
    # Delete from individual and non-individual DBs then from main
    with get_individual_connection() as ind_conn:
        ind_conn.execute("DELETE FROM kyc_individual WHERE client_id = ?", (client_id,))
        ind_conn.execute("DELETE FROM individual_details WHERE client_id = ?", (client_id,))
        ind_conn.commit()

    with get_non_individual_connection() as nconn:
        nconn.execute("DELETE FROM kyc_non_individual WHERE client_id = ?", (client_id,))
        nconn.execute("DELETE FROM non_individual_details WHERE client_id = ?", (client_id,))
        nconn.commit()

    with get_connection() as mconn:
        cursor = mconn.execute("DELETE FROM clients_main WHERE client_id = ?", (client_id,))
        mconn.commit()
        return cursor.rowcount > 0