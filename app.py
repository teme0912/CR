from datetime import datetime

from flask import Flask, redirect, render_template, request, session, url_for

from database import (
    get_connection,
    initialize_database,
    sp_delete_client,
    sp_get_admin_clients,
    sp_find_existing_customer_by_email,
    sp_find_existing_customer_by_identity,
    sp_find_existing_customer_by_phone,
    sp_get_admin_summary,
    sp_get_registration_status,
    sp_get_registration_status_by_name_and_phone,
    sp_individual_phone_exists,
    sp_non_individual_registration_exists,
    sp_register_individual,
    sp_register_non_individual,
    sp_update_client_status,
)

app = Flask(__name__)
app.secret_key = "core-banking-demo-secret"
ADMIN_USERNAME = "mica"
ADMIN_PASSWORD = "0912"


# Run once at startup to ensure DB objects exist.
initialize_database()


@app.route("/", methods=["GET"])
def index():
    return redirect(url_for("home"))


@app.route("/welcome", methods=["GET"])
def welcome():
    return render_template("welcome.html")


@app.route("/home", methods=["GET"])
def home():
    return render_template("welcome.html")


@app.route("/category", methods=["GET"])
def choose_category():
    return render_template("choose_category.html")


@app.route("/status-overview", methods=["GET"])
def status_overview():
    with get_connection() as conn:
        summary = sp_get_admin_summary(conn)

    return render_template("status_overview.html", summary=summary)


@app.route("/check-registration", methods=["GET", "POST"])
def check_registration():
    if request.method == "GET":
        return render_template(
            "check_registration.html",
            status_data=None,
            error_message="",
            decision_message="",
            full_name_value="",
            phone_value="",
        )

    full_name = request.form.get("full_name", "").strip()
    phone = request.form.get("phone", "").strip()
    if not full_name or not phone:
        return render_template(
            "check_registration.html",
            status_data=None,
            error_message="Full Name and Phone are required.",
            decision_message="",
            full_name_value=full_name,
            phone_value="",
        )

    with get_connection() as conn:
        status_data = sp_get_registration_status_by_name_and_phone(conn, full_name, phone)

    if not status_data:
        return render_template(
            "check_registration.html",
            status_data=None,
            error_message="No registration found for this Full Name and Phone combination.",
            decision_message="",
            full_name_value=full_name,
            phone_value=phone,
        )

    status = status_data["status"]
    if status == "Approved":
        decision_message = "Your registration is approved. Your account is ready for use."
    elif status == "Pending Approval":
        decision_message = "Your registration is pending admin approval. Please wait for confirmation."
    elif status == "Rejected":
        decision_message = "Your registration was rejected. Please contact support and re-submit your details."
    else:
        decision_message = "Your registration is currently blocked due to policy or compliance review."

    return render_template(
        "check_registration.html",
        status_data=status_data,
        error_message="",
        decision_message=decision_message,
        full_name_value=full_name,
        phone_value=phone,
    )


@app.after_request
def add_no_cache_headers(response):
    """Prevent browser caching for portal pages and responses."""
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    response.headers["Expires"] = "0"
    return response


@app.route("/logout", methods=["POST"])
def logout():
    session.clear()
    # Return empty response so user is out from portal without display page.
    response = app.response_class(response="", status=204)
    response.delete_cookie(app.config.get("SESSION_COOKIE_NAME", "session"))
    return response


def _is_admin_authenticated() -> bool:
    return bool(session.get("admin_authenticated"))


def _redirect_admin_login_if_needed():
    if not _is_admin_authenticated():
        return redirect(url_for("admin_login"))
    return None


def _validate_individual_payload(payload: dict) -> list[str]:
    errors: list[str] = []

    required_labels = {
        "first_name": "First Name is required.",
        "last_name": "Last Name is required.",
        "gender": "Gender is required.",
        "date_of_birth": "Date of Birth is required.",
        "phone": "Phone is required.",
        "id_type": "ID Type is required.",
        "id_number": "ID Number is required.",
    }

    for key, message in required_labels.items():
        if not payload.get(key):
            errors.append(message)

    if payload.get("consent") != "yes":
        errors.append("You must accept KYC declaration to register.")

    dob = payload.get("date_of_birth", "")
    if dob:
        try:
            dob_date = datetime.strptime(dob, "%Y-%m-%d").date()
            if dob_date > datetime.today().date():
                errors.append("Date of Birth cannot be in the future.")
        except ValueError:
            errors.append("Date of Birth format is invalid.")

    return errors


def _validate_non_individual_payload(payload: dict) -> list[str]:
    errors: list[str] = []

    required_labels = {
        "organization_name": "Organization Name is required.",
        "registration_number": "Registration Number is required.",
        "contact_person": "Contact Person is required.",
        "phone": "Phone is required.",
        "tax_id": "Tax ID is required.",
    }

    for key, message in required_labels.items():
        if not payload.get(key):
            errors.append(message)

    if payload.get("consent") != "yes":
        errors.append("You must accept KYC declaration to register.")

    return errors


def _duplicate_message(label: str, existing: dict) -> str:
    """Build a simple customer-facing duplicate warning with existing record info."""
    return (
        f"{label} already exists. Existing customer: "
        f"Account {existing['account_number']}, "
        f"Name/Organization {existing['customer_name']}, "
        f"Status {existing['status']}."
    )


@app.route("/register/individual", methods=["GET", "POST"])
def register_individual():
    if request.method == "GET":
        return render_template("individual_register.html", errors=[], form_data={})

    payload = {
        "client_type": request.form.get("client_type", "Individual Client"),
        "first_name": request.form.get("first_name", "").strip(),
        "last_name": request.form.get("last_name", "").strip(),
        "gender": request.form.get("gender", "").strip(),
        "date_of_birth": request.form.get("date_of_birth", "").strip(),
        "phone": request.form.get("phone", "").strip(),
        "email": request.form.get("email", "").strip(),
        "address": request.form.get("address", "").strip(),
        "id_type": request.form.get("id_type", "").strip(),
        "id_number": request.form.get("id_number", "").strip(),
        "occupation": request.form.get("occupation", "").strip(),
        "source_of_funds": request.form.get("source_of_funds", "").strip(),
        "risk_level": request.form.get("risk_level", "Medium").strip(),
        "consent": request.form.get("consent", ""),
    }

    errors = _validate_individual_payload(payload)

    with get_connection() as conn:
        if payload.get("phone"):
            existing_phone = sp_find_existing_customer_by_phone(conn, payload["phone"])
            if existing_phone:
                errors.append(_duplicate_message("Phone", existing_phone))

        if payload.get("email"):
            existing_email = sp_find_existing_customer_by_email(conn, payload["email"])
            if existing_email:
                errors.append(_duplicate_message("Email", existing_email))

        if payload.get("id_number"):
            existing_id = sp_find_existing_customer_by_identity(conn, payload["id_number"])
            if existing_id:
                errors.append(_duplicate_message("ID Number", existing_id))

        if errors:
            return render_template("individual_register.html", errors=errors, form_data=payload)

        client_id = sp_register_individual(conn, payload)
        conn.commit()
        status_data = sp_get_registration_status(conn, client_id)

    return render_template(
        "registration_result.html",
        status_data=status_data,
        success=True,
        reason_list=[],
        status_message="Registration saved with Pending Approval status.",
    )


@app.route("/register/non-individual", methods=["GET", "POST"])
def register_non_individual():
    if request.method == "GET":
        return render_template("non_individual_register.html", errors=[], form_data={})

    payload = {
        "client_type": request.form.get("client_type", "Corporate"),
        "organization_name": request.form.get("organization_name", "").strip(),
        "registration_number": request.form.get("registration_number", "").strip(),
        "contact_person": request.form.get("contact_person", "").strip(),
        "industry": request.form.get("industry", "").strip(),
        "phone": request.form.get("phone", "").strip(),
        "email": request.form.get("email", "").strip(),
        "address": request.form.get("address", "").strip(),
        "tax_id": request.form.get("tax_id", "").strip(),
        "certificate_of_incorporation": request.form.get("certificate_of_incorporation", "").strip(),
        "business_license_number": request.form.get("business_license_number", "").strip(),
        "beneficial_owner": request.form.get("beneficial_owner", "").strip(),
        "risk_level": request.form.get("risk_level", "Medium").strip(),
        "consent": request.form.get("consent", ""),
    }

    errors = _validate_non_individual_payload(payload)

    with get_connection() as conn:
        if payload.get("phone"):
            existing_phone = sp_find_existing_customer_by_phone(conn, payload["phone"])
            if existing_phone:
                errors.append(_duplicate_message("Phone", existing_phone))

        if payload.get("email"):
            existing_email = sp_find_existing_customer_by_email(conn, payload["email"])
            if existing_email:
                errors.append(_duplicate_message("Email", existing_email))

        if payload.get("tax_id"):
            existing_tax_id = sp_find_existing_customer_by_identity(conn, payload["tax_id"])
            if existing_tax_id:
                errors.append(_duplicate_message("Tax ID", existing_tax_id))

        if payload.get("registration_number") and sp_non_individual_registration_exists(
            conn,
            payload["registration_number"],
        ):
            errors.append("Registration Number already exists.")

        if errors:
            return render_template("non_individual_register.html", errors=errors, form_data=payload)

        client_id = sp_register_non_individual(conn, payload)
        conn.commit()
        status_data = sp_get_registration_status(conn, client_id)

    return render_template(
        "registration_result.html",
        status_data=status_data,
        success=True,
        reason_list=[],
        status_message="Registration saved with Pending Approval status.",
    )


@app.route("/approve/<int:client_id>", methods=["POST"])
def approve_client(client_id: int):
    auth_redirect = _redirect_admin_login_if_needed()
    if auth_redirect:
        return auth_redirect

    checker_name = session.get("admin_username", "Admin")
    with get_connection() as conn:
        updated = sp_update_client_status(conn, client_id, "Approved", checker_name)
        conn.commit()
        status_data = sp_get_registration_status(conn, client_id)

    if not updated or not status_data:
        return render_template(
            "registration_result.html",
            status_data=None,
            success=False,
            reason_list=["Client not found for approval."],
            status_message="Approval failed.",
        )

    return render_template(
        "registration_result.html",
        status_data=status_data,
        success=True,
        reason_list=[],
        status_message="Client approved successfully.",
    )


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    if request.method == "GET":
        # Always show login first before backend/admin dashboard.
        session.pop("admin_authenticated", None)
        session.pop("admin_username", None)
        return render_template("admin_login.html", error_message="")

    username = request.form.get("username", "").strip()
    password = request.form.get("password", "").strip()

    if username == ADMIN_USERNAME and password == ADMIN_PASSWORD:
        session["admin_authenticated"] = True
        session["admin_username"] = username
        return redirect(url_for("admin_dashboard"))

    return render_template(
        "admin_login.html",
        error_message="Invalid username or password.",
    )


@app.route("/admin/logout", methods=["GET"])
def admin_logout():
    session.pop("admin_authenticated", None)
    session.pop("admin_username", None)
    return redirect(url_for("admin_login"))


@app.route("/admin", methods=["GET"])
def admin_dashboard():
    auth_redirect = _redirect_admin_login_if_needed()
    if auth_redirect:
        return auth_redirect

    status_filter = request.args.get("status", "").strip()
    search_query = request.args.get("q", "").strip()

    with get_connection() as conn:
        clients = sp_get_admin_clients(conn, status_filter=status_filter, search_query=search_query)
        summary = sp_get_admin_summary(conn)

    return render_template(
        "admin_dashboard.html",
        clients=clients,
        summary=summary,
        status_filter=status_filter,
        search_query=search_query,
        admin_username=session.get("admin_username", "admin"),
    )


@app.route("/admin/client/<int:client_id>/status", methods=["POST"])
def admin_update_client_status(client_id: int):
    auth_redirect = _redirect_admin_login_if_needed()
    if auth_redirect:
        return auth_redirect

    new_status = request.form.get("new_status", "").strip()
    allowed_statuses = {"Pending Approval", "Approved", "Rejected", "Blocked"}
    if new_status not in allowed_statuses:
        return redirect(url_for("admin_dashboard"))

    checker_name = session.get("admin_username", "Admin")
    with get_connection() as conn:
        if new_status == "Approved":
            sp_update_client_status(conn, client_id, new_status, checker_name)
        else:
            sp_update_client_status(conn, client_id, new_status)
        conn.commit()

    return redirect(url_for("admin_dashboard"))


@app.route("/admin/client/<int:client_id>/delete", methods=["POST"])
def admin_delete_client(client_id: int):
    auth_redirect = _redirect_admin_login_if_needed()
    if auth_redirect:
        return auth_redirect

    with get_connection() as conn:
        sp_delete_client(conn, client_id)
        conn.commit()

    return redirect(url_for("admin_dashboard"))


if __name__ == "__main__":
    app.run(debug=True)
