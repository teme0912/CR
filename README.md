# Core Banking Mini Project (Customer Registration)

This beginner-friendly mini project demonstrates **module-by-module customer registration** in a core banking style system.

## What This Project Covers

1. Customer categories
- Individual
- Non-Individual

2. Customer types
- Individual types: Individual Client, Minors, Groups, Staffs
- Non-Individual types: Corporate, Association, Bank, NGOs

3. Different KYC sections
- Individual KYC table
- Non-Individual KYC table

4. Data architecture requested
- Shared main table for all customers
- Shared comments table for all customers
- Separate details table for Individual customers
- Separate details table for Non-Individual customers

5. Procedure-style data access
- All insert/read operations are called through procedure-style Python functions in [database.py](database.py)

## Project Structure

- [app.py](app.py): Flask app + routes
- [database.py](database.py): DB connection + procedure-style functions
- [schema.sql](schema.sql): Tables, PK/FK, views
- [templates/choose_category.html](templates/choose_category.html): First page for category choice
- [templates/individual_register.html](templates/individual_register.html): Individual form page
- [templates/non_individual_register.html](templates/non_individual_register.html): Non-Individual form page
- [templates/registration_result.html](templates/registration_result.html): Status and approval page
- [static/css/style.css](static/css/style.css): Styling and background
- [requirements.txt](requirements.txt): Python dependency

## Beginner Setup Steps (VS Code)

1. Open terminal in this project folder.
2. Create virtual environment:

```powershell
python -m venv .venv
```

3. Activate virtual environment:

```powershell
.\.venv\Scripts\Activate.ps1
```

4. Install dependency:

```powershell
pip install -r requirements.txt
```

5. Run the app:

```powershell
python app.py
```

6. Open browser:

- http://127.0.0.1:5000

## How It Works (Simple)

1. App starts and runs `initialize_database()`.
2. Tables are created automatically if they do not exist.
3. Home page shows only category choice:
- Individual
- Non-Individual
 - Logout (top-right)
4. Click one category to open its own registration page.
5. If you selected wrong option, use Back (one page) or Home.
6. Submit registration form.
7. Route calls a procedure-style function:
- `sp_register_individual(...)`
- `sp_register_non_individual(...)`
8. System generates a unique 12-digit account number.
9. Data is saved into:
- `clients_main`
- category-specific table (`individual_details` or `non_individual_details`)
- category-specific KYC table
10. Result page shows registration status and account details.
11. New records start as `Pending Approval`; checker can approve to `Approved`.

## Important Features Added

- Auto-generated 12-digit account number
- Auto-calculated age from date of birth for Individual customers
- Category-first flow with separate registration pages
- Basic required field validation
- Clear error reasons when registration fails
- Duplicate checks for key fields
- KYC declaration consent checkbox (important)
- Risk Level capture (Low/Medium/High)
- Back button (one-page return) on both registration pages
- Home button on both registration pages
- Professional Logout option on home page
- Maker-checker style status flow (`Pending Approval` -> `Approved`)
- Responsive dashboard (works on desktop/mobile)
- Flash success/error notifications

## Notes About Procedures in SQLite

SQLite does not support server-side stored procedures like MSSQL/MySQL packages.
For this demo, procedure behavior is implemented using **procedure-style Python functions** in [database.py](database.py), and all database writes/reads pass through these functions.

## Suggested Next Improvements

1. Add login and role-based access for bank officers.
2. Add search and filtering by account number/type.
3. Add audit trail table for every update/delete event.
4. Add document upload for KYC files.
5. Add API endpoints for integration with other banking modules.
