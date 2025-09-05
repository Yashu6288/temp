#!/usr/bin/env python3
"""
access_portal_app.py

Single-file Flask application for managing:
 - ACCESS_PORTAL_GROUP_MASTER (USER_GROUP PK, HOME_DIR, FOLDER_NAMES)
 - ACCESS_PORTAL_USER_MASTER  (USER_ID PK, USER_GROUP FK -> group)

Features:
 - Login with admin/admin (session-based)
 - Safe CRUD with FK validation and transactional writes
 - Server-side pagination and search/filtering for both Groups and Users
 - Bootstrap 5 UI
 - Config via environment variables for Oracle connection
 - Run: python access_portal_app.py
"""

import os
from functools import wraps
from math import ceil

from flask import (
    Flask, request, redirect, url_for, render_template_string,
    session, flash
)
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# ========================= CONFIG =========================
APP_SECRET_KEY = os.getenv("APP_SECRET_KEY", "change-me-in-prod")
PER_PAGE_DEFAULT = int(os.getenv("PER_PAGE_DEFAULT", "10"))
PER_PAGE_OPTIONS = [5, 10, 20, 50]

# Oracle connection env vars. Required for Oracle mode:
ORACLE_USERNAME = os.getenv("ORACLE_USERNAME", "")
ORACLE_PASSWORD = os.getenv("ORACLE_PASSWORD", "")
ORACLE_HOST     = os.getenv("ORACLE_HOST", "")
ORACLE_PORT     = os.getenv("ORACLE_PORT", "1521")
ORACLE_SERVICE  = os.getenv("ORACLE_SERVICE_NAME", "")

# Tables / Columns (uppercase to match typical Oracle naming)
TABLE_GROUP = os.getenv("TABLE_GROUP", "ACCESS_PORTAL_GROUP_MASTER")
TABLE_USER  = os.getenv("TABLE_USER", "ACCESS_PORTAL_USER_MASTER")

COL_GROUP_KEY = os.getenv("COL_GROUP_KEY", "USER_GROUP")     # PK of group table
COL_HOME_DIR  = os.getenv("COL_HOME_DIR", "HOME_DIR")
COL_FOLDER    = os.getenv("COL_FOLDER", "FOLDER_NAMES")

COL_USER_ID   = os.getenv("COL_USER_ID", "USER_ID")        # PK of user table
COL_USER_GRP  = os.getenv("COL_USER_GRP", "USER_GROUP")    # FK -> group.user_group

# Hard-coded demo login (replace with LDAP later)
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")

# Flask app
app = Flask(__name__)
app.secret_key = APP_SECRET_KEY

# ==================== DB ENGINE (SQLAlchemy Core) ====================
def get_engine():
    # Require all Oracle variables to use Oracle
    if ORACLE_USERNAME and ORACLE_PASSWORD and ORACLE_HOST and ORACLE_SERVICE:
        # Using oracledb thin driver style
        uri = (
            f"oracle+oracledb://{ORACLE_USERNAME}:{ORACLE_PASSWORD}"
            f"@{ORACLE_HOST}:{ORACLE_PORT}/?service_name={ORACLE_SERVICE}"
        )
        return create_engine(uri, pool_pre_ping=True, pool_recycle=1800)
    else:
        raise RuntimeError(
            "Oracle connection variables not set. Please set ORACLE_USERNAME, ORACLE_PASSWORD, "
            "ORACLE_HOST, ORACLE_PORT (optional), and ORACLE_SERVICE_NAME."
        )

engine = get_engine()

# ==================== AUTH HELPERS ====================
def login_required(fn):
    @wraps(fn)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        return fn(*args, **kwargs)
    return wrapper

# ==================== DATA HELPERS (with pagination + search) ====================
def _search_where_clause(col_list, search_term):
    """Return SQL snippet and params for searching across multiple columns (case-insensitive)."""
    if not search_term:
        return "", {}
    # Oracle UPPER() used for case-insensitive search
    search_term = f"%{search_term.strip().upper()}%"
    clauses = []
    params = {}
    for i, col in enumerate(col_list):
        pname = f"s{i}"
        clauses.append(f"UPPER({col}) LIKE :{pname}")
        params[pname] = search_term
    return " AND (" + " OR ".join(clauses) + ")", params

def fetch_groups(conn, page:int=1, per_page:int=10, search:str=None):
    """Return (rows, total_count)."""
    base_cols = f"{COL_GROUP_KEY}, {COL_HOME_DIR}, {COL_FOLDER}"
    where_snippet, params = _search_where_clause([COL_GROUP_KEY, COL_HOME_DIR, COL_FOLDER], search)
    count_q = text(f"SELECT COUNT(1) AS CNT FROM {TABLE_GROUP} WHERE 1=1 {where_snippet}")
    total = conn.execute(count_q, params).scalar() or 0

    offset = max(page-1, 0) * per_page
    q = text(f"""
        SELECT {base_cols}
        FROM {TABLE_GROUP}
        WHERE 1=1 {where_snippet}
        ORDER BY {COL_GROUP_KEY}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """)
    params.update({"offset": offset, "limit": per_page})
    rows = conn.execute(q, params).mappings().all()
    return [dict(r) for r in rows], int(total)

def fetch_users(conn, page:int=1, per_page:int=10, search:str=None):
    base_cols = f"{COL_USER_ID}, {COL_USER_GRP}"
    where_snippet, params = _search_where_clause([COL_USER_ID, COL_USER_GRP], search)
    count_q = text(f"SELECT COUNT(1) AS CNT FROM {TABLE_USER} WHERE 1=1 {where_snippet}")
    total = conn.execute(count_q, params).scalar() or 0

    offset = max(page-1, 0) * per_page
    q = text(f"""
        SELECT {base_cols}
        FROM {TABLE_USER}
        WHERE 1=1 {where_snippet}
        ORDER BY {COL_USER_ID}
        OFFSET :offset ROWS FETCH NEXT :limit ROWS ONLY
    """)
    params.update({"offset": offset, "limit": per_page})
    rows = conn.execute(q, params).mappings().all()
    return [dict(r) for r in rows], int(total)

def group_exists(conn, group_key: str) -> bool:
    q = text(f"SELECT 1 FROM {TABLE_GROUP} WHERE {COL_GROUP_KEY} = :k FETCH FIRST 1 ROWS ONLY")
    return conn.execute(q, {"k": group_key}).first() is not None

def user_exists(conn, user_id: str) -> bool:
    q = text(f"SELECT 1 FROM {TABLE_USER} WHERE {COL_USER_ID} = :u FETCH FIRST 1 ROWS ONLY")
    return conn.execute(q, {"u": user_id}).first() is not None

def count_users_in_groups(conn, group_keys):
    if not group_keys:
        return {}
    bind_params = {f"g{i}": g for i, g in enumerate(group_keys)}
    in_clause = "(" + ", ".join([f":g{i}" for i in range(len(group_keys))]) + ")"
    q = text(f"""
        SELECT {COL_USER_GRP} AS G, COUNT(*) AS CNT
        FROM {TABLE_USER}
        WHERE {COL_USER_GRP} IN {in_clause}
        GROUP BY {COL_USER_GRP}
    """)
    res = conn.execute(q, bind_params).all()
    return {row[0]: int(row[1]) for row in res}

# ==================== ROUTES ====================
@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        u = request.form.get("username", "").strip()
        p = request.form.get("password", "")
        if u == ADMIN_USER and p == ADMIN_PASS:
            session["logged_in"] = True
            session["username"] = u
            flash("Welcome, admin!", "success")
            return redirect(url_for("manage"))
        flash("Invalid credentials.", "danger")
    return render_template_string(TPL_LOGIN, ADMIN_USER=ADMIN_USER, ADMIN_PASS=ADMIN_PASS)

@app.route("/logout")
def logout():
    session.clear()
    flash("Logged out.", "info")
    return redirect(url_for("login"))

@app.route("/")
@login_required
def index():
    return redirect(url_for("manage"))

@app.route("/manage", methods=["GET", "POST"])
@login_required
def manage():
    # Write actions via POST and redirect back to GET to avoid form-resubmission issues
    if request.method == "POST":
        action = request.form.get("action")
        try:
            if action == "add_group":
                return add_group()
            elif action == "update_group":
                return update_group()
            elif action == "delete_groups":
                return delete_groups()
            elif action == "add_user":
                return add_user()
            elif action == "update_user":
                return update_user()
            elif action == "delete_users":
                return delete_users()
            else:
                flash("Unknown action.", "warning")
        except SQLAlchemyError as ex:
            flash(f"Database error: {ex}", "danger")
        except Exception as ex:
            flash(f"Error: {ex}", "danger")
        return redirect(url_for("manage"))

    # GET -> read query params for pagination/search
    try:
        group_page = max(int(request.args.get("group_page", "1")), 1)
    except ValueError:
        group_page = 1
    try:
        user_page = max(int(request.args.get("user_page", "1")), 1)
    except ValueError:
        user_page = 1
    try:
        per_page = int(request.args.get("per_page", PER_PAGE_DEFAULT))
        if per_page not in PER_PAGE_OPTIONS:
            per_page = PER_PAGE_DEFAULT
    except ValueError:
        per_page = PER_PAGE_DEFAULT

    group_search = request.args.get("group_search", "").strip() or None
    user_search  = request.args.get("user_search", "").strip() or None

    with engine.begin() as conn:
        groups, groups_total = fetch_groups(conn, page=group_page, per_page=per_page, search=group_search)
        users, users_total   = fetch_users(conn, page=user_page, per_page=per_page, search=user_search)

    # Helper pagination metadata
    group_pages = max(1, ceil(groups_total / per_page))
    user_pages  = max(1, ceil(users_total / per_page))

    return render_template_string(
        TPL_MANAGE,
        groups=groups,
        users=users,
        groups_total=groups_total,
        users_total=users_total,
        group_page=group_page,
        user_page=user_page,
        group_pages=group_pages,
        user_pages=user_pages,
        per_page=per_page,
        per_page_options=PER_PAGE_OPTIONS,
        group_search=group_search or "",
        user_search=user_search or "",
        COL_GROUP_KEY=COL_GROUP_KEY,
        COL_HOME_DIR=COL_HOME_DIR,
        COL_FOLDER=COL_FOLDER,
        COL_USER_ID=COL_USER_ID,
        COL_USER_GRP=COL_USER_GRP
    )

# ==================== ACTION HANDLERS (WRITE) ====================
def add_group():
    k   = (request.form.get("group_key") or "").strip()
    hd  = (request.form.get("home_dir") or "").strip()
    fld = (request.form.get("folder_names") or "").strip()

    if not k:
        flash("Group key is required.", "danger")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        if group_exists(conn, k):
            flash(f"Group '{k}' already exists.", "warning")
            return redirect(url_for("manage"))
        q = text(f"""
            INSERT INTO {TABLE_GROUP} ({COL_GROUP_KEY}, {COL_HOME_DIR}, {COL_FOLDER})
            VALUES (:k, :h, :f)
        """)
        conn.execute(q, {"k": k, "h": hd, "f": fld})
        flash(f"Group '{k}' created.", "success")
    return redirect(url_for("manage"))

def update_group():
    k   = (request.form.get("group_key") or "").strip()
    hd  = (request.form.get("home_dir") or "").strip()
    fld = (request.form.get("folder_names") or "").strip()
    if not k:
        flash("Group key is required.", "danger")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        if not group_exists(conn, k):
            flash(f"Group '{k}' does not exist.", "danger")
            return redirect(url_for("manage"))
        q = text(f"""
            UPDATE {TABLE_GROUP}
            SET {COL_HOME_DIR} = :h, {COL_FOLDER} = :f
            WHERE {COL_GROUP_KEY} = :k
        """)
        conn.execute(q, {"h": hd, "f": fld, "k": k})
        flash(f"Group '{k}' updated.", "success")
    return redirect(url_for("manage"))

def delete_groups():
    keys = request.form.getlist("group_delete")
    keys = [k.strip() for k in keys if k.strip()]
    if not keys:
        flash("No groups selected.", "info")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        usage = count_users_in_groups(conn, keys)
        blocked = [k for k in keys if usage.get(k, 0) > 0]
        to_delete = [k for k in keys if k not in blocked]

        for k in to_delete:
            conn.execute(text(f"DELETE FROM {TABLE_GROUP} WHERE {COL_GROUP_KEY} = :k"), {"k": k})

        msg = []
        if to_delete:
            msg.append(f"Deleted {len(to_delete)} group(s).")
        if blocked:
            msg.append("Blocked: " + ", ".join([f"{k} (in use by {usage[k]} user(s))" for k in blocked]))
        flash(" ".join(msg), "warning" if blocked else "success")

    return redirect(url_for("manage"))

def add_user():
    uid = (request.form.get("user_id") or "").strip()
    grp = (request.form.get("user_group") or "").strip()
    if not uid:
        flash("User ID is required.", "danger")
        return redirect(url_for("manage"))
    if not grp:
        flash("User group is required.", "danger")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        if user_exists(conn, uid):
            flash(f"User '{uid}' already exists.", "warning")
            return redirect(url_for("manage"))
        if not group_exists(conn, grp):
            flash(f"Cannot create user. Group '{grp}' does not exist.", "danger")
            return redirect(url_for("manage"))

        q = text(f"""
            INSERT INTO {TABLE_USER} ({COL_USER_ID}, {COL_USER_GRP})
            VALUES (:u, :g)
        """)
        conn.execute(q, {"u": uid, "g": grp})
        flash(f"User '{uid}' created.", "success")
    return redirect(url_for("manage"))

def update_user():
    uid = (request.form.get("user_id") or "").strip()
    grp = (request.form.get("user_group") or "").strip()
    if not uid:
        flash("User ID is required.", "danger")
        return redirect(url_for("manage"))
    if not grp:
        flash("User group is required.", "danger")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        if not user_exists(conn, uid):
            flash(f"User '{uid}' does not exist.", "danger")
            return redirect(url_for("manage"))
        if not group_exists(conn, grp):
            flash(f"Cannot set user group. Group '{grp}' does not exist.", "danger")
            return redirect(url_for("manage"))

        q = text(f"""
            UPDATE {TABLE_USER}
            SET {COL_USER_GRP} = :g
            WHERE {COL_USER_ID} = :u
        """)
        conn.execute(q, {"g": grp, "u": uid})
        flash(f"User '{uid}' updated.", "success")
    return redirect(url_for("manage"))

def delete_users():
    uids = request.form.getlist("user_delete")
    uids = [u.strip() for u in uids if u.strip()]
    if not uids:
        flash("No users selected.", "info")
        return redirect(url_for("manage"))

    with engine.begin() as conn:
        for u in uids:
            conn.execute(text(f"DELETE FROM {TABLE_USER} WHERE {COL_USER_ID} = :u"), {"u": u})
        flash(f"Deleted {len(uids)} user(s).", "warning")
    return redirect(url_for("manage"))

# ==================== TEMPLATES (Bootstrap) ====================
TPL_LOGIN = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Login — Access Portal Admin</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
</head>
<body class="bg-light">
  <div class="container py-5">
    <div class="row justify-content-center">
      <div class="col-md-4">
        <div class="card shadow-sm">
          <div class="card-body">
            <h4 class="mb-3">🗄️ Admin – Access Portal</h4>
            {% with messages = get_flashed_messages(with_categories=true) %}
              {% if messages %}
                {% for cat,msg in messages %}
                  <div class="alert alert-{{cat}} py-2">{{msg}}</div>
                {% endfor %}
              {% endif %}
            {% endwith %}
            <form method="post" autocomplete="off">
              <div class="mb-3">
                <label class="form-label">Username</label>
                <input name="username" class="form-control" required>
              </div>
              <div class="mb-3">
                <label class="form-label">Password</label>
                <input type="password" name="password" class="form-control" required>
              </div>
              <button class="btn btn-primary w-100">Login</button>
            </form>
            <div class="text-muted small mt-3">Use <b>{{ADMIN_USER}}</b> / <b>{{ADMIN_PASS}}</b> for now.</div>
          </div>
        </div>
      </div>
    </div>
  </div>
</body>
</html>
"""

TPL_MANAGE = """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Access Portal Admin — CRUD</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/css/bootstrap.min.css" rel="stylesheet">
  <style>
    .table td, .table th { vertical-align: middle; }
    .sticky-actions { position: sticky; top: 0; background: #fff; z-index: 1; }
    .nowrap { white-space: nowrap; }
    .small-input { max-width: 220px; display: inline-block; }
  </style>
</head>
<body>
<nav class="navbar navbar-expand-md bg-body-tertiary mb-3">
  <div class="container-fluid">
    <span class="navbar-brand">🗄️ Access Portal Admin</span>
    <div class="d-flex">
      <span class="navbar-text me-3">Logged in as {{ session.get('username') }}</span>
      <a href="{{ url_for('logout') }}" class="btn btn-outline-secondary btn-sm">Logout</a>
    </div>
  </div>
</nav>

<div class="container">
  {% with messages = get_flashed_messages(with_categories=true) %}
    {% if messages %}
      {% for cat,msg in messages %}
        <div class="alert alert-{{cat}} py-2">{{msg}}</div>
      {% endfor %}
    {% endif %}
  {% endwith %}

  <ul class="nav nav-tabs" id="crudTabs" role="tablist">
    <li class="nav-item" role="presentation">
      <button class="nav-link active" id="groups-tab" data-bs-toggle="tab" data-bs-target="#groups" type="button" role="tab">
        Group Master ({{ groups_total }})
      </button>
    </li>
    <li class="nav-item" role="presentation">
      <button class="nav-link" id="users-tab" data-bs-toggle="tab" data-bs-target="#users" type="button" role="tab">
        User Master ({{ users_total }})
      </button>
    </li>
  </ul>

  <div class="tab-content py-3">
    <!-- GROUPS -->
    <div class="tab-pane fade show active" id="groups" role="tabpanel">
      <div class="row g-3">
        <div class="col-md-4">
          <div class="card shadow-sm">
            <div class="card-body">
              <h5 class="card-title">Add Group</h5>
              <form method="post">
                <input type="hidden" name="action" value="add_group">
                <div class="mb-2">
                  <label class="form-label">User Group (PK)</label>
                  <input name="group_key" class="form-control" maxlength="128" required>
                </div>
                <div class="mb-2">
                  <label class="form-label">Home Dir</label>
                  <input name="home_dir" class="form-control" maxlength="512">
                </div>
                <div class="mb-3">
                  <label class="form-label">Folder Names</label>
                  <input name="folder_names" class="form-control" maxlength="512">
                </div>
                <button class="btn btn-primary">Create</button>
              </form>
            </div>
          </div>

          !-- Search & per-page -->
          <div class="card mt-3 shadow-sm">
            <div class="card-body">
              <form method="get" class="row g-2">
                <div class="col-12">
                  <label class="form-label small">Search Groups</label>
                  <input name="group_search" value="{{ group_search }}" class="form-control" placeholder="Search group, home or folder">
                </div>
                <div class="col-6">
                  <label class="form-label small">Per page</label>
                  <select name="per_page" class="form-select">
                    {% for opt in per_page_options %}
                      <option value="{{opt}}" {% if per_page==opt %}selected{% endif %}>{{opt}}</option>
                    {% endfor %}
                  </select>
                </div>
                <div class="col-6 align-self-end text-end">
                  <button class="btn btn-outline-primary">Apply</button>
                </div>
              </form>
            </div>
          </div>

        </div>

        <div class="col-md-8">
          <div class="card shadow-sm">
            <div class="card-body">
              <div class="d-flex justify-content-between align-items-center mb-2">
                <h5 class="card-title mb-0">Groups</h5>
                <form method="post" class="m-0">
                  <input type="hidden" name="action" value="delete_groups">
                  <button class="btn btn-outline-danger btn-sm"
                          onclick="return confirm('Delete selected group(s)? Blocked if in use by users.');">
                    Delete Selected
                  </button>
              </div>
              <div class="table-responsive">
                <table class="table table-sm table-striped align-middle">
                  <thead class="table-light">
                    <tr>
                      <th style="width:40px;"></th>
                      <th>User Group</th>
                      <th>Home Dir</th>
                      <th>Folder Names</th>
                      <th class="text-end">Save</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for g in groups %}
                    <tr>
                      <td>
                        <input class="form-check-input" type="checkbox" name="group_delete" value="{{ g[COL_GROUP_KEY] }}">
                      </td>
                      <form method="post">
                        <input type="hidden" name="action" value="update_group">
                        <td class="nowrap">
                          <input class="form-control form-control-sm" value="{{ g[COL_GROUP_KEY] }}" readonly name="group_key">
                        </td>
                        <td><input class="form-control form-control-sm" name="home_dir" value="{{ g[COL_HOME_DIR] or '' }}"></td>
                        <td><input class="form-control form-control-sm" name="folder_names" value="{{ g[COL_FOLDER] or '' }}"></td>
                        <td class="text-end">
                          <button class="btn btn-sm btn-outline-primary">Save</button>
                        </td>
                      </form>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
              </form> <!-- delete form wraps table checkboxes -->
!-- pagination -->
              <nav aria-label="Group pagination">
                <ul class="pagination justify-content-end mb-0 mt-2">
                  <li class="page-item {% if group_page<=1 %}disabled{% endif %}">
                    <a class="page-link" href="?group_page={{group_page-1}}&per_page={{per_page}}&group_search={{group_search}}">Previous</a>
                  </li>
                  <li class="page-item disabled"><span class="page-link">Page {{group_page}} of {{group_pages}}</span></li>
                  <li class="page-item {% if group_page>=group_pages %}disabled{% endif %}">
                    <a class="page-link" href="?group_page={{group_page+1}}&per_page={{per_page}}&group_search={{group_search}}">Next</a>
                  </li>
                </ul>
              </nav>

            </div>
          </div>
        </div>
      </div>
    </div>

    <!-- USERS -->
    <div class="tab-pane fade" id="users" role="tabpanel">
      <div class="row g-3">
        <div class="col-md-4">
          <div class="card shadow-sm">
            <div class="card-body">
              <h5 class="card-title">Add User</h5>
              <form method="post">
                <input type="hidden" name="action" value="add_user">
                <div class="mb-2">
                  <label class="form-label">User ID (PK)</label>
                  <input name="user_id" class="form-control" maxlength="128" required>
                </div>
                <div class="mb-3">
                  <label class="form-label">User Group (FK)</label>
                  <input name="user_group" class="form-control" list="groupOptions" required>
                  <datalist id="groupOptions">
                    {% for g in groups %}
                      <option value="{{ g[COL_GROUP_KEY] }}"></option>
                    {% endfor %}
                  </datalist>
                </div>
                <button class="btn btn-primary">Create</button>
              </form>
            </div>
          </div>
 <!-- Search & per-page -->
          <div class="card mt-3 shadow-sm">
            <div class="card-body">
              <form method="get" class="row g-2">
                <div class="col-12">
                  <label class="form-label small">Search Users</label>
                  <input name="user_search" value="{{ user_search }}" class="form-control" placeholder="Search user or group">
                </div>
                <div class="col-6">
                  <label class="form-label small">Per page</label>
                  <select name="per_page" class="form-select">
                    {% for opt in per_page_options %}
                      <option value="{{opt}}" {% if per_page==opt %}selected{% endif %}>{{opt}}</option>
                    {% endfor %}
                  </select>
                </div>
                <div class="col-6 align-self-end text-end">
                  <button class="btn btn-outline-primary">Apply</button>
                </div>
              </form>
            </div>
          </div>

        </div>
<div class="col-md-8">
          <div class="card shadow-sm">
            <div class="card-body">
              <div class="d-flex justify-content-between align-items-center mb-2">
                <h5 class="card-title mb-0">Users</h5>
                <form method="post" class="m-0">
                  <input type="hidden" name="action" value="delete_users">
                  <button class="btn btn-outline-danger btn-sm"
                          onclick="return confirm('Delete selected user(s)?');">
                    Delete Selected
                  </button>
              </div>
              <div class="table-responsive">
                <table class="table table-sm table-striped align-middle">
                  <thead class="table-light">
                    <tr>
                      <th style="width:40px;"></th>
                      <th>User ID</th>
                      <th>User Group</th>
                      <th class="text-end">Save</th>
                    </tr>
                  </thead>
                  <tbody>
                    {% for u in users %}
                    <tr>
                      <td>
                        <input class="form-check-input" type="checkbox" name="user_delete"
value="{{ u[COL_USER_ID] }}">
                      </td>
                      <form method="post">
                        <input type="hidden" name="action" value="update_user">
                        <td class="nowrap">
                          <input class="form-control form-control-sm" value="{{ u[COL_USER_ID] }}" readonly name="user_id">
                        </td>
                        <td>
                          <input name="user_group" class="form-control form-control-sm" list="groupOptions"
                                 value="{{ u[COL_USER_GRP] or '' }}">
                        </td>
                        <td class="text-end">
                          <button class="btn btn-sm btn-outline-primary">Save</button>
                        </td>
                      </form>
                    </tr>
                    {% endfor %}
                  </tbody>
                </table>
              </div>
              </form> <!-- delete form wraps table checkboxes -->
!-- pagination -->
              <nav aria-label="User pagination">
                <ul class="pagination justify-content-end mb-0 mt-2">
                  <li class="page-item {% if user_page<=1 %}disabled{% endif %}">
                    <a class="page-link" href="?user_page={{user_page-1}}&per_page={{per_page}}&user_search={{user_search}}">Previous</a>
                  </li>
                  <li class="page-item disabled"><span class="page-link">Page {{user_page}} of {{user_pages}}</span></li>
                  <li class="page-item {% if user_page>=user_pages %}disabled{% endif %}">
                    <a class="page-link" href="?user_page={{user_page+1}}&per_page={{per_page}}&user_search={{user_search}}">Next</a>
                  </li>
                </ul>
              </nav>

            </div>
          </div>
        </div>
      </div>
    </div>

  </div> <!-- tab-content -->
</div> <!-- container -->

<script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.3/dist/js/bootstrap.bundle.min.js"></script>
</body>
</html>
"""

# ==================== RUN ====================
if __name__ == "__main__":
    # Bind host 0.0.0.0 for container friendliness; use debug=False in prod
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "5000")), debug=False)