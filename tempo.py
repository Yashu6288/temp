# app.py
import os
import datetime as dt
import time
import streamlit as st

# =============== PAGE CONFIG ===============
st.set_page_config(
    page_title="Supply Chain Application Portal",
    page_icon="💼",
    layout="wide",
    initial_sidebar_state="collapsed",
)

# =============== SESSION KEYS ===============
def _init_state():
    ss = st.session_state
    ss.setdefault("logged_in", False)
    ss.setdefault("username", "")
    ss.setdefault("login_time", None)
    ss.setdefault("login_attempts", 0)
    ss.setdefault("lock_until", None)

_init_state()

# =============== SECURITY SETTINGS ===============
SESSION_TIMEOUT_MINUTES = 30  # auto logout
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

# =============== AUTH ===============
def _now():
    return dt.datetime.now()

def authenticate(username: str, password: str) -> bool:
    """
    Returns True if credentials are valid.
    Priority: authenticate_ldap.authenticate_user (expects 200 on success)
    Fallback: env creds ADMIN_USER / ADMIN_PASS (for local/dev)
    """
    try:
        # Use your real LDAP module if available
        from authenticate_ldap import authenticate_user
        try:
            status = authenticate_user(username, password)
            return status == 200
        except Exception as e:
            # Avoid leaking server details to UI; log e if you have logging
            st.error("Authentication service error. Please try again or contact support.")
            return False
    except Exception:
        # Fallback for dev/local use only
        dev_user = os.getenv("ADMIN_USER", "admin")
        dev_pass = os.getenv("ADMIN_PASS", "admin123")
        return username == dev_user and password == dev_pass

def is_locked() -> bool:
    lu = st.session_state.lock_until
    return lu is not None and _now() < lu

def lock_remaining() -> int:
    lu = st.session_state.lock_until
    if not lu:
        return 0
    return max(0, int((lu - _now()).total_seconds()))

def is_session_expired() -> bool:
    lt = st.session_state.login_time
    if not lt:
        return False
    elapsed_min = (_now() - lt).total_seconds() / 60
    return elapsed_min > SESSION_TIMEOUT_MINUTES

def do_logout(show_toast: bool = True):
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.login_time = None
    if show_toast:
        st.toast("Logged out", icon="✅")
    st.rerun()

# =============== GLOBAL CSS (keeps your look, adds polish + animations) ===============
st.markdown("""
<style>
  #MainMenu, header, .stDeployButton {visibility: hidden;}
  .stApp {
    background: linear-gradient(135deg, #eef2f5 0%, #dbe9f4 100%);
    font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
  }

  /* Animations */
  @keyframes fadeInUp { from {opacity:0; transform: translateY(12px);} to {opacity:1; transform: translateY(0);} }
  @keyframes softPop { from {transform: scale(0.98);} to {transform: scale(1);} }

  /* LOGIN */
  .login-container {
    max-width: 420px; margin: 7vh auto 4vh; padding: 26px 22px 18px;
    background: #fff; border: 1px solid #e6e8eb; border-radius: 14px;
    box-shadow: 0 8px 22px rgba(0,0,0,0.05);
    animation: fadeInUp .28s ease-out both;
  }
  .animated-header {
    text-align:center; font-size: 26px; font-weight: 800; color:#0b5fa5; margin-bottom: 6px;
  }
  .subheading {
    text-align:center; font-size: 14px; color:#5f6b76; margin-bottom: 18px;
  }
  .stTextInput input { border-radius:10px !important; border:1px solid #cfd6dd; padding:.6rem .7rem; }
  .primary-btn button {
    width:100%; border-radius:10px; background:#0b5fa5; color:#fff; font-weight:700; padding:.65rem;
    border:0; transition: all .15s ease; animation: softPop .2s ease-out both;
  }
  .primary-btn button:hover { background:#084b82; transform: translateY(-1px); }
  .login-footer { text-align:center; font-size:12px; color:#8a97a3; margin-top: 10px; }

  /* DASHBOARD HEADER */
  .header-wrap { display:flex; gap:14px; margin-bottom: 16px; }
  .header-card {
    flex:1; background:#fff; border:1px solid #e6e8eb; border-radius:12px; padding: 14px 18px;
    box-shadow: 0 3px 10px rgba(0,0,0,0.04);
  }
  .title { font-size: 20px; font-weight:800; color:#144f86; }
  .welcome { font-size: 14px; color:#5a6570; text-align:right; }
  .logout-col .stButton>button {
    width:100%; border-radius:10px; background:#d9534f; color:#fff; font-weight:700; border:0; padding:.55rem;
  }
  .logout-col .stButton>button:hover { background:#bf4140; }

  /* SEARCH */
  .divider { border-top:1px solid #e7eaee; margin: 10px 0 12px; }

  /* GRID (stable even for single card) */
  .grid {
    display:grid;
    grid-template-columns: repeat(auto-fit, minmax(260px, 300px));
    justify-content: center;             /* centers row when few items */
    gap: 18px;
    padding: 4px;
  }
  .app-card {
    background:#fff; border:1px solid #e6e8eb; border-radius:14px; padding:18px 14px; text-align:center;
    box-shadow: 0 4px 10px rgba(0,0,0,0.04);
    transition: border-color .15s ease, box-shadow .15s ease, transform .15s ease;
    animation: fadeInUp .25s ease-out both;
  }
  .app-card:hover { border-color:#0b5fa5; box-shadow:0 10px 20px rgba(0,0,0,0.07); transform: translateY(-3px); }
  .app-icon { font-size: 34px; margin-bottom: 6px; line-height:1; }
  .app-name { font-size: 16px; font-weight: 800; color:#144f86; margin-bottom: 4px; }
  .app-desc { font-size: 13px; color:#66727d; min-height: 34px; }

  a { text-decoration: none; color: inherit; }
</style>
""", unsafe_allow_html=True)

# =============== LOGIN VIEW ===============
def view_login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="animated-header">Welcome to Supply Chain Application Portal</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheading">Access your supply chain tools and dashboards securely.</div>', unsafe_allow_html=True)

    if is_locked():
        secs = lock_remaining()
        st.error(f"Too many failed attempts. Try again in {secs//60}m {secs%60}s.")
        st.markdown('<div class="login-footer">© 2025 Supply Chain Systems</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
        return

    # Use a form (stable reruns + Enter submits)
    with st.form("login_form", clear_on_submit=False):
        username = st.text_input("Employee ID", placeholder="Enter your employee ID")
        password = st.text_input("Password", placeholder="Enter your password", type="password")
        submitted = st.form_submit_button("Login", use_container_width=True)
    st.markdown('<div class="primary-btn"></div>', unsafe_allow_html=True)

    if submitted:
        if not username or not password:
            st.error("❌ Please enter both employee ID and password")
        else:
            with st.spinner("Authenticating..."):
                ok = authenticate(username.strip(), password)
            if ok:
                st.session_state.logged_in = True
                st.session_state.username = username.strip()
                st.session_state.login_time = _now()
                st.session_state.login_attempts = 0
                st.session_state.lock_until = None
                st.toast("Welcome!", icon="👋")
                st.rerun()
            else:
                st.session_state.login_attempts += 1
                remaining = MAX_LOGIN_ATTEMPTS - st.session_state.login_attempts
                if remaining <= 0:
                    st.session_state.lock_until = _now() + dt.timedelta(minutes=LOCKOUT_MINUTES)
                    st.error("Account temporarily locked due to multiple failed attempts.")
                else:
                    st.error(f"❌ Incorrect employee ID or password. Attempts left: {remaining}")

    st.markdown("""
        <p style='text-align: right; font-size: 13px; margin: 6px 2px 0 0;'>
            <a href='#' style='color: #0b5fa5;'>Forgot Password?</a>
        </p>
        <hr class="divider">
        <div class="login-footer">© 2025 Supply Chain Systems | Powered by Systems Development Team</div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# =============== DASHBOARD VIEW ===============
def view_dashboard():
    # Header bar with a single, well-aligned logout
    col_a, col_b, col_c = st.columns([6, 4, 1], vertical_alignment="center")
    with col_a:
        st.markdown("""
            <div class="header-card">
                <div class="title">Supply Chain Application Portal</div>
            </div>
        """, unsafe_allow_html=True)
    with col_b:
        now = _now().strftime("%A, %d %B %Y | %H:%M:%S")
        st.markdown(f"""
            <div class="header-card">
                <div class="welcome">👋 Welcome, <b>{st.session_state.username}</b><br><small>{now}</small></div>
            </div>
        """, unsafe_allow_html=True)
    with col_c:
        st.markdown('<div class="header-card logout-col">', unsafe_allow_html=True)
        if st.button("Logout", key="logout_btn", use_container_width=True):
            do_logout()
        st.markdown('</div>', unsafe_allow_html=True)

    # Divider
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # Search
    q = st.text_input("🔍 Search Applications", value="", key="search", help="Type to filter applications")

    # App catalog
    apps = [
        {"name": "File Upload Tool", "desc": "Securely upload and manage your files", "icon": "📂", "url": "/file-upload"},
        {"name": "Data Dashboard", "desc": "Real-time analytics and insights", "icon": "📊", "url": "/data-dashboard"},
        {"name": "Report Generator", "desc": "Generate PDF/Excel reports instantly", "icon": "📑", "url": "/report-generator"},
        {"name": "HR Portal", "desc": "Leave requests, salary slips, and more", "icon": "👥", "url": "/hr-portal"},
        {"name": "Inventory System", "desc": "Track and manage stock efficiently", "icon": "📦", "url": "/inventory"},
        {"name": "Workflow Tracker", "desc": "Monitor ongoing projects and progress", "icon": "📋", "url": "/workflow"},
        {"name": "Meeting Scheduler", "desc": "Plan meetings with ease", "icon": "📅", "url": "/scheduler"},
        {"name": "Workflow Manager", "desc": "Manages workflow", "icon": "🗂", "url": "/workflow-manager"},
    ]

    qn = (q or "").strip().lower()
    filtered = [a for a in apps if (qn in a["name"].lower() or qn in a["desc"].lower())] if qn else apps

    # Grid — stays centered and neat even if only one result
    st.markdown('<div class="grid">', unsafe_allow_html=True)
    for app in filtered:
        st.markdown('<div class="app-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="app-icon">{app["icon"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="app-name">{app["name"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="app-desc">{app["desc"]}</div>', unsafe_allow_html=True)
        # Use Streamlit buttons/links for actions (keeps SPA feeling)
        col_open = st.columns([1])[0]
        with col_open:
            st.link_button("Open", url=app["url"], use_container_width=True)
        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if not filtered:
        st.info("No applications found. Try a different search.")

# =============== ROUTER ===============
# Auto-expire active sessions
if st.session_state.logged_in and is_session_expired():
    st.warning("⏳ Session expired. Please login again.")
    do_logout(show_toast=False)

if st.session_state.logged_in:
    view_dashboard()
else:
    view_login()