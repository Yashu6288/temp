import datetime
import streamlit as st
from authenticate_ldap import authenticate_user  # <-- your function
from app2 import dashboard

# =================== APP CONFIG (ONCE) ===================
st.set_page_config(page_title="Supply Chain Application Portal", page_icon="💼", layout="wide")

# =================== SESSION KEYS ===================
def _init_session():
    ss = st.session_state
    ss.setdefault("logged_in", False)
    ss.setdefault("username", "")
    ss.setdefault("login_time", None)
    ss.setdefault("login_attempts", 0)
    ss.setdefault("lock_until", None)

_init_session()

# =================== SECURITY SETTINGS ===================
SESSION_TIMEOUT_MINUTES = 30
MAX_LOGIN_ATTEMPTS = 5
LOCKOUT_MINUTES = 10

def _now():
    return datetime.datetime.now()

def is_locked():
    lock_until = st.session_state.lock_until
    return lock_until is not None and _now() < lock_until

def remaining_lock_seconds():
    if st.session_state.lock_until:
        return max(0, int((st.session_state.lock_until - _now()).total_seconds()))
    return 0

def is_session_expired():
    lt = st.session_state.login_time
    if not lt:
        return False
    elapsed_min = (_now() - lt).total_seconds() / 60
    return elapsed_min > SESSION_TIMEOUT_MINUTES

def logout(show_msg=False):
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.login_time = None
    if show_msg:
        st.toast("Logged out", icon="✅")
    st.rerun()

# =================== GLOBAL CSS (Light Bootstrap-like) ===================
st.markdown("""
<style>
  #MainMenu, header, .stDeployButton {visibility: hidden;}
  .stApp { background: linear-gradient(135deg, #f6f9fc 0%, #eef2f7 100%); font-family: 'Segoe UI', Roboto, system-ui, -apple-system, sans-serif; }
  /* Containers */
  .card { background: #fff; border: 1px solid #e9ecef; border-radius: 14px; box-shadow: 0 2px 8px rgba(0,0,0,0.04); }
  .login-wrap { max-width: 420px; margin: 7vh auto; padding: 22px 22px 10px; }
  .login-title { font-size: 22px; font-weight: 700; color: #153e75; margin-bottom: 6px; text-align: center; }
  .login-sub { font-size: 13px; color: #6c757d; margin-bottom: 18px; text-align: center; }
  .footer-note { text-align:center; font-size:12px; color:#98a2a8; padding: 12px 8px 6px; }

  /* Inputs & Buttons */
  .stTextInput input { border-radius: 10px !important; }
  .primary-btn button { width: 100%; border-radius: 10px; font-weight: 600; padding: 10px 14px; background:#1f4e79; color:#fff; border:0; }
  .primary-btn button:hover { background:#173d5e; }

  /* Header */
  .hdr { display:flex; align-items:center; justify-content:space-between; gap:12px; padding:14px 18px; }
  .hdr-title { font-size: 20px; font-weight: 700; color:#153e75; }
  .hdr-right { text-align:right; color:#5c6770; font-size:13px; }

  /* Search + Grid */
  .grid { 
    display:grid; 
    grid-template-columns: repeat(auto-fit, minmax(260px, 300px)); 
    justify-content: center; 
    gap: 18px; 
    padding: 6px; 
  }
  .app-card { 
    display:flex; flex-direction:column; align-items:center; text-align:center;
    padding: 18px 14px; border-radius:14px; border:1px solid #e9ecef;
    transition:transform .15s ease, box-shadow .15s ease, border-color .15s ease; 
  }
  .app-card:hover { transform: translateY(-3px); border-color:#1f4e79; box-shadow: 0 6px 14px rgba(0,0,0,0.07); }
  .app-icon { font-size: 30px; line-height: 1; margin-bottom: 8px; }
  .app-name { font-size: 16px; font-weight: 700; color: #153e75; margin-bottom: 4px; }
  .app-desc { font-size: 13px; color: #6b7280; min-height: 34px; }

  /* Keep links clean inside cards */
  .app-card a { text-decoration:none; }

  /* Make Streamlit buttons inside narrow columns look tidy */
  .logout-col .stButton>button { width: 100%; border-radius: 10px; background:#d9534f; color:#fff; font-weight:600; border:0; }
  .logout-col .stButton>button:hover { background:#bf3e3a; }
</style>
""", unsafe_allow_html=True)

# =================== LOGIN VIEW ===================
def show_login():
    with st.container():
        st.markdown('<div class="card login-wrap">', unsafe_allow_html=True)
        st.markdown('<div class="login-title">Supply Chain Application Portal</div>', unsafe_allow_html=True)
        st.markdown('<div class="login-sub">Secure access to your tools and dashboards</div>', unsafe_allow_html=True)

        if is_locked():
            secs = remaining_lock_seconds()
            mins = secs // 60
            st.error(f"Too many failed attempts. Please try again in {mins}m {secs%60}s.")
        else:
            # Use a form to avoid mid-typing reruns and to support Enter-to-submit
            with st.form("login_form", clear_on_submit=False):
                username = st.text_input("Employee ID", placeholder="Enter your employee ID", key="inp_user")
                password = st.text_input("Password", placeholder="Enter your password", type="password", key="inp_pass")
                submitted = st.form_submit_button("Login", use_container_width=True)
            if submitted:
                if not username or not password:
                    st.error("Please enter both Employee ID and Password.")
                else:
                    try:
                        with st.spinner("Authenticating..."):
                            result = authenticate_user(username.strip(), password)  # expects 200 on success
                        if result == 200:
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
                                st.session_state.lock_until = _now() + datetime.timedelta(minutes=LOCKOUT_MINUTES)
                                st.error("Account temporarily locked due to multiple failed attempts.")
                            else:
                                st.error(f"Invalid credentials. Attempts left: {remaining}")
                    except Exception as e:
                        st.error("Authentication service unavailable. Please contact support.")
                        # Optionally log `e` to your logs

        st.markdown('<div class="footer-note">© 2025 Supply Chain Systems</div>', unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)

# =================== ROUTER ===================
# Auto-expire session if needed
if st.session_state.logged_in and is_session_expired():
    st.warning("Session expired. Please login again.")
    logout(show_msg=False)

if st.session_state.logged_in:
    # Pass a single, consistent logout callback into dashboard
    def _on_logout():
        logout(show_msg=True)

    dashboard(username=st.session_state.username, on_logout=_on_logout)
else:
    show_login()