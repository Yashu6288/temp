import streamlit as st
import datetime

# ------------------- PAGE CONFIG -------------------
st.set_page_config(page_title="Supply Chain Application Portal", page_icon="💼", layout="wide")

# ------------------- SESSION STATE INIT -------------------
if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
if "username" not in st.session_state:
    st.session_state.username = ""
if "login_time" not in st.session_state:
    st.session_state.login_time = None

# ------------------- SESSION TIMEOUT -------------------
SESSION_TIMEOUT_MINUTES = 30
def is_session_expired():
    if st.session_state.login_time:
        elapsed = (datetime.datetime.now() - st.session_state.login_time).total_seconds() / 60
        return elapsed > SESSION_TIMEOUT_MINUTES
    return False

def logout():
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.login_time = None
    st.rerun()

# ------------------- CSS FOR STYLING -------------------
st.markdown("""
    <style>
        #MainMenu, header, .stDeployButton {visibility: hidden;}
        .stApp { font-family: 'Segoe UI', sans-serif; background: #f7faff; }

        /* LOGIN PAGE */
        .login-container { max-width: 400px; margin: 80px auto; padding: 2rem; 
                           background: white; border-radius: 12px; 
                           box-shadow: 0 4px 12px rgba(0,0,0,0.08); }
        .login-title { text-align: center; font-size: 26px; font-weight: 700; 
                       color: #1f4e79; margin-bottom: 10px; }
        .login-sub { text-align: center; font-size: 14px; color: #666; margin-bottom: 25px; }
        .stTextInput > div > div > input { border-radius: 8px; padding: 10px; }
        .stButton > button { width: 100%; background-color: #1f4e79; color: white; 
                             font-weight: 600; border-radius: 8px; padding: 10px; }
        .stButton > button:hover { background-color: #163b5d; transform: scale(1.01); }
        .footer { text-align: center; font-size: 12px; color: #999; margin-top: 30px; }

        /* DASHBOARD */
        .header { display: flex; justify-content: space-between; align-items: center;
                  background: white; padding: 1rem 2rem; border-radius: 12px; 
                  box-shadow: 0 2px 6px rgba(0,0,0,0.05); margin-bottom: 20px; }
        .title { font-size: 20px; font-weight: 700; color: #1f4e79; }
        .welcome { font-size: 14px; color: #555; text-align: right; }
        .logout-btn { background: #d9534f; color: white; font-weight: 600; 
                      border: none; border-radius: 6px; padding: 6px 14px; cursor: pointer; }
        .logout-btn:hover { background: #c9302c; }

        .grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr)); 
                gap: 20px; margin-top: 20px; }
        .app-card { background: white; padding: 1.5rem; border-radius: 12px;
                    box-shadow: 0 3px 8px rgba(0,0,0,0.05); text-align: center;
                    transition: all 0.2s ease; border: 1px solid #eee; }
        .app-card:hover { transform: translateY(-4px); border-color: #1f4e79;
                          box-shadow: 0 6px 14px rgba(0,0,0,0.08); }
        .app-icon { font-size: 2rem; margin-bottom: 0.5rem; }
        .app-name { font-size: 16px; font-weight: 600; color: #1f4e79; }
        .app-desc { font-size: 13px; color: #777; }
        a { text-decoration: none; }
    </style>
""", unsafe_allow_html=True)


# ------------------- LOGIN PAGE -------------------
def show_login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="login-title">Supply Chain Application Portal</div>', unsafe_allow_html=True)
    st.markdown('<div class="login-sub">Secure access to your tools and dashboards</div>', unsafe_allow_html=True)

    username = st.text_input("Employee ID", placeholder="Enter your employee ID")
    password = st.text_input("Password", placeholder="Enter your password", type="password")

    if st.button("Login"):
        if not username or not password:
            st.error("❌ Please enter both employee ID and password")
        else:
            # 👇 replace with your real LDAP function
            if username == "admin" and password == "admin123":
                st.session_state.logged_in = True
                st.session_state.username = username
                st.session_state.login_time = datetime.datetime.now()
                st.rerun()
            else:
                st.error("❌ Invalid credentials")

    st.markdown('<div class="footer">© 2025 Supply Chain Systems | Powered by Dev Team</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)


# ------------------- DASHBOARD -------------------
def show_dashboard():
    # HEADER
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown('<div class="header"><div class="title">Supply Chain Application Portal</div></div>', unsafe_allow_html=True)
    with col2:
        current_time = datetime.datetime.now().strftime("%A, %d %B %Y | %H:%M:%S")
        st.markdown(f"""
            <div class="header" style="justify-content: flex-end;">
                <div class="welcome">
                    👋 Welcome, {st.session_state.username}<br>
                    <small>{current_time}</small>
                </div>
                <br>
                <button class="logout-btn" onclick="window.location.reload()">Logout</button>
            </div>
        """, unsafe_allow_html=True)

        # Streamlit fallback logout (JS above reloads but this ensures session clear)
        if st.button("Logout", key="logout_fallback"):
            logout()

    # SEARCH
    st.markdown("---")
    search_query = st.text_input("🔍 Search Applications", "", key="search")

    # APP LIST
    apps = [
        {"name": "File Upload Tool", "desc": "Securely upload and manage your files", "icon": "📂", "link": "#"},
        {"name": "Data Dashboard", "desc": "Real-time analytics and insights", "icon": "📊", "link": "#"},
        {"name": "Report Generator", "desc": "Generate PDF/Excel reports instantly", "icon": "📑", "link": "#"},
        {"name": "HR Portal", "desc": "Leave requests, salary slips, and more", "icon": "👥", "link": "#"},
        {"name": "Inventory System", "desc": "Track and manage stock efficiently", "icon": "📦", "link": "#"},
        {"name": "Workflow Tracker", "desc": "Monitor ongoing projects", "icon": "📋", "link": "#"},
        {"name": "Meeting Scheduler", "desc": "Plan meetings with ease", "icon": "📅", "link": "#"},
        {"name": "Workflow Manager", "desc": "Manage workflows effectively", "icon": "🗂", "link": "#"},
    ]

    filtered_apps = [
        app for app in apps
        if search_query.lower() in app['name'].lower() or search_query.lower() in app['desc'].lower()
    ] if search_query else apps

    if filtered_apps:
        st.markdown('<div class="grid">', unsafe_allow_html=True)
        for app in filtered_apps:
            st.markdown(f"""
                <a href="{app['link']}" target="_self">
                    <div class="app-card">
                        <div class="app-icon">{app['icon']}</div>
                        <div class="app-name">{app['name']}</div>
                        <div class="app-desc">{app['desc']}</div>
                    </div>
                </a>
            """, unsafe_allow_html=True)
        st.markdown('</div>', unsafe_allow_html=True)
    else:
        st.warning("No applications found. Try a different search.")


# ------------------- ROUTING -------------------
if st.session_state.logged_in and not is_session_expired():
    show_dashboard()
else:
    if is_session_expired() and st.session_state.logged_in:
        st.warning("⏳ Session expired. Please login again.")
        logout()
    else:
        show_login()