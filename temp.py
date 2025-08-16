import streamlit as st
import datetime
from authenticate_ldap import authenticate_user
from app2 import dashboard

# ------------------- PAGE CONFIG -------------------
st.set_page_config(page_title="Supply Chain Application Portal", page_icon="💼", layout="centered")

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

# ------------------- CSS FOR STYLING -------------------
st.markdown("""
    <style>
        #MainMenu {visibility: hidden;}
        header {visibility: hidden;}
        .block-container {padding-top: 30px;}
        .stDeployButton {visibility: hidden;}
        .stApp {
            background: linear-gradient(to right, #eef2f5, #dbe9f4);
        }
        .login-container {
            max-width: 420px;
            margin: auto;
            padding-top: 40px;
        }
        .animated-header {
            text-align: center;
            font-size: 30px;
            font-weight: bold;
            color: #0077b6;
            animation: fadeIn 1.2s ease-in-out;
            margin-bottom: 10px;
        }
        .subheading {
            text-align: center;
            font-size: 16px;
            color: #555;
            margin-bottom: 30px;
        }
        .stTextInput > div > div > input {
            border-radius: 8px;
            border: 1px solid #ccc;
            padding: 0.5rem;
        }
        .stButton > button {
            width: 100%;
            border-radius: 8px;
            background-color: #0077b6;
            color: white;
            font-weight: 600;
            padding: 0.6rem;
            transition: all 0.3s ease;
        }
        .stButton > button:hover {
            background-color: #023e8a;
            transform: scale(1.02);
        }
        @keyframes fadeIn {
            from {opacity: 0; transform: translateY(20px);}
            to {opacity: 1; transform: translateY(0);}
        }
        .footer {
            text-align: center;
            font-size: 12px;
            color: #888;
            margin-top: 40px;
        }
    </style>
""", unsafe_allow_html=True)

# ------------------- LOGIN LOGIC -------------------
def show_login():
    st.markdown('<div class="login-container">', unsafe_allow_html=True)
    st.markdown('<div class="animated-header">Welcome to Supply Chain Application Portal</div>', unsafe_allow_html=True)
    st.markdown('<div class="subheading">Access your supply chain tools and dashboards securely.</div>', unsafe_allow_html=True)

    username = st.text_input("Employee ID", placeholder="Enter your employee ID")
    password = st.text_input("Password", placeholder="Enter your password", type="password")

    st.markdown("""
        <p style='text-align: right; font-size: 13px;'>
            <a href='#' style='color: #0077b6;'>Forgot Password?</a>
        </p>
    """, unsafe_allow_html=True)

    if st.button("Login"):
        with st.spinner("Authenticating..."):
            if username and password:
                result = authenticate_user(username, password)
                if result == 200:
                    st.session_state.logged_in = True
                    st.session_state.username = username
                    st.session_state.login_time = datetime.datetime.now()
                    st.rerun()
                else:
                    st.error("❌ Incorrect employee ID or password")
            else:
                st.error("❌ Please enter both employee ID and password")

    st.markdown("""
        <hr>
        <div class="footer">
            © 2025 Supply Chain Systems | Powered by Systems Development Team
        </div>
    """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

# ------------------- ROUTING -------------------
if st.session_state.logged_in and not is_session_expired():
    dashboard(st.session_state.username)
else:
    st.session_state.logged_in = False
    st.session_state.username = ""
    st.session_state.login_time = None
    show_login()
----------------------------------------------------------Code 2 :-
import streamlit as st
import datetime

def dashboard(username):
    if not st.session_state.get("logged_in"):
        st.warning("⚠️ Unauthorized access. Please login first.")
        st.stop()

    st.set_page_config(page_title="Company App Portal", page_icon="💼", layout="wide")

    st.markdown("""
        <style>
            #MainMenu, header, .stDeployButton {visibility: hidden;}
            .block-container {padding-top: 10px;}
            .stApp {
                background: linear-gradient(135deg, #f7faff 0%, #eef2f7 100%);
                font-family: 'Segoe UI', sans-serif;
            }
            .header {
                display: flex;
                align-items: center;
                justify-content: space-between;
                background-color: white;
                padding: 1rem 2rem;
                border-radius: 12px;
                box-shadow: 0 3px 10px rgba(0,0,0,0.05);
                margin-bottom: 1.5rem;
            }
            .title {
                font-size: 1.6rem;
                font-weight: 700;
                color: #1f4e79;
            }
            .welcome {
                font-size: 1rem;
                color: #555;
                text-align: right;
            }
            .app-card {
                background-color: white;
                padding: 1.5rem;
                border-radius: 15px;
                box-shadow: 0 4px 8px rgba(0,0,0,0.05);
                text-align: center;
                transition: all 0.2s ease-in-out;
                border: 1px solid #e6e6e6;
                cursor: pointer;
                height: 180px;
            }
            .app-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 8px 16px rgba(0,0,0,0.08);
                border-color: #1f4e79;
            }
            .app-icon {
                font-size: 2.5rem;
                margin-bottom: 0.5rem;
            }
            .app-name {
                font-size: 1.2rem;
                font-weight: 600;
                color: #1f4e79;
            }
            .app-desc {
                font-size: 0.9rem;
                color: #777;
            }
            a {
                text-decoration: none;
            }
        </style>
    """, unsafe_allow_html=True)

    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("""
            <div class="header">
                <div class="title">Supply Chain Application Portal</div>
            </div>
        """, unsafe_allow_html=True)

    with col2:
        current_time = datetime.datetime.now().strftime("%A, %d %B %Y | %H:%M:%S")
        st.markdown(f"""
            <div class="header" style="justify-content:flex-end;">
                <div class="welcome">
                    👋 Welcome, {username}<br>
                    <small>{current_time}</small>
                </div>
            </div>
        """, unsafe_allow_html=True)

    st.markdown("---")

    search_query = st.text_input("🔍 Search Applications", "", key="search", help="Type to filter applications")

    apps = [
        {"name": "File Upload Tool", "desc": "Securely upload and manage your files", "icon": "📂", "link": "/file-upload"},
        {"name": "Data Dashboard", "desc": "Real-time analytics and insights", "icon": "📊", "link": "/data-dashboard"},
        {"name": "Report Generator", "desc": "Generate PDF/Excel reports instantly", "icon": "📑", "link": "/report-generator"},
        {"name": "HR Portal", "desc": "Leave requests, salary slips, and more", "icon": "👥", "link": "/hr-portal"},
        {"name": "Inventory System", "desc": "Track and manage stock efficiently", "icon": "📦", "link": "/inventory"},
        {"name": "Workflow Tracker", "desc": "Monitor ongoing projects and progress", "icon": "📋", "link": "/workflow"},
        {"name": "Meeting Scheduler", "desc": "Plan meetings with ease", "icon": "📅", "link": "/scheduler"},
        {"name": "Workflow Manager", "desc": "Manages workflow", "icon": "📅", "link": "/workflow"}
    ]

    filtered_apps = [app for app in apps if search_query.lower() in app['name'].lower() or search_query.lower() in app['desc'].lower()]

    if filtered_apps:
        for i in range(0, len(filtered_apps), 3):
            row_apps = filtered_apps[i:i+3]
            cols = st.columns(len(row_apps))
            for col, app in zip(cols, row_apps):
                with col:
                    st.markdown(f"""
                        <a href="{app['link']}" target="_self">
                            <div class="app-card">
                                <div class="app-icon">{app['icon']}</div>
                                <div class="app-name">{app['name']}</div>
                                <div class="app-desc">{app['desc']}</div>
                            </div>
                        </a>
                    """, unsafe_allow_html=True)
    else:
        st.warning("No applications found. Try a different search.")
