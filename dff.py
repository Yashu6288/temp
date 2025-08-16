import streamlit as st

# --- PAGE CONFIG ---
st.set_page_config(page_title="Dashboard", page_icon="📊", layout="wide")

# --- CUSTOM CSS FOR PROFESSIONAL LOOK ---
st.markdown("""
    <style>
        body {
            background-color: #f8f9fa;
        }
        .login-card {
            max-width: 350px;
            margin: auto;
            padding: 2rem;
            border-radius: 15px;
            background: white;
            box-shadow: 0 4px 12px rgba(0,0,0,0.1);
        }
        .stTextInput > div > div > input {
            border-radius: 8px;
        }
        .logout-button {
            float: right;
            margin-top: -50px;
        }
        .dashboard-header {
            font-size: 28px;
            font-weight: 600;
            margin-bottom: 1rem;
        }
        .card {
            padding: 1.5rem;
            border-radius: 12px;
            background: white;
            box-shadow: 0 4px 12px rgba(0,0,0,0.05);
            text-align: center;
        }
        .footer {
            text-align: center;
            margin-top: 2rem;
            font-size: 13px;
            color: grey;
        }
    </style>
""", unsafe_allow_html=True)


# --- SESSION STATE INIT ---
if "authenticated" not in st.session_state:
    st.session_state.authenticated = False


# --- LOGIN FUNCTION ---
def login():
    st.markdown("<div class='login-card'>", unsafe_allow_html=True)
    st.markdown("<h3 style='text-align:center;'>🔐 Login</h3>", unsafe_allow_html=True)

    username = st.text_input("Username")
    password = st.text_input("Password", type="password")

    if st.button("Login", use_container_width=True):
        if username == "admin" and password == "admin123":  # Dummy check
            st.session_state.authenticated = True
            st.rerun()
        else:
            st.error("Invalid username or password")

    st.markdown("</div>", unsafe_allow_html=True)


# --- DASHBOARD FUNCTION ---
def dashboard():
    # HEADER WITH LOGOUT
    st.markdown("<div class='dashboard-header'>📊 Dashboard</div>", unsafe_allow_html=True)
    if st.button("Logout", key="logout", help="Logout", use_container_width=False):
        st.session_state.authenticated = False
        st.rerun()

    # SEARCH BAR
    search_query = st.text_input("🔎 Search Projects", placeholder="Enter project name...")

    # GRID CARDS
    cols = st.columns(3)
    projects = ["Project Alpha", "Project Beta", "Project Gamma", "Project Delta", "Project Epsilon"]

    # Filter based on search
    if search_query:
        projects = [p for p in projects if search_query.lower() in p.lower()]

    if projects:
        for i, project in enumerate(projects):
            with cols[i % 3]:
                st.markdown(f"<div class='card'><b>{project}</b><br>Click to view details</div>", unsafe_allow_html=True)
    else:
        st.info("No projects found.")

    # FOOTER
    st.markdown("<div class='footer'>© 2025 My Company | Built with ❤️ using Streamlit</div>", unsafe_allow_html=True)


# --- MAIN APP ---
if not st.session_state.authenticated:
    login()
else:
    dashboard()