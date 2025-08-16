import datetime
import streamlit as st

def dashboard(username: str, on_logout):
    # ---------------- Header ----------------
    hdr = st.container()
    with hdr:
        col_left, col_mid, col_right = st.columns([6, 3, 1], vertical_alignment="center")
        with col_left:
            st.markdown('<div class="card hdr"><div class="hdr-title">Supply Chain Application Portal</div></div>', unsafe_allow_html=True)
        with col_mid:
            now = datetime.datetime.now().strftime("%A, %d %B %Y | %H:%M:%S")
            st.markdown(f'<div class="card hdr"><div class="hdr-right">👋 Welcome, <b>{username}</b><br><span>{now}</span></div></div>', unsafe_allow_html=True)
        with col_right:
            st.markdown('<div class="card hdr">', unsafe_allow_html=True)
            # Single, consistent logout button
            st.button("Logout", key="logout", on_click=on_logout, use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)

    # ---------------- Search ----------------
    st.markdown("---")
    q = st.text_input("🔍 Search Applications", "", placeholder="Type to filter applications (name or description)")

    # ---------------- App Catalog ----------------
    # For Streamlit multipage apps, prefer "page" with st.switch_page(…).
    # For external/internal routes, use "url".
    apps = [
        {"name":"File Upload Tool","desc":"Securely upload and manage your files","icon":"📂","type":"page","target":"pages/file_upload.py"},
        {"name":"Data Dashboard","desc":"Real-time analytics and insights","icon":"📊","type":"page","target":"pages/data_dashboard.py"},
        {"name":"Report Generator","desc":"Generate PDF/Excel reports instantly","icon":"📑","type":"page","target":"pages/report_generator.py"},
        {"name":"HR Portal","desc":"Leave requests, salary slips, and more","icon":"👥","type":"url","target":"/hr-portal"},
        {"name":"Inventory System","desc":"Track and manage stock efficiently","icon":"📦","type":"page","target":"pages/inventory.py"},
        {"name":"Workflow Tracker","desc":"Monitor ongoing projects and progress","icon":"📋","type":"page","target":"pages/workflow_tracker.py"},
        {"name":"Meeting Scheduler","desc":"Plan meetings with ease","icon":"📅","type":"url","target":"/scheduler"},
        {"name":"Workflow Manager","desc":"Manage workflows effectively","icon":"🗂","type":"page","target":"pages/workflow_manager.py"},
    ]

    # Filter (case-insensitive) — preserves layout even if only one result
    qnorm = (q or "").strip().lower()
    filtered = [a for a in apps if (qnorm in a["name"].lower() or qnorm in a["desc"].lower())] if qnorm else apps

    # ---------------- Grid Render ----------------
    # HTML grid keeps columns tidy; single result stays centered, not stretched.
    st.markdown('<div class="grid">', unsafe_allow_html=True)
    for app in filtered:
        st.markdown('<div class="card app-card">', unsafe_allow_html=True)
        st.markdown(f'<div class="app-icon">{app["icon"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="app-name">{app["name"]}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="app-desc">{app["desc"]}</div>', unsafe_allow_html=True)

        # Action row — use appropriate navigation method
        if app["type"] == "page":
            # Use a small button to keep styling consistent; switch_page is reliable for multipage apps
            if st.button("Open", key=f"open_{app['target']}", use_container_width=True):
                try:
                    st.switch_page(app["target"])
                except Exception:
                    st.error("Target page not found. Ensure it exists under /pages.")
        else:  # URL
            # Link button opens relative path in same tab (works behind reverse proxy)
            st.link_button("Open", url=app["target"], use_container_width=True)

        st.markdown('</div>', unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)

    if not filtered:
        st.info("No applications match your search. Try a different term.")