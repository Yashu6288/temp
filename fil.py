import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode
from typing import Tuple, Dict, List

st.set_page_config(page_title="Oracle Admin – Access Portal", page_icon="🗄️", layout="wide")

# ============================ CONFIG / CONSTANTS ============================
ID_COL = "_row_id"
TABLE_GROUP = "ACCESS_PORTAL_GROUP_MASTER"
TABLE_USER  = "ACCESS_PORTAL_USER_MASTER"

COL_GROUP_KEY = "USER_GROUP"   # PK of group table
COL_HOME_DIR  = "HOME_DIR"
COL_FOLDER    = "FOLDER_NAME"

COL_USER_ID   = "USER_ID"      # PK of user table
COL_USER_GRP  = "USER_GROUP"   # FK -> group.user_group

# ============================ DB HELPERS ====================================
def sql_alchemy_from_secrets():
    """
    Reads Streamlit secrets and returns a SQLAlchemy engine for Oracle using 'oracledb'.
    Supports either 'service_name' OR 'sid' in secrets.
    """
    username = st.secrets["Database"]["username"]
    password = st.secrets["Database"]["password"]
    host     = st.secrets["Database"]["host"]
    port     = st.secrets["Database"]["port"]
    # Allow either service_name or sid
    service_name = st.secrets["Database"].get("service_name")
    sid          = st.secrets["Database"].get("sid")

    if service_name:
        dsn = f"{host}:{port}/?service_name={service_name}"
    elif sid:
        dsn = f"{host}:{port}/?sid={sid}"
    else:
        st.stop()  # hard fail with clear message
    engine_path = f"oracle+oracledb://{username}:{password}@{dsn}"
    return create_engine(engine_path, pool_pre_ping=True)

def fetch_data(engine) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df_group = pd.read_sql_query(f'SELECT {COL_GROUP_KEY}, {COL_HOME_DIR}, {COL_FOLDER} FROM {TABLE_GROUP}', engine)
    df_user  = pd.read_sql_query(f'SELECT {COL_USER_ID}, {COL_USER_GRP} FROM {TABLE_USER}', engine)
    return df_group, df_user

# ============================ UTILITIES =====================================
def with_row_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df[ID_COL] = range(len(df))
    return df

def clean_grid_df(df_like) -> pd.DataFrame:
    df = pd.DataFrame(df_like).copy()
    df = df.drop(columns=["::auto_unique_id::", "_selectedRowNodeInfo"], errors="ignore")
    return df

def build_grid_options(df: pd.DataFrame, *, editable=True, selectable=True, select_options: Dict[str, List[str]] = None) -> dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=editable, resizable=True, filter=True)
    gb.configure_column(ID_COL, editable=False, hide=True)
    if selectable:
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout="autoHeight")
    # Configure select dropdowns (e.g., user_group on user table)
    if select_options:
        for col, opts in select_options.items():
            gb.configure_column(
                col,
                editable=True,
                cellEditor="agSelectCellEditor",
                cellEditorParams={"values": opts},
                filter=True
            )
    return gb.build()

def align_dtypes_like(target: pd.DataFrame, reference: pd.DataFrame, exclude_cols=None) -> pd.DataFrame:
    target = target.copy()
    exclude_cols = set(exclude_cols or [])
    for col in reference.columns:
        if col in target.columns and col not in exclude_cols:
            try:
                target[col] = target[col].astype(reference[col].dtype)
            except Exception:
                pass
    return target

def compute_changes(current: pd.DataFrame, original: pd.DataFrame):
    """Return (added, edited, deleted) using ID_COL as key; ignores ID_COL in comparisons."""
    if ID_COL not in current.columns or ID_COL not in original.columns:
        raise ValueError(f"Missing '{ID_COL}'")
    cur = current.copy()
    orig = original.copy()
    cur[ID_COL] = pd.to_numeric(cur[ID_COL], errors="coerce").astype("Int64")
    orig[ID_COL] = pd.to_numeric(orig[ID_COL], errors="coerce").astype("Int64")

    added_ids   = set(cur[ID_COL]) - set(orig[ID_COL])
    deleted_ids = set(orig[ID_COL]) - set(cur[ID_COL])
    common_ids  = list(set(cur[ID_COL]) & set(orig[ID_COL]))

    added   = cur[cur[ID_COL].isin(list(added_ids))].reset_index(drop=True)
    deleted = orig[orig[ID_COL].isin(list(deleted_ids))].reset_index(drop=True)

    if not common_ids:
        edited = pd.DataFrame(columns=cur.columns)
    else:
        cur_idx  = cur.set_index(ID_COL, drop=False)
        orig_idx = orig.set_index(ID_COL, drop=False)
        compare_cols = [c for c in cur.columns if c in orig.columns and c != ID_COL]
        cur_idx[compare_cols] = align_dtypes_like(cur_idx[compare_cols], orig_idx[compare_cols]).copy()
        changed_mask = (cur_idx.loc[common_ids, compare_cols] != orig_idx.loc[common_ids, compare_cols]).any(axis=1)
        changed_ids = changed_mask[changed_mask].index.tolist()
        edited = cur_idx.loc[changed_ids].reset_index(drop=True)

    return added, edited, deleted

def fk_validate_users(df_user: pd.DataFrame, valid_groups: List[str]) -> List[str]:
    """Return list of errors for invalid user_group references."""
    errors = []
    invalid = df_user[~df_user[COL_USER_GRP].isin(valid_groups)]
    for _, row in invalid.iterrows():
        errors.append(f"User '{row[COL_USER_ID]}' refers to unknown group '{row[COL_USER_GRP]}'.")
    return errors

def guard_group_deletes(deleted_groups: pd.DataFrame, engine) -> List[str]:
    """Block deletion of groups that are still used by any user."""
    if deleted_groups.empty:
        return []
    groups = tuple(deleted_groups[COL_GROUP_KEY].astype(str).unique())
    # Build safe IN clause
    if len(groups) == 1:
        in_clause = f"('{groups[0]}')"
    else:
        in_clause = "(" + ",".join([f":g{i}" for i in range(len(groups))]) + ")"

    sql = text(f"""
        SELECT {COL_USER_GRP}, COUNT(*) as CNT
        FROM {TABLE_USER}
        WHERE {COL_USER_GRP} IN {in_clause}
        GROUP BY {COL_USER_GRP}
    """)
    params = {f"g{i}": g for i, g in enumerate(groups)} if len(groups) > 1 else {}
    with engine.begin() as conn:
        res = conn.execute(sql, params).fetchall()
    if not res:
        return []
    in_use = {r[0]: r[1] for r in res}
    return [f"Group '{g}' is in use by {cnt} user(s) and cannot be deleted." for g, cnt in in_use.items()]
# ============================ DB APPLY (CRUD) ================================
def apply_group_changes(engine, added: pd.DataFrame, edited: pd.DataFrame, deleted: pd.DataFrame):
    """Apply CRUD to group table in a single transaction."""
    with engine.begin() as conn:
        # Deletes (safe only if not referenced – already guarded before calling)
        for _, row in deleted.iterrows():
            conn.execute(
                text(f"DELETE FROM {TABLE_GROUP} WHERE {COL_GROUP_KEY} = :k"),
                {"k": row[COL_GROUP_KEY]}
            )
        # Updates
        for _, row in edited.iterrows():
            conn.execute(
                text(f"""
                    UPDATE {TABLE_GROUP}
                    SET {COL_HOME_DIR} = :home_dir,
                        {COL_FOLDER}  = :folder
                    WHERE {COL_GROUP_KEY} = :k
                """),
                {
                    "home_dir": row[COL_HOME_DIR],
                    "folder": row[COL_FOLDER],
                    "k": row[COL_GROUP_KEY],
                }
            )
        # Inserts
        for _, row in added.iterrows():
            conn.execute(
                text(f"""
                    INSERT INTO {TABLE_GROUP} ({COL_GROUP_KEY}, {COL_HOME_DIR}, {COL_FOLDER})
                    VALUES (:k, :home_dir, :folder)
                """),
                {
                    "k": row[COL_GROUP_KEY],
                    "home_dir": row[COL_HOME_DIR],
                    "folder": row[COL_FOLDER],
                }
            )

def apply_user_changes(engine, added: pd.DataFrame, edited: pd.DataFrame, deleted: pd.DataFrame):
    """Apply CRUD to user table in a single transaction."""
    with engine.begin() as conn:
        # Deletes
        for _, row in deleted.iterrows():
            conn.execute(
                text(f"DELETE FROM {TABLE_USER} WHERE {COL_USER_ID} = :uid"),
                {"uid": row[COL_USER_ID]}
            )
        # Updates
        for _, row in edited.iterrows():
            conn.execute(
                text(f"""
                    UPDATE {TABLE_USER}
                    SET {COL_USER_GRP} = :grp
                    WHERE {COL_USER_ID} = :uid
                """),
                {"grp": row[COL_USER_GRP], "uid": row[COL_USER_ID]}
            )
        # Inserts
        for _, row in added.iterrows():
            conn.execute(
                text(f"""
                    INSERT INTO {TABLE_USER} ({COL_USER_ID}, {COL_USER_GRP})
                    VALUES (:uid, :grp)
                """),
                {"uid": row[COL_USER_ID], "grp": row[COL_USER_GRP]}
            )

# ============================ UI HELPERS ====================================
def header():
    st.markdown(
        """
        <style>
        .big-title { font-size: 28px; font-weight: 700; margin-bottom: 0.2rem; }
        .subtle { color: #666; margin-top: 0; }
        .toolbar .stButton>button { border-radius: 8px; padding: 0.4rem 0.8rem; }
        .apply-area .stButton>button { font-weight: 700; }
        .ag-theme-streamlit { --ag-row-height: 36px; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="big-title">🗄️ Access Portal Admin</div>', unsafe_allow_html=True)
    st.markdown('<p class="subtle">Manage groups and users with validation & safe, auditable updates.</p>', unsafe_allow_html=True)

def add_form(df_ref: pd.DataFrame, form_key: str, exclude_cols: List[str] = None, title="Add new row"):
    exclude_cols = set(exclude_cols or [])
    with st.form(form_key, clear_on_submit=True):
        st.markdown(f"##### {title}")
        new_row = {}
        for col in df_ref.columns:
            if col in exclude_cols:
                continue
            dtype = df_ref[col].dtype if col in df_ref.columns else object
            kind = str(dtype)
            key = f"{form_key}_{col}"
            if col == COL_USER_GRP and COL_USER_GRP in df_ref.columns:
                # this path only used if you call with a df that contains COL_USER_GRP
                pass
            if "int" in kind:
                new_row[col] = st.number_input(col, step=1, value=0, key=key)
            elif "float" in kind:
                new_row[col] = st.number_input(col, step=0.01, value=0.0, key=key, format="%.6f")
            else:
                new_row[col] = st.text_input(col, value="", key=key)
        submitted = st.form_submit_button("Add")
        return submitted, new_row
# ============================ MAIN APP ======================================
def main():
    header()

    engine = sql_alchemy_from_secrets()
    g_raw, u_raw = fetch_data(engine)

    # Maintain original snapshots
    if "orig_group" not in st.session_state:
        st.session_state.orig_group = with_row_id(g_raw)
        st.session_state.group = st.session_state.orig_group.copy()
    if "orig_user" not in st.session_state:
        st.session_state.orig_user = with_row_id(u_raw)
        st.session_state.user = st.session_state.orig_user.copy()

    valid_groups = sorted(st.session_state.group[COL_GROUP_KEY].astype(str).unique().tolist())

    tab_g, tab_u = st.tabs(["📋 Group Master", "👤 User Master"])

    # ------------------------- GROUP MASTER TAB -------------------------
    with tab_g:
        st.subheader("Group Master")
        t1a, t1b, t1c = st.columns([1, 1, 6])
        with t1a:
            if st.button("➕ Add Row", key="g_add"):
                st.session_state["show_g_add"] = True
        with t1b:
            if st.button("↩️ Reset", key="g_reset"):
                st.session_state.group = st.session_state.orig_group.copy()
                st.session_state["show_g_add"] = False
                st.success("Group Master reset to original snapshot.")

        if st.session_state.get("show_g_add", False):
            # Simple add form; ID_COL is excluded
            submitted, new_row = add_form(
                st.session_state.orig_group.drop(columns=[ID_COL]),
                "g_add_form",
                exclude_cols=[ID_COL],
                title="Add Group"
            )
            if submitted:
                # Ensure required fields exist
                for required in [COL_GROUP_KEY]:
                    if not str(new_row.get(required, "")).strip():
                        st.error(f"'{required}' is required.")
                        st.stop()
                # Prevent dupes in-session
                existing = st.session_state.group[st.session_state.group[COL_GROUP_KEY].astype(str) == str(new_row[COL_GROUP_KEY])]
                if not existing.empty:
                    st.error(f"Group '{new_row[COL_GROUP_KEY]}' already exists in current view.")
                else:
                    # assign a new row id
                    new_row[ID_COL] = (st.session_state.group[ID_COL].max() + 1) if not st.session_state.group.empty else 0
                    st.session_state.group = pd.concat([st.session_state.group, pd.DataFrame([new_row])], ignore_index=True)
                    st.session_state["show_g_add"] = False
                    st.success("Group added to pending changes.")

        grid_opts_g = build_grid_options(
            st.session_state.group,
            editable=True,
            selectable=True
        )
        ret_g = AgGrid(
            st.session_state.group,
            gridOptions=grid_opts_g,
            update_mode=GridUpdateMode.NO_UPDATE,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            theme="streamlit",
            allow_unsafe_jscode=False
        )
        edited_g = clean_grid_df(ret_g["data"])
        edited_g = edited_g[[c for c in st.session_state.group.columns if c in edited_g.columns]]
        st.session_state.group = edited_g.copy()

        del_col_g, _ = st.columns([1, 7])
        with del_col_g:
            if st.button("🗑️ Delete Selected", key="g_del"):
                sel = ret_g.get("selected_rows", [])
                if sel:
                    sel_ids = [row.get(ID_COL) for row in sel if row.get(ID_COL) is not None]
                    st.session_state.group = st.session_state.group[~st.session_state.group[ID_COL].isin(sel_ids)]
                    st.success(f"Marked {len(sel_ids)} row(s) for deletion.")
                else:
                    st.warning("No rows selected.")

    # ------------------------- USER MASTER TAB -------------------------
    with tab_u:
        st.subheader("User Master")
        t2a, t2b, t2c = st.columns([1, 1, 6])
        with t2a:
            if st.button("➕ Add Row", key="u_add"):
                st.session_state["show_u_add"] = True
        with t2b:
            if st.button("↩️ Reset", key="u_reset"):
                st.session_state.user = st.session_state.orig_user.copy()
                st.session_state["show_u_add"] = False
                st.success("User Master reset to original snapshot.")

        if st.session_state.get("show_u_add", False):
            with st.form("u_add_form", clear_on_submit=True):
                st.markdown("##### Add User")
                uid = st.text_input(COL_USER_ID, "")
                grp = st.selectbox(COL_USER_GRP, options=valid_groups, index=0 if valid_groups else None)
                if st.form_submit_button("Add"):
                    if not uid.strip():
                        st.error("User ID is required.")
                    elif grp is None:
                        st.error("Please select a group.")
                    elif (st.session_state.user[COL_USER_ID].astype(str) == uid).any():
                        st.error(f"User '{uid}' already exists in current view.")
                    else:
                        new_row = {COL_USER_ID: uid, COL_USER_GRP: grp, ID_COL: (st.session_state.user[ID_COL].max() + 1) if not st.session_state.user.empty else 0}
                        st.session_state.user = pd.concat([st.session_state.user, pd.DataFrame([new_row])], ignore_index=True)
                        st.session_state["show_u_add"] = False
                        st.success("User added to pending changes.")

        # AgGrid with dropdown editor for user_group
        grid_opts_u = build_grid_options(
            st.session_state.user,
            editable=True,
            selectable=True,
            select_options={COL_USER_GRP: valid_groups}
        )
        ret_u = AgGrid(
            st.session_state.user,
            gridOptions=grid_opts_u,
            update_mode=GridUpdateMode.NO_UPDATE,
            data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
            fit_columns_on_grid_load=True,
            theme="streamlit",
            allow_unsafe_jscode=False
        )
        edited_u = clean_grid_df(ret_u["data"])
        edited_u = edited_u[[c for c in st.session_state.user.columns if c in edited_u.columns]]
        st.session_state.user = edited_u.copy()

        del_col_u, _ = st.columns([1, 7])
        with del_col_u:
            if st.button("🗑️ Delete Selected", key="u_del"):
                sel = ret_u.get("selected_rows", [])
                if sel:
                    sel_ids = [row.get(ID_COL) for row in sel if row.get(ID_COL) is not None]
                    st.session_state.user = st.session_state.user[~st.session_state.user[ID_COL].isin(sel_ids)]
                    st.success(f"Marked {len(sel_ids)} row(s) for deletion.")
                else:
                    st.warning("No rows selected.")

    st.divider()

    # ===================== APPLY CHANGES (DB WRITE) =====================
    c1, c2 = st.columns([1, 6])
    with c1:
        if st.button("✅ Apply All Changes", use_container_width=True):
            # Compute diffs
            g_added, g_edited, g_deleted = compute_changes(st.session_state.group.drop_duplicates(subset=[ID_COL]), st.session_state.orig_group.drop_duplicates(subset=[ID_COL]))
            u_added, u_edited, u_deleted = compute_changes(st.session_state.user.drop_duplicates(subset=[ID_COL]),  st.session_state.orig_user.drop_duplicates(subset=[ID_COL]))

            # Validate foreign keys for user table (added + edited)
            pending_user = pd.concat([u_added, u_edited], ignore_index=True) if not u_added.empty or not u_edited.empty else pd.DataFrame(columns=st.session_state.user.columns)
            fk_errors = fk_validate_users(pending_user[[COL_USER_ID, COL_USER_GRP]], valid_groups)
            # Guard group deletions
            guard_errors = guard_group_deletes(g_deleted[[COL_GROUP_KEY]], engine)

            if fk_errors or guard_errors:
                st.error("Cannot apply changes due to following issues:")
                for e in fk_errors + guard_errors:
                    st.write(f"- {e}")
                st.stop()

            # Apply in safe order:
            # 1) Apply GROUP changes (inserts/updates) before USER inserts/updates (for FK)
            # 2) Apply USER deletes before GROUP deletes (for FK)
            try:
                # First: apply group adds/edits
                apply_group_changes(engine, g_added, g_edited, pd.DataFrame(columns=g_deleted.columns))
                # Then: apply user adds/edits/deletes (delete first to free FK if users moved)
                apply_user_changes(engine, pd.DataFrame(columns=u_added.columns), pd.DataFrame(columns=u_edited.columns), u_deleted)
                apply_user_changes(engine, u_added, u_edited, pd.DataFrame(columns=u_deleted.columns))
                # Finally: group deletes (already guarded)
                apply_group_changes(engine, pd.DataFrame(columns=g_added.columns), pd.DataFrame(columns=g_edited.columns), g_deleted)
            except Exception as ex:
                st.exception(ex)
                st.stop()

            st.success("✅ Changes applied to Oracle successfully.")

            # Refresh base snapshots from DB
            g_raw2, u_raw2 = fetch_data(engine)
            st.session_state.orig_group = with_row_id(g_raw2)
            st.session_state.group = st.session_state.orig_group.copy()

            st.session_state.orig_user = with_row_id(u_raw2)
            st.session_state.user = st.session_state.orig_user.copy()

            # Update valid groups for UI
            st.session_state["show_g_add"] = False
            st.session_state["show_u_add"] = False

    with c2:
        # Live diff summary
        g_added, g_edited, g_deleted = compute_changes(st.session_state.group, st.session_state.orig_group)
        u_added, u_edited, u_deleted = compute_changes(st.session_state.user,  st.session_state.orig_user)

        st.caption("Pending changes preview (before applying):")

        st.markdown("**Group Master**")
        m1, m2, m3 = st.columns(3)
        m1.metric("Added", len(g_added))
        m2.metric("Edited", len(g_edited))
        m3.metric("Deleted", len(g_deleted))
        if len(g_added):   st.dataframe(g_added.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(g_edited):  st.dataframe(g_edited.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(g_deleted): st.dataframe(g_deleted.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(g_added) == len(g_edited) == len(g_deleted) == 0:
            st.info("No pending changes in Group Master.")

        st.markdown("---")
        st.markdown("**User Master**")
        n1, n2, n3 = st.columns(3)
        n1.metric("Added", len(u_added))
        n2.metric("Edited", len(u_edited))
        n3.metric("Deleted", len(u_deleted))
        if len(u_added):   st.dataframe(u_added.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(u_edited):  st.dataframe(u_edited.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(u_deleted): st.dataframe(u_deleted.drop(columns=[ID_COL]), use_container_width=True, height=160)
        if len(u_added) == len(u_edited) == len(u_deleted) == 0:
            st.info("No pending changes in User Master.")

if __name__ == "__main__":
    main()