import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode, DataReturnMode

st.set_page_config(page_title="Oracle Table Manager", page_icon="🗄️", layout="wide")

# ----------------------------- DB helpers -----------------------------
def sql_alchemy(username, password, host, port, sid):
    dialect = "oracle"
    driver = "oracledb"
    engine_path = f"{dialect}+{driver}://{username}:{password}@{host}:{port}/?service_name={sid}"
    return create_engine(engine_path)

def get_data(engine):
    df1 = pd.read_sql_query("SELECT * FROM ACCESS_PORTAL_GROUP_MASTER", engine)
    df2 = pd.read_sql_query("SELECT * FROM ACCESS_PORTAL_USER_MASTER", engine)
    return df1, df2

# ----------------------------- Utilities -----------------------------
ID_COL = "_row_id"

def with_row_id(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    # Assign stable integer ids for this session
    df[ID_COL] = range(len(df))
    return df

def next_row_id(current_df: pd.DataFrame) -> int:
    return (int(current_df[ID_COL].max()) + 1) if not current_df.empty else 0

def clean_grid_df(df_like) -> pd.DataFrame:
    df = pd.DataFrame(df_like).copy()
    # Drop AgGrid metadata if present
    df = df.drop(columns=["::auto_unique_id::", "_selectedRowNodeInfo"], errors="ignore")
    return df

def build_grid_options(df: pd.DataFrame, *, editable=True, selectable=True) -> dict:
    gb = GridOptionsBuilder.from_dataframe(df)
    gb.configure_default_column(editable=editable, resizable=True, filter=True)
    gb.configure_column(ID_COL, editable=False, hide=True)  # keep ID hidden but available for selection
    if selectable:
        gb.configure_selection(selection_mode="multiple", use_checkbox=True)
    gb.configure_grid_options(domLayout="autoHeight")
    return gb.build()

def align_dtypes_like(target: pd.DataFrame, reference: pd.DataFrame, exclude_cols=None) -> pd.DataFrame:
    target = target.copy()
    exclude_cols = set(exclude_cols or [])
    for col in reference.columns:
        if col in target.columns and col not in exclude_cols:
            try:
                target[col] = target[col].astype(reference[col].dtype)
            except Exception:
                # If cast fails (e.g., invalid strings), skip to avoid crashing
                pass
    return target

def compute_changes(current: pd.DataFrame, original: pd.DataFrame):
    """
    Returns tuple: (added_rows, edited_rows, deleted_rows) as DataFrames.
    Uses ID_COL to compare, ignores ID_COL in content comparison.
    """
    if ID_COL not in current.columns or ID_COL not in original.columns:
        raise ValueError(f"Missing '{ID_COL}' in frames for change detection.")

    cur = current.copy()
    orig = original.copy()

    # Ensure ID is integer-like
    cur[ID_COL] = pd.to_numeric(cur[ID_COL], errors="coerce").astype("Int64")
    orig[ID_COL] = pd.to_numeric(orig[ID_COL], errors="coerce").astype("Int64")

    # Added: in current, not in original
    added_ids = set(cur[ID_COL]) - set(orig[ID_COL])
    added_rows = cur[cur[ID_COL].isin(list(added_ids))].reset_index(drop=True)

    # Deleted: in original, not in current
    deleted_ids = set(orig[ID_COL]) - set(cur[ID_COL])
    deleted_rows = orig[orig[ID_COL].isin(list(deleted_ids))].reset_index(drop=True)

    # Edited: intersection where any non-ID column differs
    common_ids = list(set(cur[ID_COL]) & set(orig[ID_COL]))
    if not common_ids:
        edited_rows = pd.DataFrame(columns=cur.columns)
    else:
        cur_idxed = cur.set_index(ID_COL, drop=False)
        orig_idxed = orig.set_index(ID_COL, drop=False)

        # Ensure same columns to compare (ignore ID_COL)
        compare_cols = [c for c in cur.columns if c in orig.columns and c != ID_COL]
        # Align dtypes to reduce false positives
        cur_idxed[compare_cols] = align_dtypes_like(cur_idxed[compare_cols], orig_idxed[compare_cols]).copy()

        cur_common = cur_idxed.loc[common_ids, compare_cols]
        orig_common = orig_idxed.loc[common_ids, compare_cols]

        changed_mask = (cur_common != orig_common).any(axis=1)
        changed_ids = changed_mask[changed_mask].index.tolist()
        edited_rows = cur_idxed.loc[changed_ids].reset_index(drop=True)

    return added_rows, edited_rows, deleted_rows

def type_aware_input(col_name: str, dtype, key: str):
    """Render an appropriate input widget based on dtype and return value."""
    kind = str(dtype)
    if "int" in kind:
        return st.number_input(col_name, value=0, step=1, key=key)
    if "float" in kind:
        return st.number_input(col_name, value=0.0, step=0.01, key=key, format="%.6f")
    if "bool" in kind:
        return st.checkbox(col_name, value=False, key=key)
    if "datetime" in kind or "date" in kind:
        # Store as ISO string to avoid timezone issues unless your schema expects datetime
        d = st.date_input(col_name, key=key)
        return pd.to_datetime(d)
    # Fallback to text
    return st.text_input(col_name, value="", key=key)

# ----------------------------- Main app -----------------------------
def main():
    # Branding header
    st.markdown(
        """
        <style>
        .big-title { font-size: 28px; font-weight: 700; margin-bottom: 0.2rem; }
        .subtle { color: #666; margin-top: 0; }
        .toolbar .stButton>button { border-radius: 6px; }
        .apply-area .stButton>button { font-weight: 600; }
        </style>
        """,
        unsafe_allow_html=True,
    )
    st.markdown('<div class="big-title">🗄️ Oracle Table Manager</div>', unsafe_allow_html=True)
    st.markdown('<p class="subtle">Curate and control your master data with confident, auditable changes.</p>', unsafe_allow_html=True)

    # DB connection
    username = st.secrets["Database"]["username"]
    password = st.secrets["Database"]["password"]
    host = st.secrets["Database"]["host"]
    port = st.secrets["Database"]["port"]
    sid = st.secrets["Database"]["sid"]

    engine = sql_alchemy(username, password, host, port, sid)
    df1_base, df2_base = get_data(engine)

    # Initialize session state
    if "orig_df1" not in st.session_state:
        st.session_state.orig_df1 = with_row_id(df1_base)
        st.session_state.df1 = st.session_state.orig_df1.copy()
        st.session_state.next_id1 = next_row_id(st.session_state.df1)
        st.session_state.show_add1 = False

    if "orig_df2" not in st.session_state:
        st.session_state.orig_df2 = with_row_id(df2_base)
        st.session_state.df2 = st.session_state.orig_df2.copy()
        st.session_state.next_id2 = next_row_id(st.session_state.df2)
        st.session_state.show_add2 = False

    # Tabs per table
    tab1, tab2 = st.tabs(["📋 Group Master", "👤 User Master"])

    # ------------- Table 1 -------------
    with tab1:
        st.subheader("Group Master")
        with st.container():
            # Toolbar
            tcol1, tcol2, tcol3 = st.columns([1, 1, 5], gap="small")
            with tcol1:
                if st.button("➕ Add Row", key="add_btn1"):
                    st.session_state.show_add1 = True
            with tcol2:
                if st.button("↩️ Reset Changes", key="reset_btn1"):
                    st.session_state.df1 = st.session_state.orig_df1.copy()
                    st.session_state.next_id1 = next_row_id(st.session_state.df1)
                    st.session_state.show_add1 = False
                    st.success("Group Master reset to original snapshot.")

            # Add form (type-aware)
            if st.session_state.show_add1:
                with st.form("add_form1", clear_on_submit=True):
                    st.markdown("##### Add new row")
                    new_row = {}
                    for col in st.session_state.df1.columns:
                        if col == ID_COL:
                            continue
                        # Use original dtypes as baseline
                        dtype_ref = st.session_state.orig_df1[col].dtype if col in st.session_state.orig_df1.columns else object
                        new_row[col] = type_aware_input(col, dtype_ref, key=f"f1_{col}")
                    submitted = st.form_submit_button("Add")
                    if submitted:
                        new_row[ID_COL] = st.session_state.next_id1
                        st.session_state.next_id1 += 1
                        st.session_state.df1 = pd.concat([st.session_state.df1, pd.DataFrame([new_row])], ignore_index=True)
                        st.session_state.show_add1 = False
                        st.success("Row added.")

            # Grid
            grid1_options = build_grid_options(st.session_state.df1, editable=True, selectable=True)
            grid_return1 = AgGrid(
                st.session_state.df1,
                gridOptions=grid1_options,
                update_mode=GridUpdateMode.NO_UPDATE,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                fit_columns_on_grid_load=True,
                theme="streamlit",
            )

            # Persist inline edits from grid to session state
            edited_view1 = clean_grid_df(grid_return1["data"])
            # Reorder columns to match df1 to avoid accidental compare issues
            edited_view1 = edited_view1[[c for c in st.session_state.df1.columns if c in edited_view1.columns]]
            st.session_state.df1 = edited_view1.copy()

            # Delete Selected (must be after grid to capture selection)
            dcol1, dcol2 = st.columns([1, 9])
            with dcol1:
                if st.button("🗑️ Delete Selected", key="del_btn1"):
                    selected = grid_return1["selected_rows"]
                    if isinstance(selected, list) and len(selected) > 0:
                        sel_ids = []
                        for row in selected:
                            # Fallback if _row_id is missing (shouldn't be, but safer)
                            rid = row.get(ID_COL, None)
                            if rid is not None:
                                sel_ids.append(rid)
                        if sel_ids:
                            st.session_state.df1 = st.session_state.df1[~st.session_state.df1[ID_COL].isin(sel_ids)]
                            st.success(f"Deleted {len(sel_ids)} row(s).")
                    else:
                        st.warning("No rows selected.")

    # ------------- Table 2 -------------
    with tab2:
        st.subheader("User Master")
        with st.container():
            # Toolbar
            t2col1, t2col2, t2col3 = st.columns([1, 1, 5], gap="small")
            with t2col1:
                if st.button("➕ Add Row", key="add_btn2"):
                    st.session_state.show_add2 = True
            with t2col2:
                if st.button("↩️ Reset Changes", key="reset_btn2"):
                    st.session_state.df2 = st.session_state.orig_df2.copy()
                    st.session_state.next_id2 = next_row_id(st.session_state.df2)
                    st.session_state.show_add2 = False
                    st.success("User Master reset to original snapshot.")

            # Add form (type-aware)
            if st.session_state.show_add2:
                with st.form("add_form2", clear_on_submit=True):
                    st.markdown("##### Add new row")
                    new_row2 = {}
                    for col in st.session_state.df2.columns:
                        if col == ID_COL:
                            continue
                        dtype_ref = st.session_state.orig_df2[col].dtype if col in st.session_state.orig_df2.columns else object
                        new_row2[col] = type_aware_input(col, dtype_ref, key=f"f2_{col}")
                    submitted2 = st.form_submit_button("Add")
                    if submitted2:
                        new_row2[ID_COL] = st.session_state.next_id2
                        st.session_state.next_id2 += 1
                        st.session_state.df2 = pd.concat([st.session_state.df2, pd.DataFrame([new_row2])], ignore_index=True)
                        st.session_state.show_add2 = False
                        st.success("Row added.")

            # Grid
            grid2_options = build_grid_options(st.session_state.df2, editable=True, selectable=True)
            grid_return2 = AgGrid(
                st.session_state.df2,
                gridOptions=grid2_options,
                update_mode=GridUpdateMode.NO_UPDATE,
                data_return_mode=DataReturnMode.FILTERED_AND_SORTED,
                fit_columns_on_grid_load=True,
                theme="streamlit",
            )

            # Persist inline edits from grid
            edited_view2 = clean_grid_df(grid_return2["data"])
            edited_view2 = edited_view2[[c for c in st.session_state.df2.columns if c in edited_view2.columns]]
            st.session_state.df2 = edited_view2.copy()

            # Delete Selected
            ddcol1, ddcol2 = st.columns([1, 9])
            with ddcol1:
                if st.button("🗑️ Delete Selected", key="del_btn2"):
                    selected2 = grid_return2["selected_rows"]
                    if isinstance(selected2, list) and len(selected2) > 0:
                        sel_ids2 = []
                        for row in selected2:
                            rid = row.get(ID_COL, None)
                            if rid is not None:
                                sel_ids2.append(rid)
                        if sel_ids2:
                            st.session_state.df2 = st.session_state.df2[~st.session_state.df2[ID_COL].isin(sel_ids2)]
                            st.success(f"Deleted {len(sel_ids2)} row(s).")
                    else:
                        st.warning("No rows selected.")

    st.divider()

    # Apply All Changes (shows diff vs original)
    apply_col1, apply_col2 = st.columns([1, 6])
    with apply_col1:
        if st.button("✅ Apply All Changes", use_container_width=True):
            # Compute changes against original snapshots
            cur1 = st.session_state.df1.copy()
            orig1 = st.session_state.orig_df1.copy()
            cur2 = st.session_state.df2.copy()
            orig2 = st.session_state.orig_df2.copy()

            added1, edited1, deleted1 = compute_changes(cur1, orig1)
            added2, edited2, deleted2 = compute_changes(cur2, orig2)

            st.subheader("Changes Summary")

            # Group Master summary
            st.markdown("##### Group Master")
            c1a, c1b, c1c = st.columns(3)
            c1a.metric("Added", len(added1))
            c1b.metric("Edited", len(edited1))
            c1c.metric("Deleted", len(deleted1))
            if len(added1) > 0:
                st.write("Added rows")
                st.dataframe(added1, use_container_width=True, height=200)
            if len(edited1) > 0:
                st.write("Edited rows")
                st.dataframe(edited1, use_container_width=True, height=200)
            if len(deleted1) > 0:
                st.write("Deleted rows")
                st.dataframe(deleted1, use_container_width=True, height=200)
            if len(added1) == len(edited1) == len(deleted1) == 0:
                st.info("No changes detected in Group Master.")

            st.markdown("---")

            # User Master summary
            st.markdown("##### User Master")
            c2a, c2b, c2c = st.columns(3)
            c2a.metric("Added", len(added2))
            c2b.metric("Edited", len(edited2))
            c2c.metric("Deleted", len(deleted2))
            if len(added2) > 0:
                st.write("Added rows")
                st.dataframe(added2, use_container_width=True, height=200)
            if len(edited2) > 0:
                st.write("Edited rows")
                st.dataframe(edited2, use_container_width=True, height=200)
            if len(deleted2) > 0:
                st.write("Deleted rows")
                st.dataframe(deleted2, use_container_width=True, height=200)
            if len(added2) == len(edited2) == len(deleted2) == 0:
                st.info("No changes detected in User Master.")

    with apply_col2:
        st.caption("Review all pending changes. You can reset each table to the original snapshot before applying to your database layer.")

if __name__ == "__main__":
    main()
