import streamlit as st
import json
import sys
import pandas as pd

from src.Main_Page import Main_Page
from src.Schedule_Check_Page import Schedule_Check_Page
from src.Job_Edit_Page import Job_Edit_Page
from src.start import StartPage
from src.column_select import ColumnPage
from src.report_package import ReportPage
from src.notification import NotificationPage
from src.DQ_Check import DQCheckPage
from src.Scheduled_Checks import ScheduledChecksPage
from src.Metrics_Page import MetricsPage
from src.metrics_alert_monitoring import table_metrics
from dotenv import load_dotenv

# Add all databases you wish the access to this array
st.session_state.databases = ["HART_DB","DM_FDP","DM_FDP_UAT_2","STANDARDIZED","DATAHUB"]

load_dotenv(override=True)
st.session_state["streamlit_mode"] = "SiS"
def set_session():
    try:
        import snowflake.permissions as permissions

        session = get_active_session()

        st.session_state["streamlit_mode"] = "NativeApp"
    except:
        try:
            session = get_active_session()

            st.session_state["streamlit_mode"] = "SiS"
        except:
            import snowflake_conn as sfc

            session = sfc.init_snowpark_session("account")

            st.session_state["streamlit_mode"] = "OSS"

    return session

st.set_page_config(layout="wide")
APP_OPP_DB = "DATA_QUALITY"
APP_CONFIG_SCHEMA = "CONFIG"
APP_RESULTS_SCHEMA = "RESULTS"
APP_DATA_SCHEMA = "DATA"
APP_TEMP_DATA_SCHEMA = "TEMPORARY_DQ_OBJECTS"
st.session_state.pag_interval = 10
st.session_state.warehouses = ["DQ_L"]
st.session_state.non_stat_proc='dq_non_stat_sproc'
st.session_state.anomoly_proc='dq_anomaly_detection_sproc'

dates_chron_dict = {
            "Hourly": "0 * * * *",
            "Daily":"0 1 * * *",
            "Weekly": "0 1 * * 1",
            "Monthly":"0 1 1 * *",
            "Annually":"0 1 1 1 *"
        }
reverse_chron_dict = inv_map = {v: k for k, v in dates_chron_dict.items()}

if 'snowflake_import_directory' in sys._xoptions:

    print("We are in Snowflake.")

    from snowflake.snowpark.context import get_active_session

    st.session_state.session = get_active_session()

else:

    print("We are not in Snowflake.")

    st.session_state.session = st.connection("snowflake").session()
session = st.session_state.session



QUERY_TAG = {"origin": "sf_sit",
             "name": "data_quality_solution",
             "version": '{major: 1, minor: 0}'
            }

@st.cache_data
def sql_to_dataframe(sql: str) -> pd.DataFrame:
    return session.sql(sql).to_pandas(
        statement_params={
            "QUERY_TAG": json.dumps(QUERY_TAG)
        }
    )


st.title("Data Quality Manager")

st.session_state.databases.append(APP_OPP_DB)

if "catalog_info" not in st.session_state:
    with st.spinner("Loading Catalog"):
        databases = ("', '").join(st.session_state.databases)
        schemas = (f"SELECT CATALOG_NAME, SCHEMA_NAME FROM SNOWFLAKE.ACCOUNT_USAGE.SCHEMATA WHERE CATALOG_NAME in ('{databases}') AND DELETED IS NULL ORDER BY SCHEMA_NAME")
        schema_df = session.sql(schemas).to_pandas()
        schemas = ("', '").join(list(schema_df["SCHEMA_NAME"]))
        tables_df = session.sql(f"SELECT TABLE_NAME, TABLE_SCHEMA FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE TABLE_CATALOG in ('{databases}') and TABLE_SCHEMA in ('{schemas}') AND DELETED IS NULL ORDER BY TABLE_NAME").to_pandas()
        database_dict = {}
        for database in st.session_state.databases:
            database_dict[database] = []
            schemas = schema_df[schema_df["CATALOG_NAME"] == database]
            for index,schema in schemas.iterrows():
                schema = schema["SCHEMA_NAME"]
                tables = list(tables_df[tables_df["TABLE_SCHEMA"] == schema]["TABLE_NAME"])
                new_obj = {
                    "schema" : schema,
                    "tables" :tables
                }
                database_dict[database].append(new_obj)
        st.session_state.catalog_info = database_dict


if 'session' not in st.session_state:
    st.session_state.session = set_session()

pages = [Main_Page(), Schedule_Check_Page(),MetricsPage(), table_metrics(), Job_Edit_Page(), StartPage(), ColumnPage(), ReportPage(), NotificationPage(), DQCheckPage(), ScheduledChecksPage()]

with st.sidebar:
    
    if st.button("Notification Page", key='dfgs', use_container_width=True):
        st.session_state.current_page = 'not_page'
        st.rerun()
    if st.button("Data Quality Checks", key='dataquality_check', use_container_width=True):
        st.session_state.current_page = 'dataquality_check'
        st.rerun()
    if st.button("Scheduled Checks", key='sched_checks', use_container_width=True):
        st.session_state.current_page = 'sched_checks'
        st.rerun()
    if st.button("Manual DMF Metrics", key='metrics_page', use_container_width=True):
        st.session_state.current_page = 'metrics_page'
        st.rerun()
    if st.button("Table DMF Metrics", key='table_metrics', use_container_width=True):
        st.session_state.current_page = 'table_metrics'
        st.rerun()



if "current_page" not in st.session_state:
    st.session_state["current_page"] = 'not_page'

for page in pages:
    if page.name == st.session_state["current_page"]:
        page.print_page()
        page.print_sidebar()