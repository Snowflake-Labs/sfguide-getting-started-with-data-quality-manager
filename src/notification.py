import streamlit as st
import time
import random
import json
import sys
import os
import math
import pandas as pd
import matplotlib.pyplot as plt
import snowflake.snowpark.functions as F

from abc import ABC, abstractmethod
from dotenv import load_dotenv
from snowflake.snowpark import Row, Session
from snowflake.snowpark import types as T
from datetime import datetime, timedelta
from ast import literal_eval

from src.job_class import Job
from src.Page import Page
from src.globals import APP_OPP_DB, APP_CONFIG_SCHEMA, APP_TEMP_DATA_SCHEMA, APP_RESULTS_SCHEMA, APP_DATA_SCHEMA, dates_chron_dict, reverse_chron_dict
from src.tools import toggle_button, get_tables, get_schemas, get_anomaly_chart, change_page, print_nsc_results, pag_up, pag_down
from src.Paginator import paginator

class NotificationPage(Page):
    def __init__(self):
        self.name = 'not_page'
        pass

    def read_all_notes(self,notes):
        for note in notes:
            id = note[0]
            read = note[7]
            type = note[6]
            if read is 'pending review':
                self.read_note(id,type)
    def read_note(self,note_id, note_type):
        session = st.session_state.session
        if note_type == 'ANOMOLY':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        elif note_type == 'NON-STAT':
            sql_str = f"UPDATE {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS SET ALERT_STATUS = 'READ' WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}'"
        session.sql(sql_str).collect()

    def save_edits(self, df, table):

        session = st.session_state.session

        t = session.table(table).filter(F.col("ALERT_FLAG") == 1)

        df = df[df["COMMENTS"].notnull()]

        if "RECORD_IDS" in df.columns:

            t_new = session.createDataFrame(df).with_column("RECORD_IDS",
                                                            F.parse_json(F.col("RECORD_IDS")).cast(T.VariantType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) &
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) &
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) &
                 (t['RECORD_IDS'] == t_new['RECORD_IDS'])),
                [
                    F.when_matched().update(
                        {"COMMENTS": t_new["COMMENTS"]}
                    )
                ]
            )

        else:
            t_new = session.createDataFrame(df).with_column("PARTITION_VALUES",
                                                            F.parse_json(F.col("PARTITION_VALUES")).cast(T.ArrayType()))
            session.table(table).merge(
                t_new,
                ((t['JOB_ID'] == t_new['JOB_ID']) &
                 (t['RUN_DATETIME'] == t_new['RUN_DATETIME']) &
                 (t['CHECK_TYPE_ID'] == t_new['CHECK_TYPE_ID']) &
                 (t['PARTITION_VALUES'] == t_new['PARTITION_VALUES'])),
                [
                    F.when_matched().update(
                        {"COMMENTS": t_new["COMMENTS"]}
                    )
                ]
            )
    def print_page(self):
        session = st.session_state.session

        if "note_paginator" not in st.session_state or st.session_state.note_paginator["start"] == \
                st.session_state.note_paginator["end"]:
            st.session_state.note_paginator = {"start": 0, "end": st.session_state.pag_interval}
        search, show, date, DQC, jc = st.columns((5, 2, 2, 2, 2))
        search = search.text_input("Search")
        show = show.selectbox("Show", ["All", "Read", "Unread"])
        show = "= 'READ'" if show == "Read" else "= 'pending review'" if show == 'Unread' else " ilike '%%' OR ALERT_STATUS IS NULL"
        start_date = date.date_input("Start Date", value=(datetime.today() - timedelta(days=30)))
        end_date = DQC.date_input("End Date")
        job_info = session.sql(
            f"""SELECT DISTINCT IFNULL(IFF(JOB_NAME = '',NULL,JOB_NAME), 'No Job Name') as JOB_NAME,JOB_ID FROM {APP_OPP_DB}.{APP_CONFIG_SCHEMA}.DQ_JOBS ORDER BY JOB_NAME""").to_pandas()
        job_names = list(job_info["JOB_NAME"])
        job_names.insert(0, '')
        job_name = jc.selectbox("Filter by job", job_names)
        if job_name != '':
            job_id_f = list(job_info[job_info["JOB_NAME"] == job_name]["JOB_ID"])[0]
            alert_flag = ''
            alert_field = ', IFF(SUM(ALERT_FLAG)>0, 1, 0) AS ALERT_FLAG'
            alert_grouper = ''
            emoji_flag = True
        else:
            job_id_f = '%%'
            alert_flag = 'and ALERT_FLAG = 1'
            alert_field = ', ALERT_FLAG'
            alert_grouper = alert_field
            emoji_flag = False
        notifications = session.sql(
            f"""SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), CHECK_TBL_NM {alert_field}, 'ANOMOLY' as CHECK_TYPE, ALERT_STATUS 
                FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS M 
                WHERE CHECK_TBL_NM ILIKE '%{search}%'
                and JOB_ID ilike '{job_id_f}'
                {alert_flag}
                and (ALERT_STATUS {show})
                AND RUN_DATETIME >= CONCAT('{start_date}', ' 00:00:00')
                AND RUN_DATETIME <= CONCAT('{end_date}', ' 23:59:59') 
                GROUP BY JOB_ID,CHECK_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS
                UNION
                SELECT (JOB_ID||'_'||RUN_DATETIME) AS RUN_KEY ,JOB_ID, RUN_DATETIME, COUNT(*), CONTROL_TBL_NM {alert_field}, 'NON-STAT' as CHECK_TYPE, ALERT_STATUS 
                FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS M 
                WHERE CONTROL_TBL_NM ILIKE '%{search}%'
                and JOB_ID ilike '{job_id_f}'
                {alert_flag}
                and (ALERT_STATUS {show})
                AND RUN_DATETIME >= CONCAT('{start_date}', ' 00:00:00')
                AND RUN_DATETIME <= CONCAT('{end_date}', ' 23:59:59')
                GROUP BY JOB_ID,CONTROL_TBL_NM,RUN_DATETIME {alert_grouper}, ALERT_STATUS order by ALERT_STATUS DESC, RUN_DATETIME DESC 
                """).collect()
        # st.write("#")
        st.button("Mark all as read", type="primary", on_click=self.read_all_notes, args=(notifications,))
        st.write("-----")
        # st.write(notifications)
        total_pages = int(len(notifications) / st.session_state.pag_interval)
        current_page = int(st.session_state.note_paginator["end"] / st.session_state.pag_interval)
        i = 0
        for note in notifications[st.session_state.note_paginator["start"]:st.session_state.note_paginator["end"]]:
            Job(note,job_info, i, emoji_flag)
        #     note_id = note[0]
        #     Job_id = note[1]
        #     run_time = note[2]
        #     count = note[3]
        #     table = note[4]
        #     alert_flag = note[5]
        #     type = note[6]
        #     read = note[7]
        #     # st.write(read)

        #     job_name = list(job_info[job_info["JOB_ID"] == Job_id]["JOB_NAME"])[0] if len(
        #         list(job_info[job_info["JOB_ID"] == Job_id]["JOB_NAME"])) > 0 else "No Job Name"
        #     description, button1, button2 = st.columns((19, 2, 2))
        #     description.subheader(f"**{job_name}**")
        #     description.write(run_time)

        #     button1.write("#")
        #     if (read == 'pending review'):
        #         button1.button("Mark as read", type="primary", key="mark read" + str(note_id), on_click=self.read_note,
        #                        args=(note_id, type))
        #     else:
        #         button1.write(" ")
        #     button2.write("#")
        #     if "show_flag" + str(note_id) not in st.session_state:
        #         st.session_state["show_flag" + str(note_id)] = False
        #     button2.button("Show more", key="show" + str(note_id) + str(i), on_click=toggle_button,
        #                    args=("show_flag" + str(note_id),))

        #     # i = i + 1
        #     # st.write(st.session_state["show_flag"+str(note_id)])
        #     if type == "ANOMOLY":
        #         if emoji_flag:
        #             if alert_flag == 0:
        #                 message = ':white_check_mark: ***No Alerts for run***'
        #             elif alert_flag == 1:
        #                 message = f':x: ***{count} anomilies found in {table}***'
        #         else:
        #             message = f'***{count} anomilies found in {table}***'
        #         description.write(f"{message}")
        #         if (st.session_state["show_flag" + str(note_id)]):
        #             df = session.sql(
        #                 f"SELECT * FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}' AND ALERT_FLAG=1").collect()
        #             # df['PARTITION_VALUES'] = df["PARTITION_VALUES"].astype('string').replace('\\[','')
        #             # df['PARTITION_STR'] = [row["PARTITION_VALUES"].split(',') for index,row in df.iterrows()]
        #             edited_df = st.data_editor(df, key="editdf" + str(note_id) + str(i))
        #             st.button("Save", type='primary', on_click=self.save_edits,
        #                       args=(edited_df, f'{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS'),
        #                       key="save_btn" + str(note_id) + str(i))
        #             with st.expander("Results"):
        #                 get_anomaly_chart(f"{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_ANOMALY_DETECT_RESULTS", note_id, 1)
        #     elif type == 'NON-STAT':
        #         if emoji_flag:
        #             if alert_flag == 0:
        #                 message = ':white_check_mark: ***No Alerts for run***'
        #             elif alert_flag == 1:
        #                 message = f':x: ***{count} alerts found in {table}***'
        #         else:
        #             message = f'***{count} alerts found in {table}***'
        #         description.write(f"{message}")
        #         if (st.session_state["show_flag" + str(note_id)]):
        #             df = session.sql(
        #                 f"SELECT * FROM {APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS WHERE (JOB_ID||'_'||RUN_DATETIME) = '{note_id}' AND ALERT_FLAG=1").collect()
        #             # df['PARTITION_STR'] = [row["PARTITION_VALUES"].split(',') for index,row in df.iterrows()]
        #             edited_df = st.data_editor(df, key="editdf" + str(note_id) + str(i))
        #             description.write(f'Table impacted: {table}')
        #             st.button("Save", type='primary', on_click=self.save_edits,
        #                       args=(edited_df, f'{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS'),
        #                       key="save_btn" + str(note_id) + str(i))
        #             with st.expander("Results"):
        #                 print_nsc_results(f"{APP_OPP_DB}.{APP_RESULTS_SCHEMA}.DQ_NON_STAT_CHECK_RESULTS", note_id, 1)
            i = i + 1
            st.write("-----")
        if len(notifications) > st.session_state.pag_interval:
            space, btn1, text, btn2, space = st.columns([20, 2, 2, 2, 20])
            text.write(f"Page ***{current_page}/{total_pages}***")
            if st.session_state.note_paginator["start"] > 0:
                btn1.button("Prev", on_click=pag_down, key="note_next", args=('note',))
            if st.session_state.note_paginator["end"] < len(notifications):
                btn2.button("Next", on_click=pag_up, key="note_back", args=('note',))


    # Repeatable element: sidebar buttons that navigate to linked pages
    def print_sidebar(self):
        pass
