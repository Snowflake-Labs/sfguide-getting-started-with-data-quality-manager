import streamlit as st
import pandas as pd
import time

from src.Page import Page
from src.tools import sql_to_dataframe,sql_to_pandas
from snowflake.snowpark.context import get_active_session

class table_metrics(Page):
    
    def __init__(self):
        self.name = "table_metrics"

    def print_page(self):

        session = st.session_state.session


        st.header('Pipeline Monitoring Dashboard')

        st.divider()
        st.subheader('📊 Data Quality Metrics')

        col1, col2, col3 = st.columns([1,1,1])

        @st.cache_data
        def LOAD_RECENT_METRICS():
            METRICS_QUERY = """
                with LATEST_METRIC as (
                    select
                        METRIC_NAME as QUALITY_CHECK,
                        TABLE_NAME,
                        ARGUMENT_NAMES,
                        max(MEASUREMENT_TIME) as LATEST_MEASUREMENT_TIME,
                        array_agg(VALUE) within group (order by MEASUREMENT_TIME) as VALUE_HISTORY
                    from
                        SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
                    where
                        timediff(day, MEASUREMENT_TIME, current_timestamp()) < 30
                    group by
                        TABLE_NAME,
                        QUALITY_CHECK,
                        ARGUMENT_NAMES
                )
                select
                    case when D.METRIC_DATABASE = 'SNOWFLAKE' then concat('❄️ ', L.QUALITY_CHECK)
                         when D.METRIC_DATABASE != 'SNOWFLAKE' then concat('💼 ', L.QUALITY_CHECK)
                    end as DMF_NAME,
                    L.TABLE_NAME,
                    L.ARGUMENT_NAMES,
                    case when D.VALUE > 0 then concat('🚨 ', D.VALUE)
                         when D.VALUE = 0 then concat('✅ ', D.VALUE)
                    end as ISSUES_FOUND,
                    L.VALUE_HISTORY,
                    concat(to_char(convert_timezone('Europe/Berlin', D.MEASUREMENT_TIME), 'YYYY-MM-DD at HH:MI:SS'),' (',(timediff(minute, D.MEASUREMENT_TIME, current_timestamp())),' minutes ago)') as LAST_MEASURED
                from
                    LATEST_METRIC L
                join
                    SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS D
                    on  L.TABLE_NAME = D.TABLE_NAME
                    and L.QUALITY_CHECK = D.METRIC_NAME
                    and L.LATEST_MEASUREMENT_TIME = D.MEASUREMENT_TIME
                order by
                    LAST_MEASURED desc
                """
            
            METRICS = sql_to_dataframe(METRICS_QUERY)
            METRICS_DF = pd.DataFrame(METRICS)
            
            return METRICS_DF


        # Load the DataFrame
        METRICS_DF = LOAD_RECENT_METRICS()

        # Function to add 'ALL' option to the unique values list
        def add_all_option(unique_values):
            return ['All'] + unique_values.tolist()


        METRICS_COUNTER = sql_to_dataframe("""
                with LATEST_METRIC as (
                    select
                        METRIC_NAME as QUALITY_CHECK,
                        TABLE_NAME,
                        ARGUMENT_NAMES,
                        max(MEASUREMENT_TIME) as LATEST_MEASUREMENT_TIME
                    from
                        SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS
                    where
                        timediff(day, MEASUREMENT_TIME, current_timestamp()) < 14
                    group by
                        TABLE_NAME,
                        QUALITY_CHECK,
                        ARGUMENT_NAMES
                ),
                LATEST_VALUE as(
                    select
                        D.VALUE as VALUE
                    from
                        LATEST_METRIC L
                    join
                        SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS D
                        on  L.TABLE_NAME = D.TABLE_NAME
                        and L.QUALITY_CHECK = D.METRIC_NAME
                        and L.LATEST_MEASUREMENT_TIME = D.MEASUREMENT_TIME
                )
                select 
                    count(*) as ALL_METRICS,
                    (select count(*) from LATEST_VALUE where VALUE > 0) as FAILED_METRICS
                from
                    LATEST_VALUE
                """)
        ALL_METRICS = str(METRICS_COUNTER[0].ALL_METRICS)
        FAILED_METRICS = str(METRICS_COUNTER[0].FAILED_METRICS)

        if FAILED_METRICS is not 0:
            st.error('🚨 '+FAILED_METRICS+' of '+ALL_METRICS+' metrics failed expectations')
        else:
            st.success('✅ '+FAILED_METRICS+' of '+ALL_METRICS+' metrics failed expectations')



        # Create multiselect widgets for each column with 'ALL' option
        if len(METRICS_DF) > 0:
            dmf_name_options = add_all_option(METRICS_DF['DMF_NAME'].unique())
            dmf_table_options = add_all_option(METRICS_DF['TABLE_NAME'].unique())
        else:
            st.warning("No DMF Metrics Returned")
            dmf_name_options = []
            dmf_table_options = []


        dmf_name_filter = col1.selectbox('DMF', options=dmf_name_options)
        dmf_table_filter = col2.selectbox('Table', options=dmf_table_options)
        issues_found_filter = col3.selectbox('Expectations', options= ['Failed', 'All'])


        def filter_dataframe(df, dmf_name_filter, dmf_table_filter, issues_found_filter):
            if dmf_name_filter != 'All':
                df = df[df['DMF_NAME'] == dmf_name_filter]
                
            if dmf_table_filter != 'All':
                df = df[df['TABLE_NAME'] == dmf_table_filter]
                
            if issues_found_filter == 'Failed':
                df = df[df['ISSUES_FOUND'] != '✅ 0']
            elif issues_found_filter == 'All':
                df = df
            return df

        if len(METRICS_DF) > 0:
            FILTERED_DF = filter_dataframe(METRICS_DF, dmf_name_filter, dmf_table_filter, issues_found_filter)
        else:
            FILTERED_DF = pd.DataFrame()


        st.dataframe(FILTERED_DF,
                    column_config={
                        "VALUE_HISTORY": st.column_config.AreaChartColumn("Checks (last 14 days)", y_min=0),
                        "ARGUMENT_NAMES": st.column_config.ListColumn("Columns")
                    },
                     hide_index= True, use_container_width=True)

        st.warning('⚠️ Manual checks already work today but they are not (yet) logged in the Metrics History.')

        with st.expander('Show Metrics History'):
            METRICS_HISTORY = sql_to_dataframe("""
                select
                    to_char(convert_timezone('Europe/Berlin', MEASUREMENT_TIME), 'YYYY-MM-DD at HH:MI:SS') as MEASUREMENT_TIME,
                    case when METRIC_DATABASE = 'SNOWFLAKE' then concat('❄️ ', METRIC_NAME)
                         when METRIC_DATABASE != 'SNOWFLAKE' then concat('💼 ', METRIC_NAME)
                    end as METRIC_NAME,
                    VALUE,
                    TABLE_NAME,
                    ARGUMENT_NAMES,
                    TABLE_SCHEMA
                from 
                    SNOWFLAKE.LOCAL.DATA_QUALITY_MONITORING_RESULTS 
                order by 
                    MEASUREMENT_TIME desc 
                limit 100
                """)
            st.dataframe(METRICS_HISTORY, hide_index= True, use_container_width=True)



        st.divider()


        # st.subheader('🗄️Event Logs')
        # event_table = st.text_input("Event Table Name")
        # col1, col2, col3 = st.columns([1,1,1])
        # SEVERITY_FILTER = col1.selectbox('Severity', options=['All'])
        # LOGGER_FILTER = col2.selectbox('Logger', options=['All'])
        # DATABASE_FILTER = col3.selectbox('Database', key='db2', options= ['All'])

        # if event_table:

        #     ALL_ERRORS = session.sql(f"""
        #                 select
        #                     count(*) as COUNT
        #                 from 
        #                     {event_table}
        #                 where 
        #                     TIMESTAMP > timeadd(hour, -24, current_timestamp()) 
        #                     and RECORD_TYPE = 'LOG'
        #                     and RECORD['severity_text']::string in ('WARN','ERROR','FATAL')
        #                 """).collect()
        #     ERROR_COUNTER = str(ALL_ERRORS[0].COUNT)
            
        #     if ERROR_COUNTER is not 0:
        #         st.error('🚨 '+ERROR_COUNTER+' errors logged over the last 24 hours.')
        #     else:
        #         st.success('✅ '+ERROR_COUNTER+' errors logged over the last 24 hours.')
            
        #     EVENT_LOGS = session.sql(f"""
        #         select
        #             to_char(convert_timezone('UTC', 'Europe/Berlin', TIMESTAMP), 'YYYY-MM-DD at HH:MI:SS') as LOCAL_TIME,
        #             case when RECORD['severity_text']::string = 'INFO'  then 'ℹ️ INFO'
        #                  when RECORD['severity_text']::string = 'WARN'  then '⚠️ WARN'
        #                  when RECORD['severity_text']::string = 'ERROR' then '⛔️ ERROR'
        #                  when RECORD['severity_text']::string = 'FATAL' then '🚨 FATAL'
        #             end as SEVERITY,   
        #             VALUE::string  as MESSAGE,
        #             RESOURCE_ATTRIBUTES['db.user']::string as USER_NAME,
        #             RESOURCE_ATTRIBUTES['snow.session.role.primary.name'] ::string as USER_ROLE,
        #             SCOPE['name']::string  as LOGGER_NAME,
        #             RESOURCE_ATTRIBUTES['snow.schema.name']::string  as SCHEMA_NAME
        #         from 
        #             {event_table}
        #         where 
        #             RECORD_TYPE = 'LOG'
        #             and LOGGER_NAME != 'snowflake.connector.cursor'
        #             and RECORD['severity_text']::string in ('INFO', 'WARN', 'ERROR', 'FATAL')
        #         order by
        #             LOCAL_TIME desc
        #         limit 
        #             100
        #         """).collect()
            
        #     st.dataframe(EVENT_LOGS, hide_index= True, use_container_width=True)
            
            
            
        #     EVENT_COUNTER = session.sql(f"""
        #         select
        #             count(*) as EVENTS,
        #             RECORD['severity_text']::string as SEVERITY,  
        #             date_trunc(hour, convert_timezone('UTC', 'Europe/Berlin', TIMESTAMP))::timestamp as LOCAL_TIME
        #         from 
        #             {event_table}
        #         where 
        #             TIMESTAMP > timeadd(hour, -17, current_timestamp())
        #             and RECORD_TYPE = 'LOG'
        #             and SCOPE['name'] != 'snowflake.connector.cursor'
        #         group by
        #             SEVERITY,
        #             LOCAL_TIME
        #         order by
        #             LOCAL_TIME desc
        #         """).to_pandas()
            
        #     st.bar_chart(EVENT_COUNTER, x='LOCAL_TIME', y='EVENTS', color='SEVERITY')
        # else:
        #     st.warning("Please enter the name of your event table")

    def print_sidebar(self):
        pass