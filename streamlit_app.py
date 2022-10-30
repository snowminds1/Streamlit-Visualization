import streamlit as st
import pandas as pd
from snowflake.snowpark import Session


st.title('Uber pickups in NYC')
connection_parameters = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
}


test_session = Session.builder.configs(connection_parameters).create()
df_table = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
df = pd.DataFrame(df_table)

with st.container():

   st.write("Custom Filters")
   st.columns(4)
   col1, col2, col3 ,col4= st.columns(4)

   with col1:
       d = st.date_input(
           "From Date",
           datetime.date(2019, 7, 6))
       st.write('From Date:', d)

   with col2:
       d = st.date_input(
           "To Date",
           datetime.date(2019, 7, 6))
       st.write('To Date :', d)

   with col3:
       df_table1 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
       df1 = pd.DataFrame(df_table1)
       select = st.selectbox('Sender', df1['SENDER'],key='select1')

   with col4:
       df_table1 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
       df1 = pd.DataFrame(df_table1)
       select = st.selectbox('Payment Type', df1['SENDER'],key='select2')
