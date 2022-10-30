import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
from matplotlib import pyplot as plt
import altair as alt
from numerize import numerize


st.title('Uber pickups in NYC')
connection_parameters = {
    "account": "dh04284.ap-southeast-1",
    "user": "SNOWMINDS",
    "password": "TeamSF@2022",
    "role": "FIN_DEVELOPER",
    "warehouse": "VWH_FIN_TRS",
    "database": "D_FIN_LND",
    "schema": "FIN_WBG"
}

test_session = Session.builder.configs(connection_parameters).create()
df_table = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
df = pd.DataFrame(df_table)
print(df)
#st.sidebar.title("Custom Filters")


with st.container():
   st.write("Custom Filters")
   st.columns(3)
   col1, col2, col3 = st.columns(3)

   with col1:
       st.header("Select Sender")
       df_table1 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
       df1 = pd.DataFrame(df_table1)
       select = st.selectbox('Select a State', df1['SENDER'],key='select1')
       st.write('You selected:', select)
       df_table3 = test_session.sql(
           "SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where sender= '"+select+ "' group by 1 ").collect()
       df3 = pd.DataFrame(df_table3)
       st.bar_chart(data=df3, x="TYPE_TRAN", y="AMOUNT_IN_MILLION", width=0, height=0, use_container_width=True)

   with col2:
       st.header("Select receiver")
       df_table2 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
       df2 = pd.DataFrame(df_table2)
       select2 = st.selectbox('Select a State', df2['SENDER'],key='select2')
       st.write('You selected:', select2)
       df_table3 = test_session.sql(
           "SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
       df3 = pd.DataFrame(df_table3)
       st.bar_chart(data=df3, x="TYPE_TRAN", y="AMOUNT_IN_MILLION", width=0, height=0, use_container_width=True)


"Energy Costs By Month"
st.bar_chart(data=df, x="TYPE_TRAN", y="AMOUNT_IN_MILLION", width=0, height=0, use_container_width=True)

"Energy Costs By Month"
st.bar_chart(data=df, x="TYPE_TRAN", y="AMOUNT_IN_MILLION", width=0, height=0, use_container_width=True)
# d = alt.Chart(df).mark_bar().encode(
#     x='TYPE_TRAN',
#     y='AMOUNT',
#     tooltip=['AMOUNT']
# )

# d = alt.Chart(df).mark_bar().encode(
#     alt.Y('AMOUNT_IN_MILLION:Q',
#           scale=alt.Scale(domain=(1000, 16000),clamp=True)),
#     x='TYPE_TRAN',
#     tooltip=['AMOUNT_IN_MILLION']
# )
#
# st.altair_chart(d)
