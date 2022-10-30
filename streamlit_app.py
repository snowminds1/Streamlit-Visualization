import streamlit as st
from snowflake.snowpark import Session


st.write("My cool account:", st.secrets["snowflake"]["account"])
st.write("My cool user:", st.secrets["snowflake"]["user"])
st.write("My cool password:", st.secrets["snowflake"]["password"])
st.write("My cool role:", st.secrets["snowflake"]["role"])
st.write("My cool warehouse:", st.secrets["snowflake"]["warehouse"])
st.write("My cool database:", st.secrets["snowflake"]["database"])
st.write("My cool schema:", st.secrets["snowflake"]["schema"])

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
print(df)
