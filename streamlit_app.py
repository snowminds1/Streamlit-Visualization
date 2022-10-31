import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import datetime
import altair as alt

st.set_page_config(layout="wide")
st.title('Money in Motion Dashboard - Pattern Analysis')
connection_parameters = {
    "account": st.secrets["snowflake"]["account"],
    "user": st.secrets["snowflake"]["user"],
    "password": st.secrets["snowflake"]["password"],
    "role": st.secrets["snowflake"]["role"],
    "warehouse": st.secrets["snowflake"]["warehouse"],
    "database": st.secrets["snowflake"]["database"],
    "schema": st.secrets["snowflake"]["schema"],
}


sender_select = ''
payment_type_select = ''
from_dt = datetime.date.today()
to_dt = datetime.date.today()


test_session = Session.builder.configs(connection_parameters).create()
df_table = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
df = pd.DataFrame(df_table)

# "with" notation
with st.sidebar:
    st.write("Custom Filters")
    today = datetime.date.today()
    year = today.year
    from_dt = st.date_input(
        "From Date",
        datetime.date(year,1,1))
    #st.write('From Date:', from_dt)

    to_dt = st.date_input(
        "To Date",
        datetime.date.today())
    #st.write('To Date :', to_dt)

    df_table1 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
    df1 = pd.DataFrame(df_table1)
    df1.loc[-1] = ['All']  # adding a row
    df1.index = df1.index + 1  # shifting index
    df1.sort_index(inplace=True)
    sender_select = st.selectbox('Sender', df1['SENDER'], key='select1')

    df_table1 = test_session.sql("SELECT TYPE_TRAN from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
    df1 = pd.DataFrame(df_table1)
    df1.loc[-1] = ['All']  # adding a row
    df1.index = df1.index + 1  # shifting index
    df1.sort_index(inplace=True)
    payment_type_select = st.selectbox('Payment Type', df1['TYPE_TRAN'], key='select2')

    df_table1 = test_session.sql("SELECT BANK from D_FIN_STG.FIN_STG.TRANS_DTA where BANK is not null group by 1 ").collect()
    df1 = pd.DataFrame(df_table1)
    df1.loc[-1] = ['All']  # adding a row
    df1.index = df1.index + 1  # shifting index
    df1.sort_index(inplace=True)
    bank_select = st.selectbox('Bank', df1['BANK'], key='select3')

    df_table1 = test_session.sql("SELECT RECEIVER from D_FIN_STG.FIN_STG.TRANS_DTA where BANK is not null group by 1 ").collect()
    df1 = pd.DataFrame(df_table1)
    df1.loc[-1] = ['All']  # adding a row
    df1.index = df1.index + 1  # shifting index
    df1.sort_index(inplace=True)
    receiver_select = st.selectbox('Receiver', df1['RECEIVER'], key='select4')


with st.container():
    st.subheader("Distribution by Banks")
    df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT,count(TYPE_TRAN) as TXN_COUNT from D_FIN_STG.FIN_STG.TRANS_DTA "
                "WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(
                    to_dt) + "' and (TYPE_TRAN= '" + payment_type_select + "' or '"+ payment_type_select +"' ='All') and (SENDER= '" + sender_select + "' or '"+ sender_select +"' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') group by 1 order by 1").collect()
    df = pd.DataFrame(df_table3)
    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(x='BANK', y='TOTAL_AMOUNT', tooltip=['BANK', 'TOTAL_AMOUNT','TXN_COUNT'],color=alt.value('#3498db')).properties(height=450, width=725)
        st.altair_chart(d)


with st.container():

   st.columns(2,gap="medium")
   col1, col2= st.columns(2,gap="medium")


   with col1:
        st.subheader("Payment Type vs Transaction Amount")
        df_table3 = test_session.sql(
            "SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION,COUNT(TYPE_TRAN) AS TXN_COUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "' group by 1 ").collect()
        df3 = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            # d = alt.Chart(df3).mark_bar().encode(x='TYPE_TRAN:O',y="AMOUNT_IN_MILLION:Q", tooltip=['AMOUNT_IN_MILLION'], color=alt.value('#3498db')).properties(height=450, width=725)
            # st.altair_chart(d)
            d = alt.Chart(df3).mark_arc(innerRadius=50).encode(theta=alt.Theta(field="AMOUNT_IN_MILLION", type="quantitative"),
                                                      color=alt.Color(field="TYPE_TRAN", type="nominal"), tooltip=['TYPE_TRAN','AMOUNT_IN_MILLION','TXN_COUNT'])
            st.altair_chart(d)

   with col2:
        st.subheader("Transactions Count")
        df_table3 = test_session.sql(
            "SELECT  COUNT(TYPE_TRAN) AS TXN_COUNT, DAY(TRANS_DATE::DATE) AS TRANS_DATE,DAY(TRANS_DATE::DATE)||'/'||MONTH(TRANS_DATE::DATE) AS TRANS_DAT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "' group by 2,3 ").collect()
        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            # d= alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q',y="RECEIVER:O",tooltip=['RECEIVER','TOTAL_AMOUNT'],color=alt.value('#3498db')).properties(height=450,width=725)
            # st.altair_chart(d)
            # d = alt.Chart(df).mark_arc(innerRadius=50).encode(theta=alt.Theta(field="TOTAL_AMOUNT", type="quantitative"),
            #                                               color=alt.Color(field="RECEIVER", type="nominal"), )
            # st.altair_chart(d)
            d = alt.Chart(df).mark_rect().encode(
                alt.X('TRANS_DATE:Q', bin=alt.Bin(maxbins=30)),
                alt.Y('TXN_COUNT:Q', bin=alt.Bin(maxbins=30)),
                alt.Color('TXN_COUNT:Q', scale=alt.Scale(scheme='greenblue')),tooltip=['TRANS_DAT','TXN_COUNT']
                    )
            st.altair_chart(d)

with st.container():
    st.columns(3,gap="small")
    col1,col2,col3 = st.columns(3,gap="small")

    with col1:
        st.subheader("Top 5 Senders")
        df_table3 = test_session.sql(
            "select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT) as TOTAL_AMOUNT  from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()

        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q',y="SENDER:O", tooltip=['SENDER', 'TOTAL_AMOUNT'], color=alt.value('#3498db')).properties(height=300, width=400)
            st.altair_chart(d)
    with col2:
        st.subheader("Top 5 Receivers")
        df_table3 = test_session.sql(
            "select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q', y="RECEIVER:O",
                                                tooltip=['RECEIVER', 'TOTAL_AMOUNT'],
                                                color=alt.value('#3498db')).properties(height=300, width=400)
            st.altair_chart(d)
            # d = alt.Chart(df).mark_arc(innerRadius=50).encode(theta=alt.Theta(field="TOTAL_AMOUNT", type="quantitative"),
            #                                               color=alt.Color(field="RECEIVER", type="nominal"), )
            # st.altair_chart(d)
    with col3:
        st.subheader("Top 5 Banks")
        df_table3 = test_session.sql(
            "select BANK,TYPE_TRAN,COUNT(TYPE_TRAN) AS TXN_COUNT,SUM(AMOUNT) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA  where BANK in(Select BANK from(select Top 5 BANK,TYPE_TRAN,TOTAL_AMOUNT from(select BANK,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc)group by 1)group by 1,2").collect()
        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            # d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q', y="BANK:O",
            #                                     tooltip=['BANK', 'TOTAL_AMOUNT'],
            #                                     color=alt.value('#3498db')).properties(height=200, width=350)
            d = alt.Chart(df).mark_bar().encode(
                x='BANK',
                y='TXN_COUNT',
                color='TYPE_TRAN',tooltip=['BANK', 'TOTAL_AMOUNT','TYPE_TRAN','TXN_COUNT'],
            ).properties(height=400, width=400)
            st.altair_chart(d)

    # with col2:
    #     st.altair_chart(d)
    #     #st.bar_chart(data=df, x="BANK", y="TOTAL_AMOUNT", height=450, use_container_width=True)
