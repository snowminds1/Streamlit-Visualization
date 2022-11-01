import streamlit as st
from snowflake.snowpark import Session
import pandas as pd
import datetime
import altair as alt
import locale
from numerize import numerize

st.set_page_config(layout="wide")

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
bank_select = ''
receiver_select =''
from_dt = datetime.date.today()
to_dt = datetime.date.today()

with st.container():
       st.title('Money in Motion Dashboard - Pattern Analysis')

test_session = Session.builder.configs(connection_parameters).create()
#df_table = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
#df = pd.DataFrame(df_table)

# "with" notation
with st.sidebar:
    st.write("Custom Filters")
    today = datetime.date.today()
    year = today.year
    from_dt = st.date_input(
        "From Date",
       # datetime.date.today()-datetime.timedelta(days=90))
        datetime.date(year,1,1))

    to_dt = st.date_input(
        "To Date",
        datetime.date.today())

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
    # create three columns
    kpi1, kpi2, kpi3 = st.columns(3)

    # fill in those three columns with respective metrics or KPIs
    df_metric1 = test_session.sql("SELECT COUNT(DISTINCT BANK) AS BANK_CNT from D_FIN_STG.FIN_STG.TRANS_DTA where BANK is not null and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "'").collect()
    df1 = pd.DataFrame(df_metric1)
    kpi1.metric(
        label="No.Of.Banks",
        value=df1['BANK_CNT'].astype('int'),
    )

    df_metric1 = test_session.sql("SELECT COUNT(TYPE_TRAN) AS TXN_COUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT is not null and BANK is not null and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "'").collect()
    df1 = pd.DataFrame(df_metric1,index=None)
    res = df1['TXN_COUNT'].loc[df1.index[0]]
    kpi2.metric(
        label="No.of.Transactions",
        value=numerize.numerize(float(res)),
    )

    df_metric1 = test_session.sql("SELECT SUM(AMOUNT) AS TXN_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT is not null and BANK is not null and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "'").collect()
    df1 = pd.DataFrame(df_metric1,index=None)
    res = df1['TXN_AMOUNT'].loc[df1.index[0]]
    currency = str(u'\u20B9')

    kpi3.metric(
        label="Transaction Amount",
        value=currency + " "+numerize.numerize(float(res))
    )


with st.container():

   st.columns(2,gap="medium")
   col1, col2= st.columns(2,gap="medium")


   with col1:
        st.subheader("Payment Type vs Transaction Amount")
        df_table3 = test_session.sql(
            "SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT,SUM(AMOUNT) AS AMOUNT_IN_WORDS,COUNT(TYPE_TRAN) AS TXN_COUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and   TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(to_dt) + "' group by 1 ").collect()
        df3 = pd.DataFrame(df_table3)

        for ind in df3.index:
            df3['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df3['AMOUNT_IN_WORDS'][ind]))

        if df3.empty:
            st.write("No Data Found")
        else:
            d = alt.Chart(df3).mark_arc(innerRadius=50).encode(theta=alt.Theta(field="AMOUNT", type="quantitative"),
                                                      color=alt.Color(field="TYPE_TRAN", type="nominal"), tooltip=['TYPE_TRAN','AMOUNT_IN_WORDS','TXN_COUNT'])
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
            d = alt.Chart(df).mark_rect().encode(
                alt.X('TRANS_DATE:Q', bin=alt.Bin(maxbins=30)),
                alt.Y('TXN_COUNT:Q', bin=alt.Bin(maxbins=30)),
                alt.Color('TXN_COUNT:Q', scale=alt.Scale(scheme='greenblue')),tooltip=['TRANS_DAT','TXN_COUNT']
                    )
            st.altair_chart(d)


with st.container():
    st.subheader("Top 5 Senders")
    df_table3 = test_session.sql(
            "select Top 5 SENDER,TOTAL_AMOUNT,TOTAL_AMOUNT AS AMOUNT_IN_WORDS from(select SENDER,TYPE_TRAN,SUM(AMOUNT) as TOTAL_AMOUNT  from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()

    df = pd.DataFrame(df_table3)

    for ind in df.index:
        df['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df['AMOUNT_IN_WORDS'][ind]))


    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q',y="SENDER:O", tooltip=['SENDER', 'AMOUNT_IN_WORDS'], color=alt.value('#3498db')).properties(height=300, width=1100)
        st.altair_chart(d)

with st.container():
    st.subheader("Top 5 Receivers")
    df_table3 = test_session.sql(
            "select Top 5 RECEIVER,TOTAL_AMOUNT, TOTAL_AMOUNT AS AMOUNT_IN_WORDS from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
    df = pd.DataFrame(df_table3)

    for ind in df.index:
        df['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df['AMOUNT_IN_WORDS'][ind]))

    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q', y="RECEIVER:O",
                                                tooltip=['RECEIVER', 'AMOUNT_IN_WORDS'],
                                                color=alt.value('#3498db')).properties(height=300, width=1100)
        st.altair_chart(d)

with st.container():
    st.subheader("Top 5 Banks")
    df_table3 = test_session.sql(
            "select BANK,TYPE_TRAN,COUNT(TYPE_TRAN) AS TXN_COUNT,SUM(AMOUNT) as TOTAL_AMOUNT,SUM(AMOUNT) as AMOUNT_IN_WORDS from D_FIN_STG.FIN_STG.TRANS_DTA  where BANK in(Select BANK from(select Top 5 BANK,TYPE_TRAN,TOTAL_AMOUNT from(select BANK,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  TRANS_DATE::DATE between '" + str(
                from_dt) + "' and '" + str(
                to_dt) + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc)group by 1)group by 1,2").collect()
    df = pd.DataFrame(df_table3)


    for ind in df.index:
        df['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df['AMOUNT_IN_WORDS'][ind]))

    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(
                x='BANK',
                y='TXN_COUNT',
                color='TYPE_TRAN',tooltip=['BANK', 'AMOUNT_IN_WORDS','TYPE_TRAN','TXN_COUNT'],
            ).properties(height=400, width=1100)
        st.altair_chart(d)

with st.container():
    st.subheader("YoY Comparison")

    df_table3 = test_session.sql("select BANK,YEAR(TRANS_DATE::DATE) AS YEAR_VAL,sum(amount) AS TOTAL_AMOUNT,sum(amount) AS AMOUNT_IN_WORDS from D_FIN_STG.FIN_STG.TRANS_DTA where YEAR(TRANS_DATE::DATE)= YEAR('"+str(from_dt)+"'::DATE)  and BANK in(Select BANK from(select Top 5 BANK,TYPE_TRAN,TOTAL_AMOUNT from(select BANK,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and  YEAR(TRANS_DATE::DATE)= YEAR('"+str(from_dt)+"'::DATE) group by 1,2 order by Total_Amount desc)order by Total_Amount desc)group by 1) group by 1,2 UNION select BANK,YEAR(TRANS_DATE::DATE),sum(amount)  AS TOTAL_AMOUNT, sum(amount) AS AMOUNT_IN_WORDS from D_FIN_STG.FIN_STG.TRANS_DTA where YEAR(TRANS_DATE::DATE)= YEAR('"+str(from_dt)+"'::DATE)-1  and BANK in(Select BANK from(select Top 5 BANK,TYPE_TRAN,TOTAL_AMOUNT from(select BANK,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where (TYPE_TRAN= '" + payment_type_select + "' or '" + payment_type_select + "' ='All') and (SENDER= '" + sender_select + "' or '" + sender_select + "' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') and YEAR(TRANS_DATE::DATE)= YEAR('"+str(from_dt)+"'::DATE) group by 1,2 order by Total_Amount desc)order by Total_Amount desc)group by 1)group by 1,2").collect()
    df = pd.DataFrame(df_table3)

    for ind in df.index:
        df['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df['AMOUNT_IN_WORDS'][ind]))

    if df.empty:
        st.write("No Data Found")
    else:
        d=alt.Chart(df).mark_bar().encode(
        x='YEAR_VAL:O',
        y='TOTAL_AMOUNT:Q',
        color='YEAR_VAL:N',
        column='BANK:N', tooltip=['YEAR_VAL', 'BANK','AMOUNT_IN_WORDS']
        ).properties(height=450, width=200)
        st.altair_chart(d)

with st.container():
    st.subheader("Distribution by Banks")
    df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT,sum(amount) as AMOUNT_IN_WORDS,count(TYPE_TRAN) as TXN_COUNT from D_FIN_STG.FIN_STG.TRANS_DTA "
                "WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(
                    to_dt) + "' and (TYPE_TRAN= '" + payment_type_select + "' or '"+ payment_type_select +"' ='All') and (SENDER= '" + sender_select + "' or '"+ sender_select +"' ='All') and (RECEIVER= '" + receiver_select + "' or '"+ receiver_select +"' ='All') and (BANK= '" + bank_select + "' or '"+ bank_select +"' ='All') group by 1 order by 1").collect()
    df = pd.DataFrame(df_table3)

    for ind in df.index:
        df['AMOUNT_IN_WORDS'][ind] = numerize.numerize(float(df['AMOUNT_IN_WORDS'][ind]))


    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(x='BANK', y='TOTAL_AMOUNT', tooltip=['BANK', 'AMOUNT_IN_WORDS','TXN_COUNT'],color=alt.value('#3498db')).properties(height=450, width=1100)
        st.altair_chart(d)
