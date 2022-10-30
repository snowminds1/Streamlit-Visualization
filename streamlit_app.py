import streamlit as st
import pandas as pd
from snowflake.snowpark import Session
import datetime
import altair as alt

st.set_page_config(layout="wide")
st.title('Money in Motion Dashboard')
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
    from_dt = st.date_input(
        "From Date",
        datetime.date.today())
    st.write('From Date:', from_dt)

    to_dt = st.date_input(
        "To Date",
        datetime.date.today())
    st.write('To Date :', to_dt)

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

with st.container():
    st.subheader("Distribution by Banks")
    if sender_select == 'All' and payment_type_select == 'All':
        if from_dt == datetime.date.today() and to_dt == datetime.date.today():
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 order by 1").collect()
        else:
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(to_dt) + "' group by 1 order by 1").collect()
    elif sender_select != 'All' and payment_type_select == 'All':
        if from_dt == datetime.date.today() and to_dt == datetime.date.today():
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where sender= '" + sender_select + "' group by 1 order by 1").collect()
        else:
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(
                    to_dt) + "' and sender= '" + sender_select + "' group by 1 order by 1").collect()
    elif sender_select == 'All' and payment_type_select != 'All':
        if from_dt == datetime.date.today() and to_dt == datetime.date.today():
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA  where TYPE_TRAN= '" + payment_type_select + "' group by 1 order by 1").collect()
        else:
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(
                    to_dt) + "' and TYPE_TRAN= '" + payment_type_select + "' group by 1 order by 1").collect()
    elif sender_select != 'All' and payment_type_select != 'All':
        if from_dt == datetime.date.today() and to_dt == datetime.date.today():
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA  where TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' group by 1 order by 1").collect()
        else:
            df_table3 = test_session.sql(
                "select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" + str(
                    from_dt) + "' and '" + str(
                    to_dt) + "' and TYPE_TRAN= '" + payment_type_select + "'  and sender= '" + sender_select + "' group by 1 order by 1").collect()

    df = pd.DataFrame(df_table3)
    if df.empty:
        st.write("No Data Found")
    else:
        d = alt.Chart(df).mark_bar().encode(x='BANK', y='TOTAL_AMOUNT', tooltip=['BANK', 'TOTAL_AMOUNT'],
                                            color=alt.value('#3498db')).properties(height=400, width=1000)
        st.altair_chart(d)

with st.container():
   st.columns(4)
   col1,col2,col3,col4 = st.columns(4)

   # with col1:
   #     from_dt = st.date_input(
   #         "From Date",
   #         datetime.date.today())
   #     st.write('From Date:', from_dt)

   # with col2:
   #     to_dt= st.date_input(
   #         "To Date",
   #         datetime.date.today())
   #     st.write('To Date :', to_dt)

   # with col3:
   #     df_table1 = test_session.sql("SELECT SENDER from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
   #     df1 = pd.DataFrame(df_table1)
   #     df1.loc[0] = ['All']
   #     sender_select = st.selectbox('Sender', df1['SENDER'],key='select1')
   #     #st.write(sender_select)


   # with col4:
   #     df_table1 = test_session.sql("SELECT TYPE_TRAN from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
   #     df1 = pd.DataFrame(df_table1)
   #     df1.loc[0] = ['All']
   #     payment_type_select = st.selectbox('Payment Type', df1['TYPE_TRAN'],key='select2')
       #st.write(payment_type_select)

with st.container():

   st.columns(2,gap="medium")
   col1, col2= st.columns(2,gap="medium")

   with col1:
        st.subheader("Payment Type vs Transaction Amount")
        if sender_select=='All' and payment_type_select=='All':
            if from_dt==datetime.date.today() and to_dt==datetime.date.today():
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 ").collect()
            else:
                df_table3 = test_session.sql(
                    "SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1 ").collect()
        elif sender_select!='All' and payment_type_select=='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where sender= '" + sender_select + "' group by 1 ").collect()
            else:
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1 ").collect()
        elif sender_select=='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where TYPE_TRAN= '" + payment_type_select + "' group by 1 ").collect()
            else:
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where TYPE_TRAN= '" + payment_type_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1 ").collect()
        elif sender_select!='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "'  group by 1 ").collect()
            else:
                df_table3 = test_session.sql("SELECT  TYPE_TRAN,SUM(AMOUNT) AS AMOUNT_IN_MILLION from D_FIN_STG.FIN_STG.TRANS_DTA where TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1 ").collect()

        print(df_table3)
        df3 = pd.DataFrame(df_table3)
        if df3.empty:
            st.write("No Data Found")
        else:
            d = alt.Chart(df3).mark_bar().encode(
                x='TYPE_TRAN:O',
                y="AMOUNT_IN_MILLION:Q",tooltip=['AMOUNT_IN_MILLION'],color=alt.value('#3498db')
            ).properties(height=300, width=500)
            st.altair_chart(d)
            #st.bar_chart(data=df3, x="TYPE_TRAN", y="AMOUNT_IN_MILLION", width=500, height=0, use_container_width=True)

   with col2:
        st.subheader("Top 5 Receivers")
        if sender_select == 'All' and payment_type_select == 'All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select!='All' and payment_type_select=='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and sender= '" + sender_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select=='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and  TYPE_TRAN= '" + payment_type_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select!='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 RECEIVER,TOTAL_AMOUNT from(select RECEIVER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()

        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            d= alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q',y="RECEIVER:O",tooltip=['RECEIVER','TOTAL_AMOUNT'],color=alt.value('#3498db')).properties(height=300,width=500)
            st.altair_chart(d)

with st.container():
    st.columns(2,gap="medium")
    col1, col2 = st.columns(2,gap="medium")

    with col1:
        st.subheader("Top 5 Senders")
        if sender_select == 'All' and payment_type_select == 'All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select!='All' and payment_type_select=='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and sender= '" + sender_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select=='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and  TYPE_TRAN= '" + payment_type_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
        elif sender_select!='All' and payment_type_select!='All':
            if from_dt == datetime.date.today() and to_dt == datetime.date.today():
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()
            else:
                df_table3 = test_session.sql("select Top 5 SENDER,TOTAL_AMOUNT from(select SENDER,TYPE_TRAN,SUM(AMOUNT)  as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where AMOUNT IS NOT NULL and TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' and  TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1,2 order by Total_Amount desc)order by Total_Amount desc").collect()

        df = pd.DataFrame(df_table3)
        if df.empty:
            st.write("No Data Found")
        else:
            d = alt.Chart(df).mark_bar().encode(x='TOTAL_AMOUNT:Q',y="SENDER:O", tooltip=['SENDER', 'TOTAL_AMOUNT'], color=alt.value('#3498db')).properties(height=500, width=500)
            st.altair_chart(d)

    # with col2:
    #     st.subheader("Distribution by Banks")
    #     if sender_select == 'All' and payment_type_select == 'All':
    #         if from_dt == datetime.date.today() and to_dt == datetime.date.today():
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA group by 1 order by 1").collect()
    #         else:
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' group by 1 order by 1").collect()
    #     elif sender_select!='All' and payment_type_select=='All':
    #         if from_dt == datetime.date.today() and to_dt == datetime.date.today():
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA where sender= '" + sender_select + "' group by 1 order by 1").collect()
    #         else:
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' and sender= '" + sender_select + "' group by 1 order by 1").collect()
    #     elif sender_select=='All' and payment_type_select!='All':
    #         if from_dt == datetime.date.today() and to_dt == datetime.date.today():
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA  where TYPE_TRAN= '" + payment_type_select + "' group by 1 order by 1").collect()
    #         else:
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' and TYPE_TRAN= '" + payment_type_select + "' group by 1 order by 1").collect()
    #     elif sender_select!='All' and payment_type_select!='All':
    #         if from_dt == datetime.date.today() and to_dt == datetime.date.today():
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA  where TYPE_TRAN= '" + payment_type_select + "' and sender= '" + sender_select + "' group by 1 order by 1").collect()
    #         else:
    #             df_table3 = test_session.sql("select CASE WHEN bank IS NULL THEN 'OTHERS' ELSE BANK END AS BANK,sum(amount) as TOTAL_AMOUNT from D_FIN_STG.FIN_STG.TRANS_DTA WHERE TRANS_DATE::DATE between '" +str(from_dt)+ "' and '"+str(to_dt)+ "' and TYPE_TRAN= '" + payment_type_select + "'  and sender= '" + sender_select + "' group by 1 order by 1").collect()
    #
    #     df = pd.DataFrame(df_table3)
    #     if df.empty:
    #         st.write("No Data Found")
    #     else:
    #         d = alt.Chart(df).mark_bar().encode(x='BANK',y='TOTAL_AMOUNT',tooltip=['BANK', 'TOTAL_AMOUNT'], color=alt.value('#3498db')).properties(height=400, width=600)
    #         st.altair_chart(d)
        #st.bar_chart(data=df, x="BANK", y="TOTAL_AMOUNT", height=450, use_container_width=True)
