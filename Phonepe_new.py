import streamlit as st
import json
import pandas as pd
import mysql.connector
import requests
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image
from streamlit_option_menu import option_menu
from PIL import Image
from git.repo.base import Repo
import plotly.express as px

#  CREATE DATAFRAMES FROM SQL 
mydb = mysql.connector.connect( host="localhost", user="root", password="", database="phonepe") # MySQL DB Connection
print(mydb)
mycursor = mydb.cursor()
# Setting up page configuration
icon = Image.open(r"Phonepe_Logo.jpg")
st.set_page_config(
                page_title="PhonePe Data Visualization and Exploration",
                page_icon=icon,
                layout="wide")
st.markdown("""<style>
                body {
                    font-family: serif;
                }
                </style>""",unsafe_allow_html=True )

# Display the PhonePe logo in full size
st.image("Phonepe_Logo.jpg", width=300)

# Display a title
st.title(':rainbow[PhonePe Data Visualization and Exploration]')
st.markdown("""
    <div style='text-align: center;'>
        <h4>A User-Friendly Tool Using Streamlit and Plotly</h4>
    </div>
    """, unsafe_allow_html=True)

selected = option_menu("Phonepe Pulse | The Beat Of Progress", ["Home","Top Charts","Explore Data","About"],
                icons=["house","bar-chart-line","graph-up-arrow", "exclamation-circle"],
                menu_icon="globe",
                default_index=0,
                orientation='horizontal',
                styles={"nav-link": {"font-size": "20px", "text-align": "left", "margin": "-2px", "--hover-color": "#6F36AD"},
                        "nav-link-selected": {"background-color": "#6F36AD"}})

if selected == "Home":
    col1, col2 = st.columns([2,2])
    with col1:
        st.markdown("""
            ### :violet[Project Description]: ###
            This project aims to extract, process, and visualize data from the PhonePe Pulse Github repository.The goal is to create a user-friendly dashboard using Streamlit and Plotly to display insights and information from the data.""", unsafe_allow_html=True)
        st.markdown("### :violet[Domain]: ###\n*Fintech*")
        st.markdown("""
                    ### :violet[Technologies used]: ###
                    - **Github Cloning**
                    - **Python**
                    - **Pandas**
                    - **MySQL**
                    - **mysql-connector-python**
                    - **Streamlit**
                    - **Plotly**
                    """, unsafe_allow_html=True)
        st.markdown("### :violet[Problem Statement]: ###\nThe PhonePe Pulse Github repository contains a large amount of data related to various metrics and statistics. The goal is to extract this data and process it to obtain insights and information that can be visualized in a user-friendly manner.")
    # Display image
    with col2:
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.image("phonepe.webp", use_column_width=True)
#--------------------------------------------
# TOP CHARTS - 
if selected == "Top Charts":
    st.markdown("## :rainbow[Top Charts]")
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    colum1, colum2 = st.columns([1,3], gap="large")

    Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2024)
    Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
    if Year == 2024 and Quarter in [2, 3, 4]:
                st.error("#### No Data available to Display for 2024 Qtr 2,3,4")
    with colum1:
        st.write("")
        st.write("")
    with colum2:
        st.info(
            """
            #### From this menu we can get insights like:
            - 📅 Ranking by Year and Quarter.
            - 📊 Top 10 States, Districts, and Pincodes by transactions and amount.
            - 📈 Top 10 States, Districts, and Pincodes by users and app openings.
            - 📱 Top 10 mobile brands and their usage percentage.
            - 🗺️ States and Districts with the least 10 app users.
            """, icon="🔍")
        st.write("")
        st.write("")
#--------------------------------------------
    # Top Charts - Transactions   
    if Type == "Transactions":
        col1, col2 = st.columns([2, 2], gap="large")
        col3 = st.columns(1)[0]
        col4, col5= st.columns([2, 2], gap="large")

        with col1:
            st.markdown("### :green[1. Top 10 States by Total number of transactions]")
            mycursor.execute(f'''select State, sum(Transaction_count) as Total_Transactions_Count
                            from aggregate_transaction where Year = {Year} and Quarter= {Quarter} group by State 
                            order by Total_Transactions_Count desc limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Transactions_Count'])
            fig = px.bar(df,
                            title='Top 10 States -Transactions count',
                            x='Total_Transactions_Count',
                            y='State',
                            orientation='h',
                            color='Total_Transactions_Count',
                            color_continuous_scale=px.colors.sequential.Viridis)
            fig.update_traces(text=df['Total_Transactions_Count'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### :green[2. Top 10 States by Total transaction amount]")
            mycursor.execute(f'''select State, sum(Transaction_amount) as Total_Transaction_Amount
                            from aggregate_transaction where Year = {Year} and Quarter= {Quarter} group by State 
                            order by Total_Transaction_Amount desc limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Transaction_Amount'])
            fig = px.bar(df,
                            title='Top 10 States - Total transaction amount',
                            x='Total_Transaction_Amount',
                            y='State',
                            orientation='h',
                            color='Total_Transaction_Amount',
                            color_continuous_scale=px.colors.sequential.Viridis)
            fig.update_traces(text=df['Total_Transaction_Amount'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown("### :green[3. Top 10 Districts by Total number of transactions]")
            mycursor.execute(f'''select Transaction_district as District, State, sum(Transaction_count) as Total_Count
                                from map_transaction
                                where Year = {Year} and Quarter = {Quarter}
                                group by District
                                order by Total_Count desc
                                limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['District', 'State', 'Total_Count'])
            fig = px.pie(df,
                        title='Top 10 Districts - Transactions Count',
                        values='Total_Count',
                        names='District',
                        color_discrete_sequence=px.colors.sequential.Agsunset,
                        hover_data=['State'],
                        labels={'Total_Count': 'Transaction_count'})
            fig.update_traces(text=df['Total_Count'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col4:
            st.markdown("### :green[4. Top 10 Districts by Total transaction amount]")
            mycursor.execute(f'''select Transaction_district as District,State, sum(Transaction_amount) as Total_Amount
                            from map_transaction where Year = {Year} and Quarter = {Quarter} group by District 
                            order by Total_Amount desc limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['District','State', 'Total_Amount'])
            fig = px.bar(df,
                            title='Top 10 Districts - Total transaction amount',
                            x='Total_Amount',
                            y='District',
                            orientation='h',
                            color='Total_Amount',
                            hover_data=['State'],
                            color_continuous_scale=px.colors.sequential.Viridis)
            fig.update_traces(text=df['Total_Amount'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col5:
            st.markdown("### :green[5. Top 10 Pincodes by Total transaction amount]")
            mycursor.execute(f'''SELECT Pincode, State, SUM(Transaction_count) AS Total_Transactions_Count, SUM(Transaction_amount) AS Total_Amount
                                FROM top_transaction
                                WHERE Year = {Year} AND Quarter = {Quarter}
                                GROUP BY Pincode
                                ORDER BY Total_Amount DESC
                                LIMIT 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'State', 'Total_Transactions_Count', 'Total_Amount'])
            df['Pincode'] = df['Pincode'].astype(str)  # Convert Pincode to string
            fig = px.bar(df,
                        title='Top 10 Pincodes - Total transaction amount',
                        x='Pincode',
                        y='Total_Amount', 
                        labels={'Pincode': 'Pincode', 'Total_Amount': 'Total Amount'},
                        hover_data=['State', 'Pincode'])
            fig.update_xaxes(type='category', tickmode='linear')
            st.plotly_chart(fig, use_container_width=True)

#------------------------------------------------
    # Top Charts - USERS          
    if Type == "Users":
        col1, col2 = st.columns([2, 2], gap="large")
        col3 = st.columns(1)[0]
        col4, col5= st.columns([2, 2], gap="large")

        with col1:
                st.markdown("### :green[6. Top 10 mobile phone brands used by PhonePe users? ]")
                mycursor.execute(f'''SELECT Brand_Name, SUM(Count) AS total_user_count, AVG(Percentage) * 100 AS avg_user_percentage 
                                    FROM aggregate_user WHERE Year = {Year} AND Quarter = {Quarter} GROUP BY Brand_Name ORDER BY total_user_count DESC LIMIT 10''')
                df = pd.DataFrame(mycursor.fetchall(), columns=['Brand_Name', 'total_user_count', 'avg_user_percentage'])
                fig = px.bar(
                    df,
                    title='Top 10 User Brands',
                    x="total_user_count",
                    y="Brand_Name",
                    orientation='h',
                    color='avg_user_percentage',
                    color_discrete_sequence=px.colors.sequential.Viridis)
                fig.update_layout(xaxis_title='Total User Count', yaxis_title='User Brand')
                st.plotly_chart(fig, use_container_width=True)

        with col2:
            st.markdown("### :green[7.Top 10 states by the total number of registered users]")
            mycursor.execute(f'''select State, sum(Registered_user) as Total_Users, sum(App_opening) as Total_Appopens from 
                            map_users where Year = {Year} and Quarter= {Quarter} group by State order by Total_Users desc limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Registered_user', 'App_opening'])
            fig = px.pie(df, values='Registered_user',
                            names='State',
                            title='Top 10 states by registered users',
                            color_discrete_sequence=px.colors.sequential.Agsunset,
                            hover_data=['App_opening'],
                            labels={'App_opening': 'App_opening'})
            fig.update_traces(text=df['Registered_user'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col3:
            st.markdown("### :green[8.Top 10 districts by the total number of registered users]")
            mycursor.execute(f'''select District,State, sum(Registered_user) as Total_Users, sum(App_opening) as Total_Appopens from
                            map_users where Year = {Year} and Quarter= {Quarter} group by District,Registered_user order by Total_Users desc limit 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['District','State','Registered_user', 'App_opening'])
            df.Registered_user = df.Registered_user.astype(float)
            fig = px.bar(df,
                        title='Top 10 districts by registered users',
                        x="Registered_user",
                        y="District",
                        orientation='h',
                        color='Registered_user',
                        hover_data=['State'],
                        color_continuous_scale=px.colors.sequential.Agsunset)
            fig.update_traces(text=df['Registered_user'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col4:
            st.markdown("### :green[9. Top 10 Pincodes by the total number of registered users]")
            mycursor.execute(f'''SELECT Pincode, State, SUM(registeredusers) AS Total_Users
                                FROM top_users
                                WHERE Year = {Year} AND Quarter = {Quarter}
                                GROUP BY Pincode
                                ORDER BY Total_Users DESC
                                LIMIT 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['Pincode', 'State', 'registeredusers'])
            fig = px.pie(df,
                        values='registeredusers',
                        names='Pincode',
                        title='Top 10 Pincodes by registered users',
                        color_discrete_sequence=px.colors.sequential.Viridis,
                        hover_data=['State'])
            fig.update_traces(text=df['registeredusers'], textposition='inside')
            st.plotly_chart(fig, use_container_width=True)

        with col5:
            st.markdown("### :green[10. State and District with the least 10 number of registered users]")
            mycursor.execute(f'''SELECT State, District, SUM(Registered_user) AS Total_Users
                                FROM map_users
                                WHERE Year = {Year} AND Quarter = {Quarter}
                                GROUP BY State, District
                                ORDER BY Total_Users ASC
                                LIMIT 10''')
            df = pd.DataFrame(mycursor.fetchall(), columns=['State', 'District', 'Total_Users'])
            df['Total_Users'] = df['Total_Users'].astype(float)  # Convert Total_Users to float
            fig = px.bar(df,
                        title='State and District with the least number of registered users',
                        x='Total_Users',
                        y='District',
                        orientation='h',
                        color='Total_Users',
                        color_continuous_scale=px.colors.sequential.Agsunset,
                        hover_data=['State', 'District', 'Total_Users'])
            st.plotly_chart(fig, use_container_width=True)

#--------------------------------------------
# EXPLORE DATA
if selected == "Explore Data":
    Type = st.sidebar.selectbox("**Type**", ("Transactions", "Users"))
    colum1, colum2 = st.columns([1, 1.5], gap="large")
    
    Year = st.sidebar.slider("**Year**", min_value=2018, max_value=2024)
    Quarter = st.sidebar.slider("Quarter", min_value=1, max_value=4)
    if Year == 2024 and Quarter in [2, 3, 4]:
                st.error("#### No Data available to Display for 2024 Qtr 2,3,4")
    col1, col2 = st.columns(2)

    # EXPLORE DATA - Transactions   
    if Type == "Transactions":     
        col1 = st.columns(1)[0]
        col2 = st.columns(1)[0]
        # Overall State Data - TRANSACTIONS AMOUNT - INDIA MAP 
        with col1:
            st.markdown("## :violet[Overall State Data - Transactions Amount]")
            mycursor.execute(f'''SELECT State, SUM(Transaction_count) AS Total_Transactions_count, SUM(Transaction_amount) AS Total_amount FROM 
                                map_transaction WHERE Year = {Year} AND Quarter = {Quarter} GROUP BY State ORDER BY State''')
            df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Transactions_count', 'Total_amount'])
            df2 = pd.read_csv('state_names.csv')
            if 'State' in df2.columns:
                df1 = df1.merge(df2[['State']], on='State', how='left')    

            fig = px.choropleth(df1, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                        featureidkey='properties.ST_NM',
                        locations='State',
                        color='Total_amount',
                        color_continuous_scale='sunset')
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(geo=dict(scope='asia'), height=600, width=800)
            st.plotly_chart(fig, use_container_width=True)
            
        with col2:
            st.markdown("## :violet[Overall State Data - Transactions count]")
            mycursor.execute(f'''SELECT State, SUM(Transaction_count) AS Total_Transactions_count, SUM(Transaction_amount) AS Total_amount FROM 
                                map_transaction WHERE Year = {Year} AND Quarter = {Quarter} GROUP BY State ORDER BY State''')
            df3 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Transactions_count', 'Total_amount'])
            df4 = pd.read_csv('state_names.csv')
            if 'State' in df4.columns:
                df3 = df3.merge(df4[['State']], on='State', how='left')
            
            fig2 = px.choropleth(df3, geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
                            featureidkey='properties.ST_NM',
                            locations='State',
                            color='Total_Transactions_count',
                            color_continuous_scale='sunset')
            fig2.update_geos(fitbounds="locations", visible=False)
            fig2.update_layout(geo=dict(scope='asia'), height=600, width=800)
            st.plotly_chart(fig2, use_container_width=True)
#--------------------------------------------
        # BAR CHART - TOP PAYMENT TYPE
        st.markdown("## :violet[Top Payment Type]")
        mycursor.execute(f'''SELECT Transaction_type, SUM(Transaction_count) AS Total_Transactions_count, SUM(Transaction_amount) AS 
                            Total_Transactions_amount FROM aggregate_transaction WHERE Year= {Year} AND Quarter = {Quarter} GROUP BY Transaction_type ORDER BY Transaction_type''')
        df = pd.DataFrame(mycursor.fetchall(), columns=['Transaction_type', 'Total_Transactions_count','Total_Transactions_amount'])

        fig = px.bar(df,
                    title='Transaction Types vs Total_Transactions_count',
                    x="Transaction_type",
                    y="Total_Transactions_count",
                    orientation='v',
                    color='Total_Transactions_amount',
                    color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig, use_container_width=False)
#--------------------------------------------
# BAR CHART TRANSACTIONS - DISTRICT WISE DATA            
        st.markdown("## :violet[Select any State for more details]")
        selected_state = st.selectbox("Select State", [
            'Andaman And Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh',
            'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh', 'Maharashtra',
            'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
            'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'], index=30)
        
        mycursor.execute(f'''
            SELECT State, Transaction_district, Year, Quarter, SUM(Transaction_count) AS Total_Transactions_count, SUM(Transaction_amount) AS Total_transaction_amount 
            FROM map_transaction 
            WHERE Year = {Year} AND Quarter = {Quarter} AND State = '{selected_state}' 
            GROUP BY State, Transaction_district, Year, Quarter 
            ORDER BY State, Transaction_district''')
        df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transaction_district', 'Year', 'Quarter',
                                                        'Total_Transactions_count', 'Total_transaction_amount'])
        fig = px.bar(df1,
                    title=selected_state,
                    x="Transaction_district",
                    y="Total_Transactions_count",
                    orientation='v',
                    color='Total_transaction_amount',
                    color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig, use_container_width=True)

# EXPLORE DATA - USERS
    if Type == "Users":
        # Create a Streamlit title
        st.markdown("## :violet[Overall State Data - User App Opening Frequency]")
        mycursor.execute(f'''SELECT State, SUM(Registered_user) AS Total_Users, SUM(App_opening) AS Total_Appopens FROM
                            map_users WHERE Year = {Year} AND Quarter = {Quarter} GROUP BY State ORDER BY State''')
        df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Total_Users', 'Total_Appopens'])
        df2 = pd.read_csv('state_names.csv')
        df1['Total_Appopens'] = df1['Total_Appopens'].astype(float)
        if 'State' in df2.columns:
            df1 = df1.merge(df2[['State']], on='State', how='left')

        fig = px.choropleth(
            df1,
            geojson="https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson",
            featureidkey='properties.ST_NM',
            locations='State',
            color='Total_Appopens',
            color_continuous_scale=px.colors.diverging.Picnic)
        fig.update_geos(fitbounds="locations", visible=False)
        fig.update_layout(geo=dict(scope='asia'), height=600, width=800)
        st.plotly_chart(fig, use_container_width=True)
#--------------------------------------------
        # BAR CHART TOTAL USERS - DISTRICT WISE DATA
        st.markdown("## :violet[Select any State to explore more]")
        selected_state = st.selectbox("Select State", [
            'Andaman And Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh', 'Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
            'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa', 'Gujarat', 'Haryana', 'Himachal Pradesh',
            'Jammu & Kashmir', 'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep', 'Madhya Pradesh', 'Maharashtra',
            'Manipur', 'Meghalaya', 'Mizoram', 'Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan', 'Sikkim',
            'Tamil Nadu', 'Telangana', 'Tripura', 'Uttar Pradesh', 'Uttarakhand', 'West Bengal'], index=30)

        mycursor.execute(f'''SELECT State, Transaction_district, Year, Quarter, SUM(Transaction_count) AS Total_Transactions_count, SUM(Transaction_amount) AS Total_transaction_amount
                            FROM map_transaction WHERE Year = {Year} AND Quarter = {Quarter} AND State = '{selected_state}' GROUP BY State, Transaction_district, Year, Quarter ORDER BY State, Transaction_district''')

        df1 = pd.DataFrame(mycursor.fetchall(), columns=['State', 'Transaction_district', 'Year', 'Quarter',
                                                        'Total_Transactions_count', 'Total_transaction_amount'])
        fig = px.bar(df1,
                    title=selected_state,
                    x="Transaction_district",
                    y="Total_Transactions_count",
                    orientation='v',
                    color='Total_transaction_amount',
                    color_continuous_scale=px.colors.sequential.Agsunset)
        st.plotly_chart(fig, use_container_width=True)
    
# MENU 4 - ABOUT
if selected == "About":
    col1,col2 = st.columns([1,1],gap="Medium")    
    with col1:
        st.write(" ")
        st.write(" ")
        st.markdown("# :violet[About PhonePe Pulse:]")
        st.markdown("""### \n📍 **BENGALURU, India, On Sept. 3, 2021** - PhonePe, India's leading fintech platform, unveiled PhonePe Pulse, an interactive website offering data, insights, and trends on digital payments in India.
            \nIt features an interactive map highlighting over 2000 Crore transactions. With a market share exceeding 45%, PhonePe's data reflects prevalent digital payment habits.PhonePe Pulse is the way of giving back to the digital payments ecosystem. This data has been structured to provide details on data cuts of Transactions and Users on the Explore tab.\n""")
        st.write(" ")
        st.markdown(""" 
            # :violet[About PhonePe:]
            📱 **PhonePe** is India's leading fintech platform with 300M+ registered users. Users can send and receive money, recharge mobile, DTH, pay at stores, make utility payments, buy gold, and make investments.\n
            PhonePe launched financial services in 2017, starting with Gold, and has since added Mutual Funds, Insurance products, and the Switch platform for in-app shopping on 600+ apps. Accepted at 20M+ merchant outlets.\n
            #### :orange[Key Features:]
            - **UPI Payments**: Secure instant transfers
            - **Wallet Services**: Quick payments
            - **Recharge and Bill Payments**: Mobile, DTH, utility bills
            - **In-app Shopping**: Orders on 600+ apps
            - **Investments**: Gold, Mutual Funds, Insurance\n
            #### :red[Usage Statistics:]
            - 📈 **300M+ Users**
            - 🏪 **20M+ Merchant Outlets**
            - 🛍️ **10M+ Merchants\n
            #### :blue[Security Features:]
            - 🔒 **UPI PIN**
            - 👁️ **24/7 Fraud Monitoring\n
            #### :orange[Awards:]
            - 🏆 **Best UPI App by NPCI\n
            #### :red[Additional Insights:]
            - **Comprehensive Data**: Digital payment trends
            - **User-Friendly**: Easy-to-navigate interface
            - **Market Leader**: Significant market share
            - **Innovative**: New products/services
            - **Community Impact**: Financial inclusion and literacy""") 

    with col2:
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.image("spotlight_Phonepe.jpg", use_column_width=True)
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.write(" ")
        st.image("Phonepeimg.jpg", use_column_width=True)
        st.write(" ")
        st.write(" ")
        st.write(" ")