# Importing Libraries
import streamlit as st
import os
import json
import pandas as pd
import mysql.connector
import requests
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image

#  1) AGGREGATED_TRANSACTION 
#github clone Added Already

path1="C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/aggregated/transaction/country/india/state/"
Agg_state_list=os.listdir(path1)
Agg_state_list

col1={'State':[], 'Year':[],'Quarter':[],'Transaction_type':[], 'Transaction_count':[], 'Transaction_amount':[]}

for i in Agg_state_list:
    p_i=path1+i+"/"
    Agg_yr=os.listdir(p_i)
    for j in Agg_yr:
        p_j=p_i+j+"/"
        Agg_yr_list=os.listdir(p_j)
        for k in Agg_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['transactionData']:
                Name=z['name']
                count=z['paymentInstruments'][0]['count']
                amount=z['paymentInstruments'][0]['amount']
                col1['Transaction_type'].append(Name)
                col1['Transaction_count'].append(count)
                col1['Transaction_amount'].append(amount)
                col1['State'].append(i)
                col1['Year'].append(j)
                col1['Quarter'].append(int(k.strip('.json')))
#Succesfully created a dataframe
Agg_Trans=pd.DataFrame(col1)

Agg_Trans["State"] = Agg_Trans["State"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
Agg_Trans["State"] = Agg_Trans["State"].str.replace("-"," ")
Agg_Trans["State"] = Agg_Trans["State"].str.title()
Agg_Trans["State"] = Agg_Trans["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

#  2) AGGREGATE_USER

path2= "C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/aggregated/user/country/india/state/"  #aggregate users path
agg_user_list=os.listdir(path2)
agg_user_list
col2={'State':[],'Year':[],'Quarter':[],'Brand_Name':[],'Count':[],'Percentage':[]}

for state in agg_user_list:
    actual_state = path2 + state + "/"
    agg_yr = os.listdir(actual_state)
    for year in agg_yr:
        actual_yr = actual_state + year + "/"
        agg_yr_list = os.listdir(actual_yr)
        for file in agg_yr_list:
            actual_file = actual_yr + file
            Data = open(actual_file, 'r')
            D2 = json.load(Data)
            try:
                for i in D2['data']['usersByDevice']:
                    Brand_Name=i['brand']
                    Count=i['count']
                    percentage=i['percentage']
                    col2['Brand_Name'].append(Brand_Name)
                    col2['Count'].append(Count)
                    col2['Percentage'].append(percentage)
                    col2['State'].append(state)
                    col2['Year'].append(year)
                    col2['Quarter'].append(int(file.strip('.json')))
            except:
                pass

aggre_user=pd.DataFrame(col2)

# Correcting the column name to 'State' instead of 'States'
aggre_user["State"] = aggre_user["State"].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
aggre_user["State"] = aggre_user["State"].str.replace("-"," ")
aggre_user["State"] = aggre_user["State"].str.title()
aggre_user["State"] = aggre_user["State"].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

#  3) MAP_TRANSACTION

path3 ="C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/map/transaction/hover/country/india/state/" # Map Transaction Path
map_state_list = os.listdir(path3)
map_state_list

col3 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_district': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in map_state_list:
    p_i=path3+i+"/"
    map_yr=os.listdir(p_i)
    for j in map_yr:
        p_j=p_i+j+"/"
        map_yr_list=os.listdir(p_j)
        for k in map_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['hoverDataList']:
                    name = z['name']
                    ttype = z['metric'][0]['type']
                    count = z['metric'][0]['count']
                    amount = z['metric'][0]['amount']
                    col3['Transaction_district'].append(name)
                    col3['Transaction_type'].append(ttype)
                    col3['Transaction_count'].append(count)
                    col3['Transaction_amount'].append(amount)
                    col3['State'].append(i)
                    col3['Year'].append(j)
                    col3['Quarter'].append(int(k.strip('.json')))

map_trans=pd.DataFrame(col3)

map_trans['State']=map_trans['State'].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_trans['State']=map_trans['State'].str.replace("-"," ")
map_trans['State']=map_trans['State'].str.title()
map_trans['State']=map_trans['State'].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

#  4) MAP_USER

path4="C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/map/user/hover/country/india/state/"
mapuser_state_list = os.listdir(path4)
mapuser_state_list

col4 = {'State': [], 'Year': [], 'Quater': [], 'District': [],'Registered_user': [], 'App_opening': []}

for i in mapuser_state_list:
    p_i = path4+i+"/"
    year = os.listdir(p_i)
    for j in year:
        p_j = p_i+j+"/"
        file = os.listdir(p_j)
        for k in file:
            p_k = p_j+k
            Data = open(p_k, 'r')
            D = json.load(Data)
            try:
                for z in D['data']["hoverData"]:
                    district = z
                    registered_user =  D['data']["hoverData"][z]["registeredUsers"]
                    app_opening = D['data']["hoverData"][z]["appOpens"]
                    col4['District'].append(district)
                    col4['Registered_user'].append(registered_user)
                    col4['App_opening'].append(app_opening)
                    col4['State'].append(i)
                    col4['Year'].append(j)
                    col4['Quater'].append(int(k.strip('.json')))

            except:
                pass

map_users = pd.DataFrame(col4)

map_users['State']=map_users['State'].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
map_users['State']=map_users['State'].str.replace("-"," ")
map_users['State']=map_users['State'].str.title()
map_users['State']=map_users['State'].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

# 5) TOP_TRANSACTION 
path5="C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/top/transaction/country/india/state/"
top_trans_state_list = os.listdir(path5)
top_trans_state_list

col5 = {'State': [], 'Year': [], 'Quarter': [], 'Transaction_type': [], 'Transaction_district': [], 'Transaction_count': [], 'Transaction_amount': []}

for i in top_trans_state_list:
    p_i=path5+i+"/"
    top_trans_yr=os.listdir(p_i)
    for j in top_trans_yr:
        p_j=p_i+j+"/"
        top_trans_yr_list=os.listdir(p_j)
        for k in top_trans_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
                name = z['entityName']
                mtype = z['metric']['type']
                count = z['metric']['count']
                amount = z['metric']['amount']
                col5['Transaction_district'].append(name)
                col5['Transaction_type'].append(mtype)
                col5['Transaction_count'].append(count)
                col5['Transaction_amount'].append(amount)
                col5['State'].append(i)
                col5['Year'].append(j)
                col5['Quarter'].append(int(k.strip('.json')))

top_trans = pd.DataFrame(col5)

top_trans['State']=top_trans['State'].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_trans['State']=top_trans['State'].str.replace("-"," ")
top_trans['State']=top_trans['State'].str.title()
top_trans['State']=top_trans['State'].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")


# 6) TOP_USERS 
path6="C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/top/user/country/india/state/"
top_users_state_list = os.listdir(path6)
top_users_state_list

col6  = {'State': [], 'Year': [], 'Quarter': [], 'registeredusers': [], 'Transaction_district': []}

for i in top_users_state_list:
    p_i=path6+i+"/"
    top_users_yr=os.listdir(p_i)
    for j in top_users_yr:
        p_j=p_i+j+"/"
        top_users_yr_list=os.listdir(p_j)
        for k in top_users_yr_list:
            p_k=p_j+k
            Data=open(p_k,'r')
            D=json.load(Data)
            for z in D['data']['districts']:
                name = z['name']
                registeredusers = z['registeredUsers']
                col6['Transaction_district'].append(name)
                col6['registeredusers'].append(registeredusers)
                col6['State'].append(i)
                col6['Year'].append(j)
                col6['Quarter'].append(int(k.strip('.json')))

top_user= pd.DataFrame(col6)

top_user['State']=top_user['State'].str.replace("andaman-&-nicobar-islands","Andaman and Nicobar")
top_user['State']=top_user['State'].str.replace("-"," ")
top_user['State']=top_user['State'].str.title()
top_user['State']=top_user['State'].str.replace("dadra-&-nagar-haveli-&-daman-&-diu","Dadra and Nagar Haveli and Daman Diu")

# ----- MySQL Connection -----

mydb = mysql.connector.connect(
                        host="localhost",
                        user="root",
                        password="")
print(mydb)
mycursor = mydb.cursor(buffered=True)
#?mycursor.execute('create database phonepe')
mycursor.execute('use phonepe')

#  Create & Insert ->  "Aggregate Transaction" Table

create_query1 = '''CREATE TABLE if not exists aggregate_transaction (State varchar(100),
                                                                        Year int,
                                                                        Quarter int,
                                                                        Transaction_type varchar(100),
                                                                        Transaction_count int,
                                                                        Transaction_amount int)'''
mycursor.execute(create_query1)
mydb.commit()

for index, row in Agg_Trans.iterrows():
    insert_query1 = '''INSERT INTO aggregate_transaction (State, Year, Quarter, Transaction_type, Transaction_count, Transaction_amount)
                                                            values(%s,%s,%s,%s,%s,%s)'''
    values = (row["State"],
            row["Year"],
            row["Quarter"],
            row["Transaction_type"],
            row["Transaction_count"],
            row["Transaction_amount"]
            )
    mycursor.execute(insert_query1,values)
    mydb.commit()

# Create & Insert -> "Aggregate User" Table

create_query2 = '''CREATE TABLE if not exists aggregate_user (State varchar(100),
                                                            Year int,
                                                            Quarter int,
                                                            Brand_Name varchar(100),
                                                            Count bigint,
                                                            Percentage float)'''

mycursor.execute(create_query2)
mydb.commit()

for index, row in aggre_user.iterrows():
    insert_query2 = '''INSERT INTO aggregate_user (State, Year, Quarter, Brand_Name, Count, Percentage)
                                                    values(%s,%s,%s,%s,%s,%s)'''

values = (row["State"],
            row["Year"],
            row["Quarter"],
            row["Brand_Name"],
            row["Count"],
            row["Percentage"])
mycursor.execute(insert_query2, values)
mydb.commit()

# Create & Insert -> "Map Transaction" Table

create_query_3 = """CREATE TABLE IF NOT EXISTS map_transaction (State VARCHAR(100),
                                                                Year INT,
                                                                Quarter INT,
                                                                Transaction_type VARCHAR(100),
                                                                Transaction_district VARCHAR(100),
                                                                Transaction_count BIGINT,
                                                                Transaction_amount FLOAT )"""             
mycursor.execute(create_query_3)
mydb.commit()           

for index , row in map_trans.iterrows():
    insert_query3 = """INSERT INTO map_transaction (State,Year,Quarter,Transaction_type ,Transaction_district,Transaction_count ,Transaction_amount ) 
                        values (%s,%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_district"],
                row["Transaction_count"],
                row["Transaction_amount"])
mycursor.execute(insert_query3, values)
mydb.commit()

# Create & Insert -> "Map User" Table

create_query_4 = """CREATE TABLE IF NOT EXISTS map_users (State VARCHAR(100),
                                                            Year INT,
                                                            Quarter INT,
                                                            District VARCHAR(100),
                                                            Registered_user BIGINT,
                                                            App_opening BIGINT )"""
mycursor.execute(create_query_4)
mydb.commit() 

for index , row in map_users.iterrows():
    insert_query4 = """INSERT INTO map_users (State,Year,Quarter,District,Registered_user,App_opening) 
                        VALUES (%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quater"],
                row["District"],
                row["Registered_user"],
                row["App_opening"])
    
data=map_users.values.tolist()
mycursor.executemany(insert_query4,data)
mydb.commit()                 

# Create & Insert -> "Top Transaction" Table
create_query_5 = """CREATE TABLE IF NOT EXISTS top_transaction (
                                            State VARCHAR(100),
                                            Year INT,
                                            Quarter INT,
                                            Transaction_type VARCHAR(100),
                                            Transaction_district VARCHAR(100),
                                            Transaction_count BIGINT,
                                            Transaction_amount BIGINT)"""
                    
mycursor.execute(create_query_5)
mydb.commit()

for index , row in top_trans.iterrows():
    insert_query5 = """INSERT INTO top_transaction (State,Year,Quarter,Transaction_type ,Transaction_district, Transaction_count ,Transaction_amount) 
                        VALUES (%s,%s,%s,%s,%s,%s,%s)""" 
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["Transaction_type"],
                row["Transaction_district"],
                row["Transaction_count"],
                row["Transaction_amount"])
    
data=top_trans.values.tolist()
mycursor.executemany(insert_query5,data)
mydb.commit()

# Create & Insert "Top User" Table
create_query_6 = """CREATE TABLE IF NOT EXISTS  top_users (State VARCHAR(100),
                                                    Year INT,
                                                    Quarter INT,
                                                    registeredusers BIGINT,
                                                    Transaction_district VARCHAR(100)
                    )"""
                    
                    
mycursor.execute(create_query_6)
mydb.commit()

for index , row in top_user.iterrows():
    insert_query6 = """INSERT INTO top_users (State,Year,Quarter,registeredusers,Transaction_district)
    VALUES (%s,%s,%s,%s,%s)"""
    
    values =(row["State"],
                row["Year"],
                row["Quarter"],
                row["registeredusers"],
                row["Transaction_district"])
    
data=top_user.values.tolist()
mycursor.executemany(insert_query6,data)
mydb.commit()

# ---------------------------------------------
#  CREATE DATAFRAMES FROM SQL 

mydb = mysql.connector.connect( host="localhost", user="root", password="", database="phonepe") # MySQL DB Connection
print(mydb)
mycursor = mydb.cursor(buffered=True)

# Aggregated_transaction
mycursor.execute("select * from aggregate_transaction")
table1 = mycursor.fetchall()
Aggre_trans = pd.DataFrame(table1,columns = ("State", "Year", "Quarter", "Transaction_type", "Transaction_count", "Transaction_amount"))

#  Aggregated_user
mycursor.execute("select * from aggregate_user")
table2 = mycursor.fetchall()
Aggre_user = pd.DataFrame(table2,columns = ("State", "Year", "Quarter", "Brand_Name", "Count", "Percentage"))

#  Map_transaction
mycursor.execute("select * from map_transaction")
table3 = mycursor.fetchall()
Map_trans = pd.DataFrame(table3,columns = ("State","Year","Quarter","Transaction_type" ,"Transaction_district","Transaction_count" ,"Transaction_amount"))

#  Map_user
mycursor.execute("select * from map_users")
table4 = mycursor.fetchall()
Map_user = pd.DataFrame(table4,columns = ("State","Year","Quarter", "District","Registered_user","App_opening"))

#  Top_transaction
mycursor.execute("select * from top_transaction")
table5 = mycursor.fetchall()
Top_trans = pd.DataFrame(table5,columns = ("State","Year","Quarter", "Transaction_type" ,"Transaction_district", "Transaction_count", "Transaction_amount"))

#  Top_user
mycursor.execute("select * from top_users")
table6 = mycursor.fetchall()
Top_user = pd.DataFrame(table6, columns = ("State","Year","Quarter", "registeredusers","Transaction_district"))

# -----------------------------------------------------------------------------------------------------------------------------------------
def animate_all_amount():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response =requests.get(url)
    data1 = json.loads(response.content)
    state_names_tra = [feature["properties"]["ST_NM"] for feature in data1["features"]]
    state_names_tra.sort()

    df_state_names_tra = pd.DataFrame({"State":state_names_tra})

    frames = []

    for year in Map_user["Year"].unique():
        for quarter in Aggre_trans["Quarter"].unique():

            at1 = Aggre_trans[(Aggre_trans["Year"]==year)&(Aggre_trans["Quarter"]==quarter)]
            atf1 = at1[["State","Transaction_amount"]]
            atf1 = atf1.sort_values(by="State")
            atf1["Year"]=year
            atf1["Quarter"]=quarter
            frames.append(atf1)

    merged_df = pd.concat(frames)

    fig_tra = px.choropleth(merged_df, geojson= data1, locations= "State", featureidkey= "properties.ST_NM", color= "Transaction_amount",
                            color_continuous_scale= "Sunsetdark", range_color= (0,4000000000), hover_name= "State", title = "TRANSACTION AMOUNT",
                            animation_frame="Year", animation_group="Quarter")

    fig_tra.update_geos(fitbounds= "locations", visible =False)
    fig_tra.update_layout(width =600, height= 700)
    fig_tra.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_tra)

# ------------------------------------------
def payment_count():
    attype= Aggre_trans[["Transaction_type", "Transaction_count"]]
    att1= attype.groupby("Transaction_type")["Transaction_count"].sum()
    df_att1= pd.DataFrame(att1).reset_index()
    fig_pc= px.bar(df_att1,x= "Transaction_type",y= "Transaction_count",title= "TRANSACTION TYPE and TRANSACTION COUNT",
                color_discrete_sequence=px.colors.sequential.Redor_r)
    fig_pc.update_layout(width=600, height= 500)
    return st.plotly_chart(fig_pc)

# ------------------------------------------
def animate_all_count():
    url = "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    state_names_tra= [feature["properties"]["ST_NM"]for feature in data1["features"]]
    state_names_tra.sort()

    df_state_names_tra= pd.DataFrame({"State":state_names_tra})

    frames= []

    for year in Aggre_trans["Year"].unique():
        for quarter in Aggre_trans["Quarter"].unique():

            at1= Aggre_trans[(Aggre_trans["Year"]==year)&(Aggre_trans["Quarter"]==quarter)]
            atf1= at1[["State", "Transaction_count"]]
            atf1=atf1.sort_values(by="State")
            atf1["Year"]=year
            atf1["Quarter"]=quarter
            frames.append(atf1)

    merged_df = pd.concat(frames)

    fig_tra= px.choropleth(merged_df, geojson= data1, locations= "State",featureidkey= "properties.ST_NM",
                        color= "Transaction_count", color_continuous_scale="Sunsetdark", range_color= (0,3000000),
                        title="TRANSACTION COUNT", hover_name= "State", animation_frame= "Year", animation_group= "Quarter")

    fig_tra.update_geos(fitbounds= "locations", visible= False)
    fig_tra.update_layout(width= 600, height= 700)
    fig_tra.update_layout(title_font={"size":25})
    return st.plotly_chart(fig_tra)

# ------------------------------------------
def payment_amount():
    attype= Aggre_trans[["Transaction_type","Transaction_amount"]]
    att1= attype.groupby("Transaction_type")["Transaction_amount"].sum()
    df_att1= pd.DataFrame(att1).reset_index()
    fig_tra_pa= px.bar(df_att1, x= "Transaction_type", y= "Transaction_amount", title= "TRANSACTION TYPE and TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Blues_r)
    fig_tra_pa.update_layout(width= 600, height= 500)
    return st.plotly_chart(fig_tra_pa)

# ------------------------------------------
def reg_all_states(state):
    mu= Map_user[["State","District","Registered_user"]]
    mu1= mu.loc[(mu["State"]==state)]
    mu2= mu1[["District", "Registered_user"]]
    mu3= mu2.groupby("District")["Registered_user"].sum()
    mu4= pd.DataFrame(mu3).reset_index()
    fig_mu= px.bar(mu4, x= "District", y= "Registered_user", title= "DISTRICTS and REGISTERED USER",
                color_discrete_sequence=px.colors.sequential.Bluered_r)
    fig_mu.update_layout(width= 1000, height= 500)
    return st.plotly_chart(fig_mu)

# ------------------------------------------
def transaction_amount_year(sel_year):
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1= json.loads(response.content)
    state_names_tra= [feature["properties"]['ST_NM']for feature in data1["features"]]
    state_names_tra.sort()

    year= int(sel_year)
    atay= Aggre_trans[["State","Year","Transaction_amount"]]
    atay1= atay.loc[(Aggre_trans["Year"]==year)]
    atay2= atay1.groupby("State")["Transaction_amount"].sum()
    atay3= pd.DataFrame(atay2).reset_index()

    fig_atay= px.choropleth(atay3, geojson= data1, locations= "State", featureidkey= "properties.ST_NM",
                            color= "Transaction_amount", color_continuous_scale="rainbow", range_color=(0,800000000000),
                            title="TRANSACTION AMOUNT and STATES", hover_name= "State")

    fig_atay.update_geos(fitbounds= "locations", visible= False)
    fig_atay.update_layout(width=600,height=700)
    fig_atay.update_layout(title_font= {"size":25})
    return st.plotly_chart(fig_atay)

# ------------------------------------------
def payment_count_year(sel_year):
    year= int(sel_year)
    apc= Aggre_trans[["Transaction_type", "Year", "Transaction_count"]]
    apc1= apc.loc[(Aggre_trans["Year"]==year)]
    apc2= apc1.groupby("Transaction_type")["Transaction_count"].sum()
    apc3= pd.DataFrame(apc2).reset_index()

    fig_apc= px.bar(apc3,x= "Transaction_type", y= "Transaction_count", title= "PAYMENT COUNT and PAYMENT TYPE",
                    color_discrete_sequence=px.colors.sequential.Brwnyl_r)
    fig_apc.update_layout(width=600, height=500)
    return st.plotly_chart(fig_apc)

# ------------------------------------------
def transaction_count_year(sel_year):
    url= "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
    response= requests.get(url)
    data1=json.loads(response.content)
    state_names_tra= [feature["properties"]["ST_NM"]for feature in data1["features"]]
    state_names_tra.sort()

    year= int(sel_year)
    atcy= Aggre_trans[["State", "Year", "Transaction_count"]]
    atcy1= atcy.loc[(Aggre_trans["Year"]==year)]
    atcy2= atcy1.groupby("State")["Transaction_count"].sum()
    atcy3= pd.DataFrame(atcy2).reset_index()

    fig_atcy= px.choropleth(atcy3, geojson=data1, locations= "State", featureidkey= "properties.ST_NM",
                            color= "Transaction_count", color_continuous_scale= "rainbow",range_color=(0,3000000000),
                            title= "TRANSACTION COUNT and STATES",hover_name= "State")
    fig_atcy.update_geos(fitbounds= "locations", visible= False)
    fig_atcy.update_layout(width=600, height= 700)
    fig_atcy.update_layout(title_font={"size":25})
    return st.plotly_chart(fig_atcy)

# ------------------------------------------
def payment_amount_year(sel_year):
    year= int(sel_year)
    apay = Aggre_trans[["Year", "Transaction_type", "Transaction_amount"]]
    apay1= apay.loc[(Aggre_trans["Year"]==year)]
    apay2= apay1.groupby("Transaction_type")["Transaction_amount"].sum()
    apay3= pd.DataFrame(apay2).reset_index()

    fig_apay= px.bar(apay3, x="Transaction_type", y= "Transaction_amount", title= "PAYMENT TYPE and PAYMENT AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Burg_r)
    fig_apay.update_layout(width=600, height=500)
    return st.plotly_chart(fig_apay)

# ------------------------------------------
def reg_state_all_RU(sel_year,state):
    year= int(sel_year)
    mus= Map_user[["State", "Year", "District","Registered_user"]]
    mus1= mus.loc[(Map_user["State"]==state)&(Map_user["Year"]==year)]
    mus2= mus1.groupby("District")["Registered_user"].sum()
    mus3= pd.DataFrame(mus2).reset_index()

    fig_mus= px.bar(mus3, x= "District", y="Registered_user", title="DISTRICTS and REGISTERED USER",
                    color_discrete_sequence=px.colors.sequential.Cividis_r)
    fig_mus.update_layout(width= 600, height= 500)
    return st.plotly_chart(fig_mus)

# ------------------------------------------
def reg_state_all_TA(sel_year,state):
    year= int(sel_year)
    mts= Map_trans[["State", "Year","Transaction_district", "Transaction_amount"]]
    mts1= mts.loc[(Map_trans["State"]==state)&(Map_trans["Year"]==year)]
    mts2= mts1.groupby("Transaction_district")["Transaction_amount"].sum()
    mts3= pd.DataFrame(mts2).reset_index()

    fig_mts= px.bar(mts3, x= "Transaction_district", y= "Transaction_amount", title= "DISTRICT and TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Darkmint_r)
    fig_mts.update_layout(width= 600, height=  500)
    return st.plotly_chart(fig_mts)

# -----------------------------------------------------------------------------------------------------------------------------------------

def ques1():
    brand= Aggre_user[["Brand_Name","Count"]]
    brand1= brand.groupby("Brand_Name")["Count"].sum().sort_values(ascending=False)
    brand2= pd.DataFrame(brand1).reset_index()

    fig_brands= px.pie(brand2, values= "Count", names= "Brand_Name", color_discrete_sequence=px.colors.sequential.dense_r,
                        title= "Top Mobile Brands of Transaction_count")
    return st.plotly_chart(fig_brands)

def ques2():
    ht= Aggre_trans[["State", "Transaction_amount"]]
    ht1= ht.groupby("State")["Transaction_amount"].sum().sort_values(ascending= False)
    ht2= pd.DataFrame(ht1).reset_index().head(10)

    fig_lts= px.bar(ht2, x= "State", y= "Transaction_amount",title= "STATES WITH HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques3():
    lt= Aggre_trans[["State", "Transaction_amount"]]
    lt1= lt.groupby("State")["Transaction_amount"].sum().sort_values(ascending= True)
    lt2= pd.DataFrame(lt1).reset_index().head(10)

    fig_lts= px.bar(lt2, x= "State", y= "Transaction_amount",title= "STATES WITH LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence= px.colors.sequential.Oranges_r)
    return st.plotly_chart(fig_lts)

def ques4():
    stc= Aggre_trans[["State", "Transaction_count"]]
    stc1= stc.groupby("State")["Transaction_count"].sum().sort_values(ascending=False)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "State", y= "Transaction_count", title= "STATES WITH HIGHEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Magenta_r)
    return st.plotly_chart(fig_stc)

def ques5():
    stc= Aggre_trans[["State", "Transaction_count"]]
    stc1= stc.groupby("State")["Transaction_count"].sum().sort_values(ascending=True)
    stc2= pd.DataFrame(stc1).reset_index()

    fig_stc= px.bar(stc2, x= "State", y= "Transaction_count", title= "STATES WITH LOWEST TRANSACTION COUNT",
                    color_discrete_sequence= px.colors.sequential.Jet_r)
    return st.plotly_chart(fig_stc)

def ques6():
    htd= Map_trans[["Transaction_district", "Transaction_amount"]]
    htd1= htd.groupby("Transaction_district")["Transaction_amount"].sum().sort_values(ascending=False)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Transaction_district", title="TOP 10 DISTRICTS OF HIGHEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Emrld_r)
    return st.plotly_chart(fig_htd)

def ques7():
    htd= Map_trans[["Transaction_district", "Transaction_amount"]]
    htd1= htd.groupby("Transaction_district")["Transaction_amount"].sum().sort_values(ascending=True)
    htd2= pd.DataFrame(htd1).head(10).reset_index()

    fig_htd= px.pie(htd2, values= "Transaction_amount", names= "Transaction_district", title="TOP 10 DISTRICTS OF LOWEST TRANSACTION AMOUNT",
                    color_discrete_sequence=px.colors.sequential.Greens_r)
    return st.plotly_chart(fig_htd)

def ques8():
    sa= Map_user[["State", "App_opening"]]
    sa1= sa.groupby("States")["App_opening"].sum().sort_values(ascending=False)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "States", y= "App_opening", title="Top 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.deep_r)
    return st.plotly_chart(fig_sa)

def ques9():
    sa= Map_user[["State", "App_opening"]]
    sa1= sa.groupby("State")["App_opening"].sum().sort_values(ascending=True)
    sa2= pd.DataFrame(sa1).reset_index().head(10)

    fig_sa= px.bar(sa2, x= "State", y= "App_opening", title="Lowest 10 States With AppOpens",
                color_discrete_sequence= px.colors.sequential.dense_r)
    return st.plotly_chart(fig_sa)

def ques10():
    dt= Map_trans[["Transaction_district", "Transaction_amount"]]
    dt1= dt.groupby("Transaction_district")["Transaction_amount"].sum().sort_values(ascending=True)
    dt2= pd.DataFrame(dt1).reset_index().head(50)

    fig_dt= px.bar(dt2, x= "Transaction_district", y= "Transaction_amount", title= "DISTRICTS WITH LOWEST TRANSACTION AMOUNT",
                color_discrete_sequence= px.colors.sequential.Mint_r)
    return st.plotly_chart(fig_dt)

# -----------------------------------------------------------------------------------------------------------------------------------------
# Streamlit part
def main():
    st.set_page_config(layout="wide")

    # Streamlit part
    st.title(":rainbow[PhonePe Data Visualization and Exploration]")
    tab1, tab2, tab3 = st.tabs(["***HOME***","***EXPLORE DATA***","***TOP CHARTS***"])
    col1, col2, col3 = st.columns(3)

    with tab1:
        col1= st.columns(1)[0]
        with col1:
            image = Image.open(r"C:\Users\Dell\Desktop\Python file\Phonepe_Logo.jpg")
            st.image(image)
            st.markdown("<h2 style='color: #ff5733; text-align: center;'>Welcome to PhonePe Data Visualization</h2>", unsafe_allow_html=True)
            st.markdown("<blockquote style='font-style: italic; font-size: 18px; color: #333;'>\"Explore India's Best Transaction App\"</blockquote>", unsafe_allow_html=True)
            st.markdown("PhonePe is an Indian digital payments and financial technology company. "
                        "It offers the following features:")
            st.markdown("- **Credit & Debit card linking**")
            st.markdown("- **Bank Balance check**")
            st.markdown("- **Money Storage**")
            st.markdown("- **PIN Authorization**")
            st.download_button("DOWNLOAD THE APP NOW", "https://www.phonepe.com/app-download/")

        col2= st.columns(1)[0]
# ----------------------------------------------------
    with tab2:
        sel_year = st.selectbox("select the Year",("All", "2018", "2019", "2020", "2021", "2022", "2023"))
        if sel_year == "All" :
            col1, col2 = st.columns(2)
            with col1:
                animate_all_amount()
                payment_count()
                
            with col2:
                animate_all_count()
                payment_amount()

            state=st.selectbox("select the state",('Andaman And Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh','Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                    'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa','Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep','Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 
                                                    'Mizoram','Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan','Sikkim', 'Tamil Nadu', 'Telangana',
                                                    'Tripura', 'Uttar Pradesh','Uttarakhand', 'West Bengal'))
            reg_all_states(state)

        else:
            col1,col2= st.columns(2)

            with col1:
                transaction_amount_year(sel_year)
                payment_count_year(sel_year)

            with col2:
                transaction_count_year(sel_year)
                payment_amount_year(sel_year)
                state= st.selectbox("select the state",('Andaman And Nicobar', 'Andhra Pradesh', 'Arunachal Pradesh','Assam', 'Bihar', 'Chandigarh', 'Chhattisgarh',
                                                    'Dadra & Nagar Haveli & Daman & Diu', 'Delhi', 'Goa','Gujarat', 'Haryana', 'Himachal Pradesh', 'Jammu & Kashmir',
                                                    'Jharkhand', 'Karnataka', 'Kerala', 'Ladakh', 'Lakshadweep','Madhya Pradesh', 'Maharashtra', 'Manipur', 'Meghalaya', 
                                                    'Mizoram','Nagaland', 'Odisha', 'Puducherry', 'Punjab', 'Rajasthan','Sikkim', 'Tamil Nadu', 'Telangana',
                                                    'Tripura', 'Uttar Pradesh','Uttarakhand', 'West Bengal'))
                reg_state_all_RU(sel_year,state)
                reg_state_all_TA(sel_year,state)

# Display list of 10 questions in the sidebar
    with tab3:
        ques = st.selectbox("select the question", ('1.Top Brands Of Mobiles Used PhonePE','2.States With Highest Transaction Amount','3.States With Lowest Transaction Amount',
                                                    '4.States With Highest Transaction Count','5.States With Lowest Transaction Count',
                                                    '6.Districts With Highest Transaction Amount', '7.Top 10 Districts With Lowest Transaction Amount',
                                                    '8.Top 10 States With AppOpens', '9.Least 10 States With AppOpens', 
                                                    '10.Top 50 Districts With Lowest Transaction Amount'))
    if ques=='1.Top Brands Of Mobiles Used PhonePE':
        ques1()

    elif ques=='2.States With Highest Transaction Amount':
        ques2()

    elif ques=='3.States With Lowest Transaction Amount':
        ques3()

    elif ques=='4.States With Highest Transaction Count':
        ques4()

    elif ques=='5.States With Lowest Transaction Count':
        ques5()

    elif ques=='6.Districts With Highest Transaction Amount':
        ques6()

    elif ques=='7.Top 10 Districts With Lowest Transaction Amount':
        ques7()

    elif ques=='8.Top 10 States With AppOpens':
        ques8()

    elif ques=='9.Least 10 States With AppOpens':
        ques9()

    elif ques=='10.Top 50 Districts With Lowest Transaction Amount':
        ques10()
# -------------------------------------------
if __name__ == "__main__":
    main()