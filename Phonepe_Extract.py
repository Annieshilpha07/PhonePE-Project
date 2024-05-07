import git
import os
import pandas as pd
import json

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


#  2) AGGREGATE_USER

path2= "C:/Users/Dell/Desktop/Python file/PhonePE Project/pulse/data/aggregated/user/country/india/state/"  #aggregate users path
agg_user_list=os.listdir(path2)
agg_user_list
col2={'State':[],'Year':[],'Quarter':[],'Brand_Name':[],'Count':[],'Percentage':[]}

for state in agg_user_list:
    actual_state=path2 + "/"+state + "/"
    agg_yr=os.listdir(actual_state)
    for year in agg_yr:
        actual_yr=actual_state +"/"+ year + "/"
        agg_yr_list=os.listdir(actual_yr)
        for file in agg_yr_list:
            actual_file=actual_yr+file
            Data=open(actual_file,'r')
            D2=json.load(Data)
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
