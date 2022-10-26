# -*- coding: utf-8 -*-
"""
Created on Fri Oct 21 12:04:23 2022

@author: proc12
"""

import requests
import pandas as pd
from datetime import timedelta
import datetime
from prefect import task, Flow
from prefect.schedules import IntervalSchedule
import numpy as np
import psycopg2
from sqlalchemy import create_engine
engine = create_engine('postgresql://postgres:12345@localhost:5432/Engenharia')

Client_id="eab5d854-0749-4951-9f16-e9d381da81b1"
tenant_id="efc36488-111c-4a40-aa3d-0cf66f4998dd"
Url= "https://login.microsoftonline.com/efc36488-111c-4a40-aa3d-0cf66f4998dd/oauth2/token"
grant_type  = "client_credentials"
client_secret = "Od68Q~~ZYDvhihCzrsxnYjAIz_grqWmzClNGPckw"
resource = "https://serdia-prod.operations.dynamics.com/"
Dynamics_data_url="https://serdia-prod.operations.dynamics.com/data/"

def get_access_token(response):
    return (response['token_type'],response['access_token'])

def get_crdentials():
    response= requests.post(Url, data={'client_id': Client_id ,
                                      'tenant_id':tenant_id,
                                      'grant_type': grant_type,
                                      'client_secret':client_secret,
                                      'resource':resource,
                                      }).json()
    type, token= get_access_token(response)
    bearer = f"{type} {token}"
    return bearer

def extract_latest_data(URI, bearer):
    #urls=[]
    Customer_Data= requests.get(URI, headers = {'Authorization':bearer})
    res=Customer_Data.json()
    values=res['value']
    #urls.append(URI)
    while '@odata.nextLink' in res.keys():
        Customer = res['@odata.nextLink']
        print(Customer)
        Customer_Data= requests.get(Customer, headers = {'Authorization':bearer})
        res=Customer_Data.json()
        for item in res['value']:
            values.append(item)
        #urls.append(Customer)
        URI=Customer
        #URI=urls[-3]
    new_df= pd.json_normalize(values) 
    print('new df ', new_df.shape)
    print('last_URI in '+'________'+ URI)
    return new_df, URI

token =get_crdentials()
    
df1, uri1=extract_latest_data('https://serdia-prod.operations.dynamics.com/data/MPS_ItensAlternativos_MPSReleasedProducts',token)
df2=df1[['ItemId','MPS_ItensAltTipoComponente','MPS_ItensAltCodigoEncapsulamento','MPS_ItensAltCodigoTecnoMontagem']]

df2.to_sql('MPSReleasedProducts', engine, if_exists='replace',index=False)