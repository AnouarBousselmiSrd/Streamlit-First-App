# -*- coding: utf-8 -*-
"""
Created on Mon Oct 17 09:46:00 2022

@author: proc12
"""

import streamlit as st
import pandas as pd 
from io import BytesIO
#from pyxlsb import open_workbook as open_xlsb

header = st.container()
dataset = st.container()
features = st.container()

test1=False
test2=False

with header:
    st.title("Conferência de Database")
    st.text("Escolha os arquivos para serem comparados")
    uploaded_file1 = st.file_uploader("Escolha a nova versão")
    uploaded_file2 = st.file_uploader("escolha a versão antiga")

if uploaded_file1:
    # Check MIME type of the uploaded file
    if uploaded_file1.type == "text/csv":
        df1 = pd.read_csv(uploaded_file1,sheet_name=None)
    else:
        df1 = pd.read_excel(uploaded_file1,sheet_name=None)
    test1=True
    for sheet in df1:
        df1[sheet]=df1[sheet].apply(lambda x: x.astype(str).str.upper())
        df1[sheet]=df1[sheet].apply(lambda x: x.astype(str).str.strip())
    
if uploaded_file2:
    # Check MIME type of the uploaded file
    if uploaded_file2.type == "text/csv":
        df2 = pd.read_csv(uploaded_file2,sheet_name=None)
    else:
        df2 = pd.read_excel(uploaded_file2,sheet_name=None)
    test2=True
    for sheet in df2:
        df2[sheet]=df2[sheet].apply(lambda x: x.astype(str).str.upper())    
        df2[sheet]=df2[sheet].apply(lambda x: x.astype(str).str.strip())

            
            
def to_excel(df1, df2):
    output = BytesIO()
    writer = pd.ExcelWriter(output, engine='xlsxwriter')
    for sheet in df1:
        if sheet in df2:
            x=df2[sheet]
            y=df1[sheet]
            for column in x.columns:
                if column not in y.columns:
                    x.drop(column , axis=1, inplace=True)
            for column in y.columns:
                if column not in x.columns:
                    y.drop(column , axis=1, inplace=True)
            df3 = y[x.ne(y).any(axis=1)]
            df3.to_excel(writer, sheet_name=sheet, index=False, header=True)
    writer.save()
    processed_data = output.getvalue()
    return processed_data

if test1 & test2:
    df_xlsx = to_excel(df1, df2)
    st.download_button(label='📥 Baixar Resultado',
                                data=df_xlsx ,
                                file_name= 'Conferência final.xlsx')