from jproperties import Properties
from pathlib import Path
import pandas as pd
import numpy as np
import tabula
import pdfplumber
import seaborn as sns
import matplotlib.pyplot as plt
from snowflake.snowpark import Session

class analiza_data:

    def __init__(self):
        configs = Properties()
        with open('config.properties','rb') as f:
            configs.load(f)
        ruta = configs.get('data.source_path').data
        filename = configs.get('data.filename').data
        self.datafilename_pdf = configs.get('data.filename_pdf').data
        self.arch= ruta + filename
        self.arch_pdf = ruta + self.datafilename_pdf
        ## parametros de conexión a snowflake
        self.connection_parameters = {
            "account": configs.get('snowflake.account').data,
            "user": configs.get('snowflake.user').data,
            "password": configs.get('snowflake.password').data,
            "role": configs.get('snowflake.role').data,
            "warehouse": configs.get('snowflake.warehouse').data,
            "database": configs.get('snowflake.database').data,
            "schema": configs.get('snowflake.schema').data
        } 
        

    def analiza(self):
        print(self.arch)
        print(self.arch_pdf)
        #tablas = tabula.read_pdf(self.arch_pdf,pages='all',multiple_tables=True)
        #print(len(tablas))
        #for i in range(0,len(tablas)):
        #    print(tablas[i])
        with pdfplumber.open(self.arch_pdf) as pdf:
            pagina = pdf.pages[0]
            tabla = pagina.extract_table()
            #print(tabla)
        df = pd.DataFrame(tabla[1:], columns=tabla[0])
        print(df.head())
        df.columns = [
            'Estado', 'Probables_25', 'Conf_25', 'Incidencia_25', 'Def_25', 'Letalidad_25',
            'Probables_26', 'Conf_26', 'Incidencia_26', 'Def_26', 'Letalidad_26', 'Total_Conf'
        ]

        # Limpiar caracteres basura y convertir a numérico
        cols_num = df.columns.drop('Estado')
        for col in cols_num:
            df[col] = pd.to_numeric(df[col].astype(str).str.replace(',', '').replace('-', '0'), errors='coerce').fillna(0)
        print(df.head())
        df.drop(0,axis=0,inplace=True)
        df['fecha_informe'] = pd.to_datetime('2026-02-10')
        df['ratio_conf_2025'] = df['Conf_25'] / df['Probables_25']
        df['ratio_conf_2026'] = df['Conf_26'] / df['Probables_26']
        print(df.head())
        print(df.columns.to_list())
        print(df[['Estado', 'Probables_25','Conf_25','ratio_conf_2025', 'Probables_26','Conf_26','ratio_conf_2026']])
        


                    


