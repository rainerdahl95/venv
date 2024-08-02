import pandas as pd 
import numpy as np
# !pip install matplotlib
# !pip install openpyxl
import matplotlib.pyplot as plt
import openpyxl
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta

# Se definen las rutas de acceso a las bases de datos (veremos si lo obtenemos mas adelante por consultas SQL, pero en ese caso, asegurarse que cada DF es extraido de la misma manera en cuanto a estructura)
path_excel=r"C:\Users\RainerDahlbeck\infini.es\Clientes - Documentos\Splitmania\Proyectos\Modelo Recuperación\Documentos de Trabajo\DATA_TRABAJADA\DATOS PARA IA.xlsx"
path_excel_articulos=r"C:\Users\RainerDahlbeck\infini.es\Clientes - Documentos\Splitmania\Proyectos\Modelo Recuperación\Documentos de Trabajo\DATA_TRABAJADA\Tabla maestra Artículos.xlsx"
path_excel_tipo_pagos=r"C:\Users\RainerDahlbeck\infini.es\Clientes - Documentos\Splitmania\Proyectos\Modelo Recuperación\Documentos de Trabajo\DATA_TRABAJADA\Mapping_Formas_Pago.xlsx"
path_excel_tiendas=r"C:\Users\RainerDahlbeck\infini.es\Clientes - Documentos\Splitmania\Proyectos\Modelo Recuperación\Documentos de Trabajo\DATA_TRABAJADA\Tiendas.xlsx"

df_tipo_pago=pd.read_excel(path_excel_tipo_pagos)
df_tiendas=pd.read_excel(path_excel_tiendas)

#Creacion de df del "maestro de clientes" llamado "df_clientes"
df_clientes=pd.read_excel(path_excel,sheet_name='CLIENTES')
#limpieza de df_clientes
df_clientes=df_clientes[df_clientes['CODIGOCLIENTE']!=999999]
df_clientes=df_clientes[df_clientes['CODIGOCLIENTE']!=999998]
df_clientes=df_clientes[df_clientes['NOMBRECLIENTE']!='EMPRESA INEXISTENTE']
df_clientes=df_clientes[df_clientes['NOMBRECLIENTE']!=0]
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.startswith('**PASA A CODIGO')]
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.startswith('*PASA A CODIGO')]
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.startswith('PASA A CODIGO')]
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.startswith('PASADO')]
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.startswith('*PASADO')]
df_clientes['concatenado']=df_clientes['CODIGOTIENDA'].astype(str)+"-"+df_clientes['CODIGOCLIENTE'].astype(str) # se crea el concatenado que define cliente único según id interno de cada tienda y el id de cada tienda, para SplitMania
df_clientes=df_clientes[~df_clientes['NOMBRECLIENTE'].astype(str).str.contains("SPLIT")]

# Creacion de df de todas las lineas de ventas, llamado "df"
df=pd.read_excel(path_excel,sheet_name='FACTURAS+LINEAS')
#limpieza datos facturas
df=df[df['CODIGOCLIENTE']!=999999]
df=df[df['CODIGOCLIENTE']!=999998]
df=df[df['CIF']!=0]
df=df[df['CIF']!=99999999]
df=df[df['RECTIFICATIVA']==0]
df=df[df['UNIDADES']!=0]

#Creacion de parametros transformados (concatenados)
df['concatenado']=df['CODIGOTIENDA'].astype(str)+"-"+df['CODIGOCLIENTE'].astype(str) # se crea el concatenado que define cliente único según id interno de cada tienda y el id de cada tienda, para SplitMania
df['Factura']=df['concatenado']+"-"+df['NUMFACTURA'].astype(str)
df['FECHAFACTURA'] = pd.to_datetime(df['FECHAFACTURA']) #.dt.strftime('%d/%m/%Y')

# Creacion de df de articulos
df_articulos=pd.read_excel(path_excel_articulos)
# Limpieza de df de articulos
df_articulos=df_articulos[df_articulos['CODIGOFAMILIA'].notna()]

## Se definen los distintos periodos para el analisis de los datos de venta
# Obtener la fecha actual
hoy = datetime.now()
# Ajustar la fecha al primer día del mes actual
fin_mes_actual = hoy.replace(day=1 , hour=0 , minute=0 , second=0 , microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
# Crear una lista para almacenar las fechas de inicio de cada mes
meses = []
# Generar fechas de inicio de mes desde el mes actual hasta 37 meses atrás, si se desea ampliar o reducir el periodo se cambia el valor dentro de "range()"
for mes in range(37):  # Incluye 0 (mes actual) hasta 37 meses atrás
    fecha_inicio_aux = fin_mes_actual - relativedelta(months=mes)
    meses.append(fecha_inicio_aux)

meses = pd.to_datetime(meses)

# Se define df_list_aux, lista que en cada casilla contiene un df de resumen de la facturacion para cada mes
df_list_aux = []
for mes in meses:
    df_facturacion_aux = df[(df['FECHAFACTURA']<=mes) & (df['FECHAFACTURA']>(mes.replace(day=1) - relativedelta(days=1)))].groupby('Factura').agg({
        'CODIGOARTICULO': 'count',  # Número de productos distintos por factura
        'UNIDADES': 'sum',    # Suma de cantidades totales de producto por factura (aquí se debe analizar casos de productos a granel, que están como variables continuas y no discretas)
        'TFACTURA': 'mean',    # Media del TFACTURA (cada linea tiene el final)
        'concatenado' : 'first',   # El codigo de cliente vinculado a la factura
        'FECHAFACTURA' : 'first'    # La fecha de la factura
    }).reset_index()
    df_list_aux.append(df_facturacion_aux)


## Se definen las funciones para calculo de metricas

# def calcular_metrics(concatenado, df_facturacion):
#     results = {}
#     results['avg_tkt'] = df_facturacion['TFACTURA'].mean()
#     results['purch_freq'] = (df_facturacion['FECHAFACTURA'].max()-df_facturacion['FECHAFACTURA'].min()).days/(df_facturacion['Factura'].count()-1) if df_facturacion['Factura'].count()>1 else 0
#     results['cantidad_compras']= df_facturacion['Factura'].count()
#     return results

def avg_tkt(concatenado, df_facturacion):
    avg_tkt = df_facturacion['TFACTURA'].mean()
    return avg_tkt

def purch_freq(concatenado, df_facturacion):
    purch_freq = (df_facturacion['FECHAFACTURA'].max()-df_facturacion['FECHAFACTURA'].min()).days/(df_facturacion['Factura'].count()-1) if df_facturacion['Factura'].count()>1 else 0
    return purch_freq

def cantidad_compras(concatenado, df_facturacion):
    cantidad_compras = df_facturacion['Factura'].count()
    return cantidad_compras

# Se define df_modelo_list, que será la lista de df_merged_final de cada periodo analizado, lo que alimentaremos al modelo
df_modelo_list = []
# Aqui debe empezar el for que recorre los periodos analizados y regula el dataframe de facturas acorde al mes en cuestion

for i in range(1,2):

    # Se define el df "df_facturacion" que contiene datos de venta desde el inicio hasta "i" meses atrás
    df_facturacion = pd.concat(df_list_aux[i:] , ignore_index= True)
    df_facturacion.sort_values('FECHAFACTURA',ascending=True)

    ## Fechas de referencia para cada iteracion
    # Fechas del periodo analizado
    df_facturacion['FECHAFACTURA']=pd.to_datetime(df_facturacion['FECHAFACTURA'])
    # Fechas de inicio para cada periodo
    inicio_mes_analisis = df_facturacion['FECHAFACTURA'].max().replace(day=1 , hour=0 , minute=0 , second=0 , microsecond=0)
    fin_mes = inicio_mes_analisis + relativedelta(months=1) - relativedelta(days=1)
    periodos = {}
    # Generar fechas para cada mes desde 1 hasta 36 meses hacia atrás
    for i in range(1, 37):  # Esto incluye hasta 36 meses
        periodo_clave = f'{i}m'  # Clave como '1m', '2m', ..., '36m'
        periodos[periodo_clave] = inicio_mes_analisis - relativedelta(months=i)

    # Se generan df auxiliares para analisis posterior de clientes "activos" o que tuvieron compra en los 12 meses previos al mes de analisis

    df_facturacion_aux = df_facturacion[df_facturacion['FECHAFACTURA']<inicio_mes_analisis].groupby('concatenado').agg({
        'FECHAFACTURA': 'max',  # Fecha de ultima compra, previo al inicio de mes analizado
    }).reset_index()

    df_facturacion_aux = df_facturacion_aux.rename(columns={'FECHAFACTURA': 'ultima_compra'}) # df_facturacion_aux  sirve para obtener la fecha de la ultima compra para cada cliente, previo al mes de analisis

    df_clientes_aux = pd.merge(df_clientes,df_facturacion_aux , how = 'inner' , on = 'concatenado')
    df_clientes_aux['ultima_compra']=pd.to_datetime(df_clientes_aux['ultima_compra'])

    # Se define df_clientes_activos que es el dataframe de los clientes con compras en los ultimos 12 meses, previos al mes de analisis
    df_clientes_activos = df_clientes_aux[(df_clientes_aux['ultima_compra']>=periodos['12m']) ]

    # Definicion de df_facturacion para 1m
    df_facturacion2 = df_facturacion[(df_facturacion['FECHAFACTURA'] >= periodos['1m']) & (df_facturacion['FECHAFACTURA'] < inicio_mes_analisis)]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])

    # Calculo avg_tkt_1m
    df_clientes_activos['avg_tkt_1m'] = df_clientes_activos['concatenado'].apply(
            # lambda x: calcular_metrics(x, df_facturacion2[df_facturacion2['concatenado'] == x])['avg_tkt']
            lambda x: avg_tkt(x, df_facturacion2[df_facturacion2['concatenado'] == x])
        )
    df_clientes_activos['avg_tkt_1m']=df_clientes_activos['avg_tkt_1m'].fillna(0)

    #Definicion de df_facturacion para ccc
    df_facturacion2 = df_facturacion[(df_facturacion['FECHAFACTURA'] >= inicio_mes_analisis)]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])

    # Calculo CCC, métrica que nos indica si el cliente realizó compras en el mes de analisis (actual)
    df_clientes_activos['ccc'] = df_clientes_activos['concatenado'].apply(
            # lambda x: calcular_metrics(x, df_facturacion2[df_facturacion2['concatenado'] == x])['avg_tkt']
            lambda x: avg_tkt(x, df_facturacion2[df_facturacion2['concatenado'] == x])
        )
    df_clientes_activos['ccc']=np.where(df_clientes_activos['ccc']>0 , 1 , 0)

    #Definicion de df_facturacion para 2m
    df_facturacion2 = df_facturacion[(df_facturacion['FECHAFACTURA'] >= periodos['2m']) & (df_facturacion['FECHAFACTURA'] < periodos['1m'])]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])
    # Calculo avg_tkt_2m
    df_clientes_activos['avg_tkt_2m'] = df_clientes_activos['concatenado'].apply(
            # lambda x: calcular_metrics(x, df_facturacion2[df_facturacion2['concatenado'] == x])['avg_tkt']
            lambda x: avg_tkt(x, df_facturacion2[df_facturacion2['concatenado'] == x])
        )
    df_clientes_activos['avg_tkt_2m']=df_clientes_activos['avg_tkt_2m'].fillna(0)

    #Definicion de df_facturacion para 3m
    df_facturacion2 = df_facturacion[(df_facturacion['FECHAFACTURA'] >= periodos['3m']) & (df_facturacion['FECHAFACTURA'] < periodos['2m'])]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])
    # Calculo avg_tkt_3m
    df_clientes_activos['avg_tkt_3m'] = df_clientes_activos['concatenado'].apply(
            # lambda x: calcular_metrics(x, df_facturacion2[(df_facturacion2['concatenado'] == x)])['avg_tkt']
            lambda x: avg_tkt(x, df_facturacion2[(df_facturacion2['concatenado'] == x)])
        )
    df_clientes_activos['avg_tkt_3m']=df_clientes_activos['avg_tkt_3m'].fillna(0)

    # Se define el parametro "SMD", booleano que nos indica si fue atendido o no poor agente de tipo SMD que teoricamente es personalizado para atencion especial para caso churn
    df_clientes_activos['SMD'] = np.where(df_clientes_activos['NOMBREAGENTE'].str.contains('SMD') , 1 , 0)

    #Definicion de df_facturacion para el total del historico, excluyendo el mes analizado
    df_facturacion2 = df_facturacion[df_facturacion['FECHAFACTURA'] < inicio_mes_analisis]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])
    # Calculo de frecuencia de compra 
    df_clientes_activos['frecuencia_compra'] = df_clientes_activos['concatenado'].apply(
        # lambda x: calcular_metrics(x, df_facturacion2[df_facturacion2['concatenado'] == x])['purch_freq']
        lambda x: purch_freq(x, df_facturacion2[df_facturacion2['concatenado'] == x])
    )
    df_clientes_activos['frecuencia_compra']=df_clientes_activos['frecuencia_compra'].fillna(pd.to_timedelta(0))

    #Definicion de df_facturacion para el total del historico, excluyendo el mes analizado
    df_facturacion2 = df_facturacion[df_facturacion['FECHAFACTURA'] < inicio_mes_analisis]
    df_facturacion2['FECHAFACTURA']=pd.to_datetime(df_facturacion2['FECHAFACTURA'])
    # Calculo de cantidad de compras
    df_clientes_activos['cantidad_compras'] = df_clientes_activos['concatenado'].apply(
        # lambda x: calcular_metrics(x, df_facturacion[df_facturacion['concatenado'] == x])['cantidad_compras']
        lambda x: cantidad_compras(x, df_facturacion[df_facturacion['concatenado'] == x])
    )

    # Calculo de parametro cliente_perdido, booleano que indica si el cliente en cuestión no realizó compras en los últimos 3 meses previos al mes de analisis
    df_clientes_activos['cliente_perdido'] = np.where((df_clientes_activos['avg_tkt_1m']==0) & (df_clientes_activos['avg_tkt_2m']==0) & (df_clientes_activos['avg_tkt_3m']==0) , 1 , 0)

    # Calculo de parametro recuperado, booleano que indica si el cliente en cuestión no realizó compras en los últimos 3 meses previos al mes de analisis, pero si compró en el mes de analisis
    df_clientes_activos['cliente_recuperado'] = np.where((df_clientes_activos['ccc']!=0) & (df_clientes_activos['avg_tkt_1m']==0) & (df_clientes_activos['avg_tkt_2m']==0) & (df_clientes_activos['avg_tkt_3m']==0) , 1 , 0)

    df_clientes_activos ['mes_analizado'] = inicio_mes_analisis.month 
    df_clientes_activos['anho_analizado'] = inicio_mes_analisis.year

    df_clientes_activos['ultima_compra'] = pd.to_datetime(df_clientes_activos['ultima_compra'].replace("0", pd.NaT), errors='coerce')
    df_clientes_activos['ultima_compra'] = (inicio_mes_analisis - df_clientes_activos['ultima_compra']).dt.days

    df_merged = pd.merge(df_clientes_activos, df_tiendas, on='CODIGOTIENDA', how='left')

    df_merged['periodo'] = df_merged['mes_analizado'].astype(str) + "-" + df_merged['anho_analizado'].astype(str)
    df_merged_final = df_merged[['concatenado' , 'ultima_compra' , 'avg_tkt_3m' , 'avg_tkt_2m' , 'avg_tkt_1m' , 'ccc' , 'SMD' , 'frecuencia_compra' , 'cantidad_compras' ,  'cliente_perdido' , 'cliente_recuperado' , 'periodo' , 'Nombre']]

    df_modelo_list.append(df_merged_final)

# Aqui debería terminar el for, teniendo finalmente la lista df_modelo_list con los df_merged_final, modelo de analisis para cada periodo y luego lo juntamos todo al df_modelo que seria el final
df_modelo_final = pd.concat(df_modelo_list, ignore_index= True)

print(df_modelo_list)