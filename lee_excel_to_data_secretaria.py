import psycopg2
from datetime import datetime as dt, date


'''
NOTA IMPORTANTE: TODOS LOS CAMPOS EN LA TABLA A CARGAR EN LA BD DEBEN SER DE TIPO STRING
Y LA TRANSFORMACION DE TIPOS SE DEBE HACER EN LA TRANSFORMACION DE LOS DATOS
ESTO SEGUN REUNION DEL 2020-07-02 CON BINDE Y DW
''' 


'''
conexion = psycopg2.connect(user='ibm_cloud_d7851057_5048_42bd_947b_bf2d2f6621d1',
                            password='9cf0a2e0e3797c3745f9573b1ecd88da60384cadaec50192e6d66f0051a16447',
                            host='75fadf29-6907-44fd-8c98-56c72bf26948.blijti4d0v0nkr55oei0.databases.appdomain.cloud',
                            port='32579',
                            database='IJALTI_PRUEBA')
'''



conexion = psycopg2.connect(user='postgres',
                            password='admin',
                            host='127.0.0.1',
                            port='5432',
                            database='coronavirus')
                            


#archivo = open("C:\\Clientes\\IJALTI - Coronavirus\\data desde la pagina de coronavirus.gob.mx\\200620COVID19MEXICO_rut.csv", "r")
archivo = open("C:\\Clientes\\IJALTI - Coronavirus\\Datos Abiertos Secretaria e Ijalti\\200804COVID19MEXICO.csv", "r")

filas = [[]]

try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    #sql = 'TRUNCATE TABLE staging.datos_secretaria'
    print('antes del truncate')
    sql = 'TRUNCATE TABLE staging.datos_secretaria'
    cursor1.execute(sql)
    print('despues del truncate')
    sql = 'INSERT INTO staging.datos_secretaria (FECHA_ACTUALIZACION,UID_REGISTRO,ORIGEN,SECTOR,ENTIDAD_UM,SEXO,ENTIDAD_NAC,ENTIDAD_RES,MUNICIPIO_RES,TIPO_PACIENTE,FECHA_INGRESO,FECHA_SINTOMAS,FECHA_DEF,INTUBADO,NEUMONIA,EDAD,NACIONALIDAD,EMBARAZO,HABLA_LENGUA_INDIG,DIABETES,EPOC,ASMA,INMUSUPR,HIPERTENSION,OTRA_COM,CARDIOVASCULAR,OBESIDAD,RENAL_CRONICA,TABAQUISMO,OTRO_CASO,RESULTADO,MIGRANTE,PAIS_NACIONALIDAD,PAIS_ORIGEN,UCI) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'

    i=0
    #aqui leo linea por linea...
    for linea in archivo:
        #la primera linea es el encabezado...
        if i==0:
            print('primera fila que son encabezados')
            filas.clear()
        else:
            #aca debo separar los campos por coma, y asignarselo a cada campo
            if linea:

                campos = linea.rstrip("\n").split(sep=',')

                for j in range(34):
                    campos[j] = campos[j].replace('"','').replace("'","")

                filas.append(campos)

                #cursor1.execute(sql, campos)
        i+=1
   
    #print(filas.head())
    cursor1.executemany(sql, filas) #se usa executemany en lugar de execute y esto es porque lo que queremos agregar es la tupla
    conexion.commit()
    registros_insertados = cursor1.rowcount
    print(f'Registros insertados: {registros_insertados}')
    
except Exception as e:
    print("ocurrio un error en el insert")
    print(e)
finally:
    archivo.close()
    #conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')