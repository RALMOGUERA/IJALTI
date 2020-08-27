import psycopg2
from datetime import datetime as dt, date

'''
NOTA IMPORTANTE: TODOS LOS CAMPOS EN LA TABLA A CARGAR EN LA BD DEBEN SER DE TIPO STRING
Y LA TRANSFORMACION DE TIPOS SE DEBE HACER EN LA TRANSFORMACION DE LOS DATOS.
ADEMAS DE ESTO SE DEBE GUARDAR EN UNA TABLA HISTORICA TODO LO QUE SE SUBA TA CUAL COMO SUBIO.
ESTO APLICA PARA TODAS LAS FUENTES DE DATOS, EN DONDE TODO SERA SIEMPRE DE TIPO STRING
Y SE TENDRA UNA TABLA CON LOS HISTORICOS APARTE
ADEMAS SE GUARDARAN TODAS LAS FECHAS EN FORMATO TIMESTAMP Y NO DE TIPO DATE
ESTO SEGUN REUNION DEL 2020-07-02 CON BINDE Y DW
''' 


'''
conexion = psycopg2.connect(user='ibm_cloud_a78535f7_a870_4818_9acb_f8b18136e36d',
                            password='6661d40f3b44f002fb53bdefe28636cfdd6717aa6090616278f64f7ee76b3ecc',
                            host='52b0cc64-c3d4-4536-a276-d44bf2748330.br37s45d0p54n73ffbr0.databases.appdomain.cloud',
                            port='32283',
                            database='ijalti_covid')
'''



conexion = psycopg2.connect(user='postgres',
                            password='admin',
                            host='127.0.0.1',
                            port='5432',
                            database='coronavirus')
                            
filas = [[]]
filas.clear()
try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    
    #aqui se borra la data que esta en el historico de datos_secretaria_hist que tenga la misma fecha que esta en data_secretaria
    sql = 'DELETE FROM staging.datos_secretaria_hist drh WHERE drh.fecha_actualizacion in (SELECT distinct(dr.fecha_actualizacion) fecha FROM staging.datos_secretaria dr);'
    cursor1.execute(sql)
    
    #aqui se insertan los valores en la tabla de historicos
    sql = 'INSERT INTO staging.datos_secretaria_hist \
    (fecha_actualizacion, uid_registro, origen, sector, entidad_um, sexo, entidad_nac, entidad_res, municipio_res, tipo_paciente, fecha_ingreso, fecha_sintomas, fecha_def, intubado, neumonia, edad, nacionalidad, embarazo, habla_lengua_indig, diabetes, epoc, asma, inmusupr, hipertension, otra_com, cardiovascular, obesidad, renal_cronica, tabaquismo, otro_caso, resultado, migrante, pais_nacionalidad, pais_origen, uci) \
    SELECT fecha_actualizacion, uid_registro, origen, sector, entidad_um, sexo, entidad_nac, entidad_res, municipio_res, tipo_paciente, fecha_ingreso, fecha_sintomas, fecha_def, intubado, neumonia, edad, nacionalidad, embarazo, habla_lengua_indig, diabetes, epoc, asma, inmusupr, hipertension, otra_com, cardiovascular, obesidad, renal_cronica, tabaquismo, otro_caso, resultado, migrante, pais_nacionalidad, pais_origen, uci \
    FROM staging.datos_secretaria;'
    cursor1.execute(sql)
    
    #print('antes del truncate')
    sql = 'TRUNCATE TABLE staging.datos_secretaria_limpios'
    cursor1.execute(sql)
    #print('despues del truncate')

    
    
    #sql = 'SELECT FECHA_ACTUALIZACION,UID_REGISTRO,ORIGEN,SECTOR,ENTIDAD_UM,SEXO,ENTIDAD_NAC,ENTIDAD_RES,MUNICIPIO_RES,TIPO_PACIENTE,FECHA_INGRESO,FECHA_SINTOMAS,FECHA_DEF,INTUBADO,NEUMONIA,EDAD,NACIONALIDAD,EMBARAZO,HABLA_LENGUA_INDIG,DIABETES,EPOC,ASMA,INMUSUPR,HIPERTENSION,OTRA_COM,CARDIOVASCULAR,OBESIDAD,RENAL_CRONICA,TABAQUISMO,OTRO_CASO,RESULTADO,MIGRANTE,PAIS_NACIONALIDAD,PAIS_ORIGEN,UCI FROM staging.datos_secretaria;'
    sql = '''    SELECT \
    dr.FECHA_ACTUALIZACION, dr.UID_REGISTRO, COALESCE(catORI."CLAVE", 0) AS ORIGEN, COALESCE(catSEC."CLAVE", 0) AS SECTOR, \
    COALESCE(catENT_UM."CLAVE_ENTIDAD", 0) AS ENTIDAD_UM, COALESCE(catSEXO."CLAVE", 0) AS SEXO, COALESCE(catENT_NAC."CLAVE_ENTIDAD", 0) AS ENTIDAD_NAC, \
    COALESCE(catENT_RES."CLAVE_ENTIDAD", 0) AS ENTIDAD_RES, COALESCE(catMUN_RES."CLAVE_MUNICIPIO", 0) AS MUNICIPIO_RES, \
    COALESCE(catTP."CLAVE", 0) AS TIPO_PACIENTE, dr.FECHA_INGRESO, dr.FECHA_SINTOMAS, dr.FECHA_DEF, COALESCE(INTB."CLAVE", 0) AS INTUBADO, \
    COALESCE(NEUM."CLAVE", 0) AS NEUMONIA, dr.EDAD, COALESCE(catNAC."CLAVE", 0) AS NACIONALIDAD, COALESCE(EMBAR."CLAVE", 0) AS EMBARAZO, \
    COALESCE(HAIND."CLAVE", 0) AS HABLA_LENGUA_INDIG, COALESCE(DIAB."CLAVE", 0) AS DIABETES, COALESCE(EPOC."CLAVE", 0) AS EPOC, \
    COALESCE(ASMA."CLAVE", 0) AS ASMA, COALESCE(INMUNO."CLAVE", 0) AS INMUSUPR, COALESCE(HIPERT."CLAVE", 0) AS HIPERTENSION, \
    COALESCE(OCOM."CLAVE", 0) AS OTRA_COM, COALESCE(CARDIO."CLAVE", 0) AS CARDIOVASCULAR, COALESCE(OBESI."CLAVE", 0) AS OBESIDAD, \
    COALESCE(RENCR."CLAVE", 0) AS RENAL_CRONICA, COALESCE(TABAQ."CLAVE", 0) AS TABAQUISMO, COALESCE(OCACOR."CLAVE", 0) AS OTRO_CASO, \
    COALESCE(RESULT."CLAVE", 0) AS RESULTADO, COALESCE(MIGRA."CLAVE", 0) AS MIGRANTE, \
    COALESCE(PANAC."CLAVE_PAIS", 0) AS PAIS_NACIONALIDAD, \
    COALESCE(PAORG."CLAVE_PAIS", 0) AS  PAIS_ORIGEN, \
    COALESCE(UCI."CLAVE", 0) AS UCI \
    FROM staging.datos_secretaria dr \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_ORIGEN_SS") catORI ON dr.ORIGEN = catORI."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_ORIGEN_SS") catSEC ON dr.SECTOR = catSEC."CLAVE_STR" \
    LEFT OUTER JOIN (select clave_estado::varchar as "CLAVE_ENTIDAD_STR", clave_estado as "CLAVE_ENTIDAD" from staging."catalogo_ESTADOS") catENT_UM ON dr.entidad_um = catENT_UM."CLAVE_ENTIDAD_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SEXO") catSEXO ON dr.SEXO = catSEXO."CLAVE_STR" \
    LEFT OUTER JOIN (select clave_estado::varchar as "CLAVE_ENTIDAD_STR", clave_estado as "CLAVE_ENTIDAD" from staging."catalogo_ESTADOS") catENT_NAC ON dr.ENTIDAD_NAC = catENT_NAC."CLAVE_ENTIDAD_STR" \
    LEFT OUTER JOIN (select clave_estado::varchar as "CLAVE_ENTIDAD_STR", clave_estado as "CLAVE_ENTIDAD" from staging."catalogo_ESTADOS") catENT_RES ON dr.ENTIDAD_RES = catENT_RES."CLAVE_ENTIDAD_STR" \
    LEFT OUTER JOIN (select clave_estado::varchar as "CLAVE_ENTIDAD_STR", clave_estado as "CLAVE_ENTIDAD", codigo_municipio::varchar as "CLAVE_MUNICIPIO_STR", clave_municipio as "CLAVE_MUNICIPIO" from staging."catalogo_MUNICIPIOS") catMUN_RES ON dr.MUNICIPIO_RES = catMUN_RES."CLAVE_MUNICIPIO_STR" AND dr.ENTIDAD_RES = catMUN_RES."CLAVE_ENTIDAD_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_TIPO_PACIENTE_SS") catTP ON dr.TIPO_PACIENTE = catTP."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") INTB ON dr.INTUBADO = INTB."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") NEUM ON dr.NEUMONIA = NEUM."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_NACIONALIDAD") catNAC ON dr.NACIONALIDAD = catNAC."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") EMBAR ON dr.EMBARAZO = EMBAR."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") HAIND ON dr.HABLA_LENGUA_INDIG = HAIND."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") DIAB ON dr.DIABETES = DIAB."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") EPOC ON dr.EPOC = EPOC."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") ASMA ON dr.ASMA = ASMA."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") INMUNO ON dr.INMUSUPR = INMUNO."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") HIPERT ON dr.HIPERTENSION = HIPERT."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") OCOM ON dr.OTRA_COM = OCOM."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") CARDIO ON dr.CARDIOVASCULAR = CARDIO."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") OBESI ON dr.OBESIDAD = OBESI."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") RENCR ON dr.RENAL_CRONICA = RENCR."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") TABAQ ON dr.TABAQUISMO = TABAQ."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") OCACOR ON dr.OTRO_CASO = OCACOR."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_RESULTADO_SS") RESULT ON dr.RESULTADO = RESULT."CLAVE_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") MIGRA ON dr.MIGRANTE = MIGRA."CLAVE_STR" \
    LEFT OUTER JOIN (select clave_pais::varchar as "CLAVE_PAIS_STR", clave_pais as "CLAVE_PAIS" from staging."catalogo_PAISES") PAORG ON dr.pais_origen = PAORG."CLAVE_PAIS_STR" \
    LEFT OUTER JOIN (select clave_pais::varchar as "CLAVE_PAIS_STR", clave_pais as "CLAVE_PAIS" from staging."catalogo_PAISES") PANAC ON dr.pais_nacionalidad = PANAC."CLAVE_PAIS_STR" \
    LEFT OUTER JOIN (select "CLAVE"::varchar as "CLAVE_STR", "CLAVE" from staging."catalogo_SI_NO") UCI ON dr.UCI = UCI."CLAVE_STR";'''


    cursor1.execute(sql)
    registros = cursor1.fetchall()
    
    sql = 'INSERT INTO staging.datos_secretaria_limpios (FECHA_ACTUALIZACION,UID_REGISTRO,ORIGEN,SECTOR,ENTIDAD_UM,SEXO,ENTIDAD_NAC,ENTIDAD_RES,MUNICIPIO_RES,TIPO_PACIENTE,FECHA_INGRESO,FECHA_SINTOMAS,FECHA_DEF,INTUBADO,NEUMONIA,EDAD,NACIONALIDAD,EMBARAZO,HABLA_LENGUA_INDIG,DIABETES,EPOC,ASMA,INMUSUPR,HIPERTENSION,OTRA_COM,CARDIOVASCULAR,OBESIDAD,RENAL_CRONICA,TABAQUISMO,OTRO_CASO,RESULTADO,MIGRANTE,PAIS_NACIONALIDAD,PAIS_ORIGEN,UCI) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);'
    
    
    for fila in registros:
        campos = list(fila)
        
        if campos[0].find('/') >= 0:
            data_fecha = campos[0].replace('"', '').replace("'", "").split(sep='/')
        elif campos[0].find('-') >= 0:
            data_fecha = campos[0].replace('"', '').replace("'", "").split(sep='-')
        else:
            data_fecha = None
                
        try: 
            if len(data_fecha[2]) >= 4:
                anio = data_fecha[2].replace('"', '').replace("'", "")
                anio = int(anio[:4])
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[0].replace('"', '').replace("'", ""))
            elif len(data_fecha[2]) == 2:
                anio = int(data_fecha[0].replace('"', '').replace("'", ""))
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[2].replace('"', '').replace("'", ""))
            if data_fecha != None:
                FECHA_ACTUALIZACION = date(anio, mes, dia)
            else:
                FECHA_ACTUALIZACION = None
                
            campos[0] = FECHA_ACTUALIZACION
        except ValueError:
            campos[0] = None
        
        
        UID_REGISTRO = campos[1].replace('"', '').replace("'", "")
        campos[1] = UID_REGISTRO

        if campos[10].find('/') >= 0:
            data_fecha = campos[10].replace('"', '').replace("'", "").split(sep='/')
        elif campos[10].find('-') >= 0:
            data_fecha = campos[10].replace('"', '').replace("'", "").split(sep='-')
        else:
            data_fecha = None
                            
        try: 
            if len(data_fecha[2]) >= 4:
                anio = data_fecha[2].replace('"', '').replace("'", "")
                anio = int(anio[:4])
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[0].replace('"', '').replace("'", ""))
            elif len(data_fecha[2]) == 2:
                anio = int(data_fecha[0].replace('"', '').replace("'", ""))
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[2].replace('"', '').replace("'", ""))
            if data_fecha != None:
                FECHA_INGRESO = date(anio, mes, dia)
            else:
                FECHA_INGRESO = None
                
            campos[10] = FECHA_INGRESO
        except ValueError:
            campos[10] = None

        if campos[11].find('/') >= 0:
            data_fecha = campos[11].replace('"', '').replace("'", "").split(sep='/')
        elif campos[11].find('-') >= 0:
            data_fecha = campos[11].replace('"', '').replace("'", "").split(sep='-')
        else:
            data_fecha = None
                            
        try: 
            if len(data_fecha[2]) >= 4:
                anio = data_fecha[2].replace('"', '').replace("'", "")
                anio = int(anio[:4])
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[0].replace('"', '').replace("'", ""))
            elif len(data_fecha[2]) == 2:
                anio = int(data_fecha[0].replace('"', '').replace("'", ""))
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[2].replace('"', '').replace("'", ""))
            
            if data_fecha != None:
                FECHA_SINTOMAS = date(anio, mes, dia)
            else:
                FECHA_SINTOMAS = None
                
            campos[11] = FECHA_SINTOMAS
        except ValueError:
            campos[11] = None

        
        
        
        if campos[12].find('/') >= 0:
            data_fecha = campos[12].replace('"', '').replace("'", "").split(sep='/')
        elif campos[12].find('-') >= 0:
            data_fecha = campos[12].replace('"', '').replace("'", "").split(sep='-')
        else:
            data_fecha = None
                            
        try: 
            if len(data_fecha[2]) >= 4:
                anio = data_fecha[2].replace('"', '').replace("'", "")
                anio = int(anio[:4])
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[0].replace('"', '').replace("'", ""))
            elif len(data_fecha[2]) == 2:
                anio = int(data_fecha[0].replace('"', '').replace("'", ""))
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[2].replace('"', '').replace("'", ""))
            
            if data_fecha != None and anio != 9999:
                FECHA_DEF = date(anio, mes, dia)
            else:
                FECHA_DEF = None
                
            campos[12] = FECHA_DEF
        except ValueError:
            campos[12] = None

        #ojo
        #PAIS_NACIONALIDAD = campos[32].replace('"', '').replace("'", "")
        PAIS_NACIONALIDAD = campos[32]
        campos[32] = PAIS_NACIONALIDAD

        #ojo
        #PAIS_ORIGEN = campos[33].replace('"', '').replace("'", "")
        PAIS_ORIGEN = campos[33]
        campos[33] = PAIS_ORIGEN



        filas.append(campos)

        
        
    #print(filas)
    cursor1.executemany(sql, filas) #se usa executemany en lugar de execute y esto es porque lo que queremos agregar es la tupla
    conexion.commit()
    registros_insertados = cursor1.rowcount
    print(f'Registros insertados: {registros_insertados}')
    
except Exception as e:
    print("ocurrio un error en el insert")
finally:
    conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')

    
    