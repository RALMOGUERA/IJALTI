
import psycopg2
from datetime import datetime as dt, date



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
i = 0
try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    
    #aqui se borra la data de la fact
    sql = 'TRUNCATE TABLE staging.fact_radar;'
    print('antes de hacer el TRUNCATE de la fact de radar')
    cursor1.execute(sql)
    
    
    
    sql = '''SELECT catalogo, campo_clave_catalogo, columna_tabla_radar, rango_valores, columna_fact, tipo \
    FROM staging."catalogo_DIMS_MEDIDAS" \
    WHERE columna_tabla_radar IS NOT NULL AND columna_tabla_radar <> '';'''
   
    
    print('antes del select de datos_radar')

    cursor1.execute(sql)
    registros = cursor1.fetchall()
    print('despues del select')
     
    filas = [[]]
    filas.clear()
    
    campos_radar = ''
    campos_fact = ''
    left_joins = ''
    
    catalogo = ''
    campo_clave_catalogo = ''
    columna_tabla_radar = ''
    rango_valores = ''
    columna_fact = ''
    tipo = ''
    i = 0
    for fila in registros:
        campos = list(fila)
        #print(campos)
        catalogo = campos[0]
        campo_clave_catalogo = campos[1]
        columna_tabla_radar = campos[2]
        rango_valores = campos[3]
        columna_fact = campos[4]
        tipo = campos[5]
        i += 1  
        campos_fact += f'{columna_fact}, '
        #print(campos_fact)
        if rango_valores == 1:
            campos_radar += f'COALESCE(t{i}."CLAVE", 0), '
            left_joins += f'LEFT OUTER JOIN  staging."{catalogo}" t{i} ON ({columna_tabla_radar}  >= t{i}."MINIMO" AND {columna_tabla_radar} <= t{i}."MAXIMO") OR ({columna_tabla_radar}  >= t{i}."MINIMO" AND  t{i}."MAXIMO" IS NULL) '
        else:
            campos_radar += f'{columna_tabla_radar}, '
        
        
    #print(campos_fact)
    #print(campos_radar)
    #print(left_joins)
    
    sql = f'''insert into staging.fact_radar ({campos_fact} fecha_carga, cantidad) select {campos_radar} CURRENT_DATE, count(distinct id_tamiz) from staging.datos_radar_limpios {left_joins} group by {campos_radar} CURRENT_DATE;'''
    print(sql)
    cursor1.execute(sql)
    conexion.commit()
    registros_insertados = cursor1.rowcount
    print(f'Registros insertados: {registros_insertados}')
    
    #actualizacion de la cantidad acumulada
    sql = '''SELECT radar_hoy.ciudad, radar_hoy.colonia, radar_hoy.condicion_medica01, \
    radar_hoy.condicion_medica02, radar_hoy.condicion_medica03, radar_hoy.condicion_medica04, \
    radar_hoy.condicion_medica05, radar_hoy.condicion_medica06, radar_hoy.condicion_medica07, \
    radar_hoy.condicion_medica08, radar_hoy.condicion_medica09, radar_hoy.condicion_medica10, \
    radar_hoy.condicion_medica11, radar_hoy.condicion_medica12, radar_hoy.condicion_medica13, \
    radar_hoy.condicion_medica14, radar_hoy.condicion_medica15, radar_hoy.condicion_medica16, \
    radar_hoy.condicion_medica17, radar_hoy.condicion_medica18, radar_hoy.condicion_medica19, \
    radar_hoy.condicion_medica20, radar_hoy.condicion_medica21, radar_hoy.condicion_medica22, \
    radar_hoy.condicion_medica23, radar_hoy.condicion_medica24, radar_hoy.condicion_medica25, \
    radar_hoy.condicion_medica26, radar_hoy.condicion_medica27, radar_hoy.condicion_medica28, \
    radar_hoy.condicion_medica29, radar_hoy.condicion_medica30, radar_hoy.contacto_covid, \
    radar_hoy.cp, radar_hoy.estado, radar_hoy.fecha, radar_hoy.fecha_carga, radar_hoy.pregunta01, \
    radar_hoy.pregunta02, radar_hoy.pregunta03, radar_hoy.pregunta04, radar_hoy.pregunta05, \
    radar_hoy.pregunta06, radar_hoy.pregunta07, radar_hoy.pregunta08, radar_hoy.pregunta09, \
    radar_hoy.pregunta10, radar_hoy.pregunta11, radar_hoy.pregunta12, radar_hoy.pregunta13, \
    radar_hoy.pregunta14, radar_hoy.pregunta15, radar_hoy.pregunta16, radar_hoy.pregunta17, \
    radar_hoy.pregunta18, radar_hoy.pregunta19, radar_hoy.pregunta20, radar_hoy.pregunta21, \
    radar_hoy.pregunta22, radar_hoy.pregunta23, radar_hoy.pregunta24, radar_hoy.pregunta25, \
    radar_hoy.pregunta26, radar_hoy.pregunta27, radar_hoy.pregunta28, radar_hoy.pregunta29, \
    radar_hoy.pregunta30, radar_hoy.rango_edad, radar_hoy.resultado, radar_hoy.sexo, \
    radar_hoy.sintoma01, radar_hoy.sintoma02, radar_hoy.sintoma03, radar_hoy.sintoma04, \
    radar_hoy.sintoma05, radar_hoy.sintoma06, radar_hoy.sintoma07, radar_hoy.sintoma08, \
    radar_hoy.sintoma09, radar_hoy.sintoma10, radar_hoy.sintoma11, radar_hoy.sintoma12, \
    radar_hoy.sintoma13, radar_hoy.sintoma14, radar_hoy.sintoma15, radar_hoy.sintoma16, \
    radar_hoy.sintoma17, radar_hoy.sintoma18, radar_hoy.sintoma19, radar_hoy.sintoma20, \
    radar_hoy.sintoma21, radar_hoy.sintoma22, radar_hoy.sintoma23, radar_hoy.sintoma24, \
    radar_hoy.sintoma25, radar_hoy.sintoma26, radar_hoy.sintoma27, radar_hoy.sintoma28, \
    radar_hoy.sintoma29, radar_hoy.sintoma30, sum(coalesce(radar_pasado.cantidad, 0)) as cantidad_acumulada, \
    radar_hoy.municipio \
    FROM staging.fact_radar radar_hoy \
    INNER JOIN staging.fact_radar radar_pasado \
    ON \
    ((radar_hoy.ciudad IS NULL AND radar_pasado.ciudad IS NULL) OR (radar_hoy.ciudad = radar_pasado.ciudad)) \
    AND ((radar_hoy.colonia IS NULL AND radar_pasado.colonia IS NULL) OR (radar_hoy.colonia = radar_pasado.colonia)) \
    AND ((radar_hoy.condicion_medica01 IS NULL AND radar_pasado.condicion_medica01 IS NULL) OR (radar_hoy.condicion_medica01 = radar_pasado.condicion_medica01)) \
    AND ((radar_hoy.condicion_medica02 IS NULL AND radar_pasado.condicion_medica02 IS NULL) OR (radar_hoy.condicion_medica02 = radar_pasado.condicion_medica02)) \
    AND ((radar_hoy.condicion_medica03 IS NULL AND radar_pasado.condicion_medica03 IS NULL) OR (radar_hoy.condicion_medica03 = radar_pasado.condicion_medica03)) \
    AND ((radar_hoy.condicion_medica04 IS NULL AND radar_pasado.condicion_medica04 IS NULL) OR (radar_hoy.condicion_medica04 = radar_pasado.condicion_medica04)) \
    AND ((radar_hoy.condicion_medica05 IS NULL AND radar_pasado.condicion_medica05 IS NULL) OR (radar_hoy.condicion_medica05 = radar_pasado.condicion_medica05)) \
    AND ((radar_hoy.condicion_medica06 IS NULL AND radar_pasado.condicion_medica06 IS NULL) OR (radar_hoy.condicion_medica06 = radar_pasado.condicion_medica06)) \
    AND ((radar_hoy.condicion_medica07 IS NULL AND radar_pasado.condicion_medica07 IS NULL) OR (radar_hoy.condicion_medica07 = radar_pasado.condicion_medica07)) \
    AND ((radar_hoy.condicion_medica08 IS NULL AND radar_pasado.condicion_medica08 IS NULL) OR (radar_hoy.condicion_medica08 = radar_pasado.condicion_medica08)) \
    AND ((radar_hoy.condicion_medica09 IS NULL AND radar_pasado.condicion_medica09 IS NULL) OR (radar_hoy.condicion_medica09 = radar_pasado.condicion_medica09)) \
    AND ((radar_hoy.condicion_medica10 IS NULL AND radar_pasado.condicion_medica10 IS NULL) OR (radar_hoy.condicion_medica10 = radar_pasado.condicion_medica10)) \
    AND ((radar_hoy.condicion_medica11 IS NULL AND radar_pasado.condicion_medica11 IS NULL) OR (radar_hoy.condicion_medica11 = radar_pasado.condicion_medica11)) \
    AND ((radar_hoy.condicion_medica12 IS NULL AND radar_pasado.condicion_medica12 IS NULL) OR (radar_hoy.condicion_medica12 = radar_pasado.condicion_medica12)) \
    AND ((radar_hoy.condicion_medica13 IS NULL AND radar_pasado.condicion_medica13 IS NULL) OR (radar_hoy.condicion_medica13 = radar_pasado.condicion_medica13)) \
    AND ((radar_hoy.condicion_medica14 IS NULL AND radar_pasado.condicion_medica14 IS NULL) OR (radar_hoy.condicion_medica14 = radar_pasado.condicion_medica14)) \
    AND ((radar_hoy.condicion_medica15 IS NULL AND radar_pasado.condicion_medica15 IS NULL) OR (radar_hoy.condicion_medica15 = radar_pasado.condicion_medica15)) \
    AND ((radar_hoy.condicion_medica16 IS NULL AND radar_pasado.condicion_medica16 IS NULL) OR (radar_hoy.condicion_medica16 = radar_pasado.condicion_medica16)) \
    AND ((radar_hoy.condicion_medica17 IS NULL AND radar_pasado.condicion_medica17 IS NULL) OR (radar_hoy.condicion_medica17 = radar_pasado.condicion_medica17)) \
    AND ((radar_hoy.condicion_medica18 IS NULL AND radar_pasado.condicion_medica18 IS NULL) OR (radar_hoy.condicion_medica18 = radar_pasado.condicion_medica18)) \
    AND ((radar_hoy.condicion_medica19 IS NULL AND radar_pasado.condicion_medica19 IS NULL) OR (radar_hoy.condicion_medica19 = radar_pasado.condicion_medica19)) \
    AND ((radar_hoy.condicion_medica20 IS NULL AND radar_pasado.condicion_medica20 IS NULL) OR (radar_hoy.condicion_medica20 = radar_pasado.condicion_medica20)) \
    AND ((radar_hoy.condicion_medica21 IS NULL AND radar_pasado.condicion_medica21 IS NULL) OR (radar_hoy.condicion_medica21 = radar_pasado.condicion_medica21)) \
    AND ((radar_hoy.condicion_medica22 IS NULL AND radar_pasado.condicion_medica22 IS NULL) OR (radar_hoy.condicion_medica22 = radar_pasado.condicion_medica22)) \
    AND ((radar_hoy.condicion_medica23 IS NULL AND radar_pasado.condicion_medica23 IS NULL) OR (radar_hoy.condicion_medica23 = radar_pasado.condicion_medica23)) \
    AND ((radar_hoy.condicion_medica24 IS NULL AND radar_pasado.condicion_medica24 IS NULL) OR (radar_hoy.condicion_medica24 = radar_pasado.condicion_medica24)) \
    AND ((radar_hoy.condicion_medica25 IS NULL AND radar_pasado.condicion_medica25 IS NULL) OR (radar_hoy.condicion_medica25 = radar_pasado.condicion_medica25)) \
    AND ((radar_hoy.condicion_medica26 IS NULL AND radar_pasado.condicion_medica26 IS NULL) OR (radar_hoy.condicion_medica26 = radar_pasado.condicion_medica26)) \
    AND ((radar_hoy.condicion_medica27 IS NULL AND radar_pasado.condicion_medica27 IS NULL) OR (radar_hoy.condicion_medica27 = radar_pasado.condicion_medica27)) \
    AND ((radar_hoy.condicion_medica28 IS NULL AND radar_pasado.condicion_medica28 IS NULL) OR (radar_hoy.condicion_medica28 = radar_pasado.condicion_medica28)) \
    AND ((radar_hoy.condicion_medica29 IS NULL AND radar_pasado.condicion_medica29 IS NULL) OR (radar_hoy.condicion_medica29 = radar_pasado.condicion_medica29)) \
    AND ((radar_hoy.condicion_medica30 IS NULL AND radar_pasado.condicion_medica30 IS NULL) OR (radar_hoy.condicion_medica30 = radar_pasado.condicion_medica30)) \
    AND ((radar_hoy.contacto_covid IS NULL AND radar_pasado.contacto_covid IS NULL) OR (radar_hoy.contacto_covid = radar_pasado.contacto_covid)) \
    AND ((radar_hoy.cp IS NULL AND radar_pasado.cp IS NULL) OR (radar_hoy.cp = radar_pasado.cp)) \
    AND ((radar_hoy.estado IS NULL AND radar_pasado.estado IS NULL) OR (radar_hoy.estado = radar_pasado.estado)) \
    AND ((radar_hoy.fecha >= radar_pasado.fecha)) \
    AND (radar_hoy.fecha_carga = radar_pasado.fecha_carga) \
    AND ((radar_hoy.pregunta01 IS NULL AND radar_pasado.pregunta01 IS NULL) OR (radar_hoy.pregunta01 = radar_pasado.pregunta01)) \
    AND ((radar_hoy.pregunta02 IS NULL AND radar_pasado.pregunta02 IS NULL) OR (radar_hoy.pregunta02 = radar_pasado.pregunta02)) \
    AND ((radar_hoy.pregunta03 IS NULL AND radar_pasado.pregunta03 IS NULL) OR (radar_hoy.pregunta03 = radar_pasado.pregunta03)) \
    AND ((radar_hoy.pregunta04 IS NULL AND radar_pasado.pregunta04 IS NULL) OR (radar_hoy.pregunta04 = radar_pasado.pregunta04)) \
    AND ((radar_hoy.pregunta05 IS NULL AND radar_pasado.pregunta05 IS NULL) OR (radar_hoy.pregunta05 = radar_pasado.pregunta05)) \
    AND ((radar_hoy.pregunta06 IS NULL AND radar_pasado.pregunta06 IS NULL) OR (radar_hoy.pregunta06 = radar_pasado.pregunta06)) \
    AND ((radar_hoy.pregunta07 IS NULL AND radar_pasado.pregunta07 IS NULL) OR (radar_hoy.pregunta07 = radar_pasado.pregunta07)) \
    AND ((radar_hoy.pregunta08 IS NULL AND radar_pasado.pregunta08 IS NULL) OR (radar_hoy.pregunta08 = radar_pasado.pregunta08)) \
    AND ((radar_hoy.pregunta09 IS NULL AND radar_pasado.pregunta09 IS NULL) OR (radar_hoy.pregunta09 = radar_pasado.pregunta09)) \
    AND ((radar_hoy.pregunta10 IS NULL AND radar_pasado.pregunta10 IS NULL) OR (radar_hoy.pregunta10 = radar_pasado.pregunta10)) \
    AND ((radar_hoy.pregunta11 IS NULL AND radar_pasado.pregunta11 IS NULL) OR (radar_hoy.pregunta11 = radar_pasado.pregunta11)) \
    AND ((radar_hoy.pregunta12 IS NULL AND radar_pasado.pregunta12 IS NULL) OR (radar_hoy.pregunta12 = radar_pasado.pregunta12)) \
    AND ((radar_hoy.pregunta13 IS NULL AND radar_pasado.pregunta13 IS NULL) OR (radar_hoy.pregunta13 = radar_pasado.pregunta13)) \
    AND ((radar_hoy.pregunta14 IS NULL AND radar_pasado.pregunta14 IS NULL) OR (radar_hoy.pregunta14 = radar_pasado.pregunta14)) \
    AND ((radar_hoy.pregunta15 IS NULL AND radar_pasado.pregunta15 IS NULL) OR (radar_hoy.pregunta15 = radar_pasado.pregunta15)) \
    AND ((radar_hoy.pregunta16 IS NULL AND radar_pasado.pregunta16 IS NULL) OR (radar_hoy.pregunta16 = radar_pasado.pregunta16)) \
    AND ((radar_hoy.pregunta17 IS NULL AND radar_pasado.pregunta17 IS NULL) OR (radar_hoy.pregunta17 = radar_pasado.pregunta17)) \
    AND ((radar_hoy.pregunta18 IS NULL AND radar_pasado.pregunta18 IS NULL) OR (radar_hoy.pregunta18 = radar_pasado.pregunta18)) \
    AND ((radar_hoy.pregunta19 IS NULL AND radar_pasado.pregunta19 IS NULL) OR (radar_hoy.pregunta19 = radar_pasado.pregunta19)) \
    AND ((radar_hoy.pregunta20 IS NULL AND radar_pasado.pregunta20 IS NULL) OR (radar_hoy.pregunta20 = radar_pasado.pregunta20)) \
    AND ((radar_hoy.pregunta21 IS NULL AND radar_pasado.pregunta21 IS NULL) OR (radar_hoy.pregunta21 = radar_pasado.pregunta21)) \
    AND ((radar_hoy.pregunta22 IS NULL AND radar_pasado.pregunta22 IS NULL) OR (radar_hoy.pregunta22 = radar_pasado.pregunta22)) \
    AND ((radar_hoy.pregunta23 IS NULL AND radar_pasado.pregunta23 IS NULL) OR (radar_hoy.pregunta23 = radar_pasado.pregunta23)) \
    AND ((radar_hoy.pregunta24 IS NULL AND radar_pasado.pregunta24 IS NULL) OR (radar_hoy.pregunta24 = radar_pasado.pregunta24)) \
    AND ((radar_hoy.pregunta25 IS NULL AND radar_pasado.pregunta25 IS NULL) OR (radar_hoy.pregunta25 = radar_pasado.pregunta25)) \
    AND ((radar_hoy.pregunta26 IS NULL AND radar_pasado.pregunta26 IS NULL) OR (radar_hoy.pregunta26 = radar_pasado.pregunta26)) \
    AND ((radar_hoy.pregunta27 IS NULL AND radar_pasado.pregunta27 IS NULL) OR (radar_hoy.pregunta27 = radar_pasado.pregunta27)) \
    AND ((radar_hoy.pregunta28 IS NULL AND radar_pasado.pregunta28 IS NULL) OR (radar_hoy.pregunta28 = radar_pasado.pregunta28)) \
    AND ((radar_hoy.pregunta29 IS NULL AND radar_pasado.pregunta29 IS NULL) OR (radar_hoy.pregunta29 = radar_pasado.pregunta29)) \
    AND ((radar_hoy.pregunta30 IS NULL AND radar_pasado.pregunta30 IS NULL) OR (radar_hoy.pregunta30 = radar_pasado.pregunta30)) \
    AND ((radar_hoy.rango_edad IS NULL AND radar_pasado.rango_edad IS NULL) OR (radar_hoy.rango_edad = radar_pasado.rango_edad)) \
    AND ((radar_hoy.resultado IS NULL AND radar_pasado.resultado IS NULL) OR (radar_hoy.resultado = radar_pasado.resultado)) \
    AND ((radar_hoy.sexo IS NULL AND radar_pasado.sexo IS NULL) OR (radar_hoy.sexo = radar_pasado.sexo)) \
    AND ((radar_hoy.sintoma01 IS NULL AND radar_pasado.sintoma01 IS NULL) OR (radar_hoy.sintoma01 = radar_pasado.sintoma01)) \
    AND ((radar_hoy.sintoma02 IS NULL AND radar_pasado.sintoma02 IS NULL) OR (radar_hoy.sintoma02 = radar_pasado.sintoma02)) \
    AND ((radar_hoy.sintoma03 IS NULL AND radar_pasado.sintoma03 IS NULL) OR (radar_hoy.sintoma03 = radar_pasado.sintoma03)) \
    AND ((radar_hoy.sintoma04 IS NULL AND radar_pasado.sintoma04 IS NULL) OR (radar_hoy.sintoma04 = radar_pasado.sintoma04)) \
    AND ((radar_hoy.sintoma05 IS NULL AND radar_pasado.sintoma05 IS NULL) OR (radar_hoy.sintoma05 = radar_pasado.sintoma05)) \
    AND ((radar_hoy.sintoma06 IS NULL AND radar_pasado.sintoma06 IS NULL) OR (radar_hoy.sintoma06 = radar_pasado.sintoma06)) \
    AND ((radar_hoy.sintoma07 IS NULL AND radar_pasado.sintoma07 IS NULL) OR (radar_hoy.sintoma07 = radar_pasado.sintoma07)) \
    AND ((radar_hoy.sintoma08 IS NULL AND radar_pasado.sintoma08 IS NULL) OR (radar_hoy.sintoma08 = radar_pasado.sintoma08)) \
    AND ((radar_hoy.sintoma09 IS NULL AND radar_pasado.sintoma09 IS NULL) OR (radar_hoy.sintoma09 = radar_pasado.sintoma09)) \
    AND ((radar_hoy.sintoma10 IS NULL AND radar_pasado.sintoma10 IS NULL) OR (radar_hoy.sintoma10 = radar_pasado.sintoma10)) \
    AND ((radar_hoy.sintoma11 IS NULL AND radar_pasado.sintoma11 IS NULL) OR (radar_hoy.sintoma11 = radar_pasado.sintoma11)) \
    AND ((radar_hoy.sintoma12 IS NULL AND radar_pasado.sintoma12 IS NULL) OR (radar_hoy.sintoma12 = radar_pasado.sintoma12)) \
    AND ((radar_hoy.sintoma13 IS NULL AND radar_pasado.sintoma13 IS NULL) OR (radar_hoy.sintoma13 = radar_pasado.sintoma13)) \
    AND ((radar_hoy.sintoma14 IS NULL AND radar_pasado.sintoma14 IS NULL) OR (radar_hoy.sintoma14 = radar_pasado.sintoma14)) \
    AND ((radar_hoy.sintoma15 IS NULL AND radar_pasado.sintoma15 IS NULL) OR (radar_hoy.sintoma15 = radar_pasado.sintoma15)) \
    AND ((radar_hoy.sintoma16 IS NULL AND radar_pasado.sintoma16 IS NULL) OR (radar_hoy.sintoma16 = radar_pasado.sintoma16)) \
    AND ((radar_hoy.sintoma17 IS NULL AND radar_pasado.sintoma17 IS NULL) OR (radar_hoy.sintoma17 = radar_pasado.sintoma17)) \
    AND ((radar_hoy.sintoma18 IS NULL AND radar_pasado.sintoma18 IS NULL) OR (radar_hoy.sintoma18 = radar_pasado.sintoma18)) \
    AND ((radar_hoy.sintoma19 IS NULL AND radar_pasado.sintoma19 IS NULL) OR (radar_hoy.sintoma19 = radar_pasado.sintoma19)) \
    AND ((radar_hoy.sintoma20 IS NULL AND radar_pasado.sintoma20 IS NULL) OR (radar_hoy.sintoma20 = radar_pasado.sintoma20)) \
    AND ((radar_hoy.sintoma21 IS NULL AND radar_pasado.sintoma21 IS NULL) OR (radar_hoy.sintoma21 = radar_pasado.sintoma21)) \
    AND ((radar_hoy.sintoma22 IS NULL AND radar_pasado.sintoma22 IS NULL) OR (radar_hoy.sintoma22 = radar_pasado.sintoma22)) \
    AND ((radar_hoy.sintoma23 IS NULL AND radar_pasado.sintoma23 IS NULL) OR (radar_hoy.sintoma23 = radar_pasado.sintoma23)) \
    AND ((radar_hoy.sintoma24 IS NULL AND radar_pasado.sintoma24 IS NULL) OR (radar_hoy.sintoma24 = radar_pasado.sintoma24)) \
    AND ((radar_hoy.sintoma25 IS NULL AND radar_pasado.sintoma25 IS NULL) OR (radar_hoy.sintoma25 = radar_pasado.sintoma25)) \
    AND ((radar_hoy.sintoma26 IS NULL AND radar_pasado.sintoma26 IS NULL) OR (radar_hoy.sintoma26 = radar_pasado.sintoma26)) \
    AND ((radar_hoy.sintoma27 IS NULL AND radar_pasado.sintoma27 IS NULL) OR (radar_hoy.sintoma27 = radar_pasado.sintoma27)) \
    AND ((radar_hoy.sintoma28 IS NULL AND radar_pasado.sintoma28 IS NULL) OR (radar_hoy.sintoma28 = radar_pasado.sintoma28)) \
    AND ((radar_hoy.sintoma29 IS NULL AND radar_pasado.sintoma29 IS NULL) OR (radar_hoy.sintoma29 = radar_pasado.sintoma29)) \
    AND ((radar_hoy.sintoma30 IS NULL AND radar_pasado.sintoma30 IS NULL) OR (radar_hoy.sintoma30 = radar_pasado.sintoma30)) \
    AND ((radar_hoy.municipio IS NULL AND radar_pasado.municipio IS NULL) OR (radar_hoy.municipio = radar_pasado.municipio)) \
    GROUP BY \
    radar_hoy.ciudad, radar_hoy.colonia, radar_hoy.condicion_medica01, radar_hoy.condicion_medica02, radar_hoy.condicion_medica03, \
    radar_hoy.condicion_medica04, radar_hoy.condicion_medica05, radar_hoy.condicion_medica06, radar_hoy.condicion_medica07, \
    radar_hoy.condicion_medica08, radar_hoy.condicion_medica09, radar_hoy.condicion_medica10, radar_hoy.condicion_medica11, \
    radar_hoy.condicion_medica12, radar_hoy.condicion_medica13, radar_hoy.condicion_medica14, radar_hoy.condicion_medica15, \
    radar_hoy.condicion_medica16, radar_hoy.condicion_medica17, radar_hoy.condicion_medica18, radar_hoy.condicion_medica19, \
    radar_hoy.condicion_medica20, radar_hoy.condicion_medica21, radar_hoy.condicion_medica22, radar_hoy.condicion_medica23, \
    radar_hoy.condicion_medica24, radar_hoy.condicion_medica25, radar_hoy.condicion_medica26, radar_hoy.condicion_medica27, \
    radar_hoy.condicion_medica28, radar_hoy.condicion_medica29, radar_hoy.condicion_medica30, radar_hoy.contacto_covid, radar_hoy.cp, \
    radar_hoy.estado, radar_hoy.fecha, radar_hoy.fecha_carga, radar_hoy.pregunta01, radar_hoy.pregunta02, radar_hoy.pregunta03, \
    radar_hoy.pregunta04, radar_hoy.pregunta05, radar_hoy.pregunta06, radar_hoy.pregunta07, radar_hoy.pregunta08, radar_hoy.pregunta09, \
    radar_hoy.pregunta10, radar_hoy.pregunta11, radar_hoy.pregunta12, radar_hoy.pregunta13, radar_hoy.pregunta14, radar_hoy.pregunta15, \
    radar_hoy.pregunta16, radar_hoy.pregunta17, radar_hoy.pregunta18, radar_hoy.pregunta19, radar_hoy.pregunta20, radar_hoy.pregunta21, \
    radar_hoy.pregunta22, radar_hoy.pregunta23, radar_hoy.pregunta24, radar_hoy.pregunta25, radar_hoy.pregunta26, radar_hoy.pregunta27, \
    radar_hoy.pregunta28, radar_hoy.pregunta29, radar_hoy.pregunta30, radar_hoy.rango_edad, radar_hoy.resultado, radar_hoy.sexo, \
    radar_hoy.sintoma01, radar_hoy.sintoma02, radar_hoy.sintoma03, radar_hoy.sintoma04, radar_hoy.sintoma05, radar_hoy.sintoma06, \
    radar_hoy.sintoma07, radar_hoy.sintoma08, radar_hoy.sintoma09, radar_hoy.sintoma10, radar_hoy.sintoma11, radar_hoy.sintoma12, \
    radar_hoy.sintoma13, radar_hoy.sintoma14, radar_hoy.sintoma15, radar_hoy.sintoma16, radar_hoy.sintoma17, radar_hoy.sintoma18, \
    radar_hoy.sintoma19, radar_hoy.sintoma20, radar_hoy.sintoma21, radar_hoy.sintoma22, radar_hoy.sintoma23, radar_hoy.sintoma24, \
    radar_hoy.sintoma25, radar_hoy.sintoma26, radar_hoy.sintoma27, radar_hoy.sintoma28, radar_hoy.sintoma29, radar_hoy.sintoma30, \
    radar_hoy.municipio;'''

    cursor1.execute(sql)
    registros = cursor1.fetchall()
    print('despues del select del acumulado')
    
    for fila in registros:
        campos = list(fila)

        ciudad = campos[0]
        colonia = campos[1]
        condicion_medica01 = campos[2]
        condicion_medica02 = campos[3]
        condicion_medica03 = campos[4]
        condicion_medica04 = campos[5]
        condicion_medica05 = campos[6]
        condicion_medica06 = campos[7]
        condicion_medica07 = campos[8]
        condicion_medica08 = campos[9]
        condicion_medica09 = campos[10]
        condicion_medica10 = campos[11]
        condicion_medica11 = campos[12]
        condicion_medica12 = campos[13]
        condicion_medica13 = campos[14]
        condicion_medica14 = campos[15]
        condicion_medica15 = campos[16]
        condicion_medica16 = campos[17]
        condicion_medica17 = campos[18]
        condicion_medica18 = campos[19]
        condicion_medica19 = campos[20]
        condicion_medica20 = campos[21]
        condicion_medica21 = campos[22]
        condicion_medica22 = campos[23]
        condicion_medica23 = campos[24]
        condicion_medica24 = campos[25]
        condicion_medica25 = campos[26]
        condicion_medica26 = campos[27]
        condicion_medica27 = campos[28]
        condicion_medica28 = campos[29]
        condicion_medica29 = campos[30]
        condicion_medica30 = campos[31]
        contacto_covid = campos[32]
        cp = campos[33]
        estado = campos[34]
        fecha = campos[35]
        fecha_carga = campos[36]
        pregunta01 = campos[37]
        pregunta02 = campos[38]
        pregunta03 = campos[39]
        pregunta04 = campos[40]
        pregunta05 = campos[41]
        pregunta06 = campos[42]
        pregunta07 = campos[43]
        pregunta08 = campos[44]
        pregunta09 = campos[45]
        pregunta10 = campos[46]
        pregunta11 = campos[47]
        pregunta12 = campos[48]
        pregunta13 = campos[49]
        pregunta14 = campos[50]
        pregunta15 = campos[51]
        pregunta16 = campos[52]
        pregunta17 = campos[53]
        pregunta18 = campos[54]
        pregunta19 = campos[55]
        pregunta20 = campos[56]
        pregunta21 = campos[57]
        pregunta22 = campos[58]
        pregunta23 = campos[59]
        pregunta24 = campos[60]
        pregunta25 = campos[61]
        pregunta26 = campos[62]
        pregunta27 = campos[63]
        pregunta28 = campos[64]
        pregunta29 = campos[65]
        pregunta30 = campos[66]
        rango_edad = campos[67]
        resultado = campos[68]
        sexo = campos[69]
        sintoma01 = campos[70]
        sintoma02 = campos[71]
        sintoma03 = campos[72]
        sintoma04 = campos[73]
        sintoma05 = campos[74]
        sintoma06 = campos[75]
        sintoma07 = campos[76]
        sintoma08 = campos[77]
        sintoma09 = campos[78]
        sintoma10 = campos[79]
        sintoma11 = campos[80]
        sintoma12 = campos[81]
        sintoma13 = campos[82]
        sintoma14 = campos[83]
        sintoma15 = campos[84]
        sintoma16 = campos[85]
        sintoma17 = campos[86]
        sintoma18 = campos[87]
        sintoma19 = campos[88]
        sintoma20 = campos[89]
        sintoma21 = campos[90]
        sintoma22 = campos[91]
        sintoma23 = campos[92]
        sintoma24 = campos[93]
        sintoma25 = campos[94]
        sintoma26 = campos[95]
        sintoma27 = campos[96]
        sintoma28 = campos[97]
        sintoma29 = campos[98]
        sintoma30 = campos[99]
        cantidad_acumulada = campos[100]
        municipio = campos[101]
        
        sql = f'UPDATE staging.fact_radar SET cantidad_acumulada = {cantidad_acumulada} WHERE '
        
        if ciudad != None:
            sql += f'ciudad = {ciudad} '
        else:
            sql += 'ciudad is null '

        if colonia != None:
            sql += f'AND colonia = {colonia} '
        else:
            sql += 'AND colonia is null '

        if condicion_medica01 != None:
            sql += f'AND condicion_medica01 = {condicion_medica01} '
        else:
            sql += 'AND condicion_medica01 is null '

        if condicion_medica02 != None:
            sql += f'AND condicion_medica02 = {condicion_medica02} '
        else:
            sql += 'AND condicion_medica02 is null '

        if condicion_medica03 != None:
            sql += f'AND condicion_medica03 = {condicion_medica03} '
        else:
            sql += 'AND condicion_medica03 is null '

        if condicion_medica04 != None:
            sql += f'AND condicion_medica04 = {condicion_medica04} '
        else:
            sql += 'AND condicion_medica04 is null '

        if condicion_medica05 != None:
            sql += f'AND condicion_medica05 = {condicion_medica05} '
        else:
            sql += 'AND condicion_medica05 is null '

        if condicion_medica06 != None:
            sql += f'AND condicion_medica06 = {condicion_medica06} '
        else:
            sql += 'AND condicion_medica06 is null '
        if condicion_medica07 != None:
            sql += f'AND condicion_medica07 = {condicion_medica07} '
        else:
            sql += 'AND condicion_medica07 is null '
        if condicion_medica08 != None:
            sql += f'AND condicion_medica08 = {condicion_medica08} '
        else:
            sql += 'AND condicion_medica08 is null '
        if condicion_medica09 != None:
            sql += f'AND condicion_medica09 = {condicion_medica09} '
        else:
            sql += 'AND condicion_medica09 is null '
        if condicion_medica10 != None:
            sql += f'AND condicion_medica10 = {condicion_medica10} '
        else:
            sql += 'AND condicion_medica10 is null '
        if condicion_medica11 != None:
            sql += f'AND condicion_medica11 = {condicion_medica11} '
        else:
            sql += 'AND condicion_medica11 is null '
        if condicion_medica12 != None:
            sql += f'AND condicion_medica12 = {condicion_medica12} '
        else:
            sql += 'AND condicion_medica12 is null '
        if condicion_medica13 != None:
            sql += f'AND condicion_medica13 = {condicion_medica13} '
        else:
            sql += 'AND condicion_medica13 is null '
        if condicion_medica14 != None:
            sql += f'AND condicion_medica14 = {condicion_medica14} '
        else:
            sql += 'AND condicion_medica14 is null '
        if condicion_medica15 != None:
            sql += f'AND condicion_medica15 = {condicion_medica15} '
        else:
            sql += 'AND condicion_medica15 is null '
        if condicion_medica16 != None:
            sql += f'AND condicion_medica16 = {condicion_medica16} '
        else:
            sql += 'AND condicion_medica16 is null '
        if condicion_medica17 != None:
            sql += f'AND condicion_medica17 = {condicion_medica17} '
        else:
            sql += 'AND condicion_medica17 is null '
        if condicion_medica18 != None:
            sql += f'AND condicion_medica18 = {condicion_medica18} '
        else:
            sql += 'AND condicion_medica18 is null '
        if condicion_medica19 != None:
            sql += f'AND condicion_medica19 = {condicion_medica19} '
        else:
            sql += 'AND condicion_medica19 is null '
        if condicion_medica20 != None:
            sql += f'AND condicion_medica20 = {condicion_medica20} '
        else:
            sql += 'AND condicion_medica20 is null '
        if condicion_medica21 != None:
            sql += f'AND condicion_medica21 = {condicion_medica21} '
        else:
            sql += 'AND condicion_medica21 is null '
        if condicion_medica22 != None:
            sql += f'AND condicion_medica22 = {condicion_medica22} '
        else:
            sql += 'AND condicion_medica22 is null '
        if condicion_medica23 != None:
            sql += f'AND condicion_medica23 = {condicion_medica23} '
        else:
            sql += 'AND condicion_medica23 is null '
        if condicion_medica24 != None:
            sql += f'AND condicion_medica24 = {condicion_medica24} '
        else:
            sql += 'AND condicion_medica24 is null '
        if condicion_medica25 != None:
            sql += f'AND condicion_medica25 = {condicion_medica25} '
        else:
            sql += 'AND condicion_medica25 is null '
        if condicion_medica26 != None:
            sql += f'AND condicion_medica26 = {condicion_medica26} '
        else:
            sql += 'AND condicion_medica26 is null '
        if condicion_medica27 != None:
            sql += f'AND condicion_medica27 = {condicion_medica27} '
        else:
            sql += 'AND condicion_medica27 is null '
        if condicion_medica28 != None:
            sql += f'AND condicion_medica28 = {condicion_medica28} '
        else:
            sql += 'AND condicion_medica28 is null '
        if condicion_medica29 != None:
            sql += f'AND condicion_medica29 = {condicion_medica29} '
        else:
            sql += 'AND condicion_medica29 is null '
        if condicion_medica30 != None:
            sql += f'AND condicion_medica30 = {condicion_medica30} '
        else:
            sql += 'AND condicion_medica30 is null '
        if contacto_covid != None:
            sql += f'AND contacto_covid = {contacto_covid} '
        else:
            sql += 'AND contacto_covid is null '
        if cp != None:
            sql += f"AND cp = '{cp}' "
        else:
            sql += 'AND cp is null '
        if estado != None:
            sql += f'AND estado = {estado} '
        else:
            sql += 'AND estado is null '
        if fecha != None:
            sql += f"AND fecha = to_date('{fecha}', 'YYYY-MM-DD') "
        else:
            sql += 'AND fecha is null '
        if fecha_carga != None:
            sql += f"AND fecha_carga = to_date('{fecha_carga}', 'YYYY-MM-DD') "
        else:
            sql += 'AND fecha_carga is null '
        if pregunta01 != None:
            sql += f'AND pregunta01 = {pregunta01} '
        else:
            sql += 'AND pregunta01 is null '
        if pregunta02 != None:
            sql += f'AND pregunta02 = {pregunta02} '
        else:
            sql += 'AND pregunta02 is null '
        if pregunta03 != None:
            sql += f'AND pregunta03 = {pregunta03} '
        else:
            sql += 'AND pregunta03 is null '
        if pregunta04 != None:
            sql += f'AND pregunta04 = {pregunta04} '
        else:
            sql += 'AND pregunta04 is null '
        if pregunta05 != None:
            sql += f'AND pregunta05 = {pregunta05} '
        else:
            sql += 'AND pregunta05 is null '
        if pregunta06 != None:
            sql += f'AND pregunta06 = {pregunta06} '
        else:
            sql += 'AND pregunta06 is null '
        if pregunta07 != None:
            sql += f'AND pregunta07 = {pregunta07} '
        else:
            sql += 'AND pregunta07 is null '
        if pregunta08 != None:
            sql += f'AND pregunta08 = {pregunta08} '
        else:
            sql += 'AND pregunta08 is null '
        if pregunta09 != None:
            sql += f'AND pregunta09 = {pregunta09} '
        else:
            sql += 'AND pregunta09 is null '
        if pregunta10 != None:
            sql += f'AND pregunta10 = {pregunta10} '
        else:
            sql += 'AND pregunta10 is null '
        if pregunta11 != None:
            sql += f'AND pregunta11 = {pregunta11} '
        else:
            sql += 'AND pregunta11 is null '
        if pregunta12 != None:
            sql += f'AND pregunta12 = {pregunta12} '
        else:
            sql += 'AND pregunta12 is null '
        if pregunta13 != None:
            sql += f'AND pregunta13 = {pregunta13} '
        else:
            sql += 'AND pregunta13 is null '
        if pregunta14 != None:
            sql += f'AND pregunta14 = {pregunta14} '
        else:
            sql += 'AND pregunta14 is null '
        if pregunta15 != None:
            sql += f'AND pregunta15 = {pregunta15} '
        else:
            sql += 'AND pregunta15 is null '
        if pregunta16 != None:
            sql += f'AND pregunta16 = {pregunta16} '
        else:
            sql += 'AND pregunta16 is null '
        if pregunta17 != None:
            sql += f'AND pregunta17 = {pregunta17} '
        else:
            sql += 'AND pregunta17 is null '
        if pregunta18 != None:
            sql += f'AND pregunta18 = {pregunta18} '
        else:
            sql += 'AND pregunta18 is null '
        if pregunta19 != None:
            sql += f'AND pregunta19 = {pregunta19} '
        else:
            sql += 'AND pregunta19 is null '
        if pregunta20 != None:
            sql += f'AND pregunta20 = {pregunta20} '
        else:
            sql += 'AND pregunta20 is null '
        if pregunta21 != None:
            sql += f'AND pregunta21 = {pregunta21} '
        else:
            sql += 'AND pregunta21 is null '
        if pregunta22 != None:
            sql += f'AND pregunta22 = {pregunta22} '
        else:
            sql += 'AND pregunta22 is null '
        if pregunta23 != None:
            sql += f'AND pregunta23 = {pregunta23} '
        else:
            sql += 'AND pregunta23 is null '
        if pregunta24 != None:
            sql += f'AND pregunta24 = {pregunta24} '
        else:
            sql += 'AND pregunta24 is null '
        if pregunta25 != None:
            sql += f'AND pregunta25 = {pregunta25} '
        else:
            sql += 'AND pregunta25 is null '
        if pregunta26 != None:
            sql += f'AND pregunta26 = {pregunta26} '
        else:
            sql += 'AND pregunta26 is null '
        if pregunta27 != None:
            sql += f'AND pregunta27 = {pregunta27} '
        else:
            sql += 'AND pregunta27 is null '
        if pregunta28 != None:
            sql += f'AND pregunta28 = {pregunta28} '
        else:
            sql += 'AND pregunta28 is null '
        if pregunta29 != None:
            sql += f'AND pregunta29 = {pregunta29} '
        else:
            sql += 'AND pregunta29 is null '
        if pregunta30 != None:
            sql += f'AND pregunta30 = {pregunta30} '
        else:
            sql += 'AND pregunta30 is null '
        if rango_edad != None:
            sql += f'AND rango_edad = {rango_edad} '
        else:
            sql += 'AND rango_edad is null '
        if resultado != None:
            sql += f'AND resultado = {resultado} '
        else:
            sql += 'AND resultado is null '
        if sexo != None:
            sql += f'AND sexo = {sexo} '
        else:
            sql += 'AND sexo is null '
        if sintoma01 != None:
            sql += f'AND sintoma01 = {sintoma01} '
        else:
            sql += 'AND sintoma01 is null '
        if sintoma02 != None:
            sql += f'AND sintoma02 = {sintoma02} '
        else:
            sql += 'AND sintoma02 is null '
        if sintoma03 != None:
            sql += f'AND sintoma03 = {sintoma03} '
        else:
            sql += 'AND sintoma03 is null '
        if sintoma04 != None:
            sql += f'AND sintoma04 = {sintoma04} '
        else:
            sql += 'AND sintoma04 is null '
        if sintoma05 != None:
            sql += f'AND sintoma05 = {sintoma05} '
        else:
            sql += 'AND sintoma05 is null '
        if sintoma06 != None:
            sql += f'AND sintoma06 = {sintoma06} '
        else:
            sql += 'AND sintoma06 is null '
        if sintoma07 != None:
            sql += f'AND sintoma07 = {sintoma07} '
        else:
            sql += 'AND sintoma07 is null '
        if sintoma08 != None:
            sql += f'AND sintoma08 = {sintoma08} '
        else:
            sql += 'AND sintoma08 is null '
        if sintoma09 != None:
            sql += f'AND sintoma09 = {sintoma09} '
        else:
            sql += 'AND sintoma09 is null '
        if sintoma10 != None:
            sql += f'AND sintoma10 = {sintoma10} '
        else:
            sql += 'AND sintoma10 is null '
        if sintoma11 != None:
            sql += f'AND sintoma11 = {sintoma11} '
        else:
            sql += 'AND sintoma11 is null '
        if sintoma12 != None:
            sql += f'AND sintoma12 = {sintoma12} '
        else:
            sql += 'AND sintoma12 is null '
        if sintoma13 != None:
            sql += f'AND sintoma13 = {sintoma13} '
        else:
            sql += 'AND sintoma13 is null '
        if sintoma14 != None:
            sql += f'AND sintoma14 = {sintoma14} '
        else:
            sql += 'AND sintoma14 is null '
        if sintoma15 != None:
            sql += f'AND sintoma15 = {sintoma15} '
        else:
            sql += 'AND sintoma15 is null '
        if sintoma16 != None:
            sql += f'AND sintoma16 = {sintoma16} '
        else:
            sql += 'AND sintoma16 is null '
        if sintoma17 != None:
            sql += f'AND sintoma17 = {sintoma17} '
        else:
            sql += 'AND sintoma17 is null '
        if sintoma18 != None:
            sql += f'AND sintoma18 = {sintoma18} '
        else:
            sql += 'AND sintoma18 is null '
        if sintoma19 != None:
            sql += f'AND sintoma19 = {sintoma19} '
        else:
            sql += 'AND sintoma19 is null '
        if sintoma20 != None:
            sql += f'AND sintoma20 = {sintoma20} '
        else:
            sql += 'AND sintoma20 is null '
        if sintoma21 != None:
            sql += f'AND sintoma21 = {sintoma21} '
        else:
            sql += 'AND sintoma21 is null '
        if sintoma22 != None:
            sql += f'AND sintoma22 = {sintoma22} '
        else:
            sql += 'AND sintoma22 is null '
        if sintoma23 != None:
            sql += f'AND sintoma23 = {sintoma23} '
        else:
            sql += 'AND sintoma23 is null '
        if sintoma24 != None:
            sql += f'AND sintoma24 = {sintoma24} '
        else:
            sql += 'AND sintoma24 is null '
        if sintoma25 != None:
            sql += f'AND sintoma25 = {sintoma25} '
        else:
            sql += 'AND sintoma25 is null '
        if sintoma26 != None:
            sql += f'AND sintoma26 = {sintoma26} '
        else:
            sql += 'AND sintoma26 is null '
        if sintoma27 != None:
            sql += f'AND sintoma27 = {sintoma27} '
        else:
            sql += 'AND sintoma27 is null '
        if sintoma28 != None:
            sql += f'AND sintoma28 = {sintoma28} '
        else:
            sql += 'AND sintoma28 is null '
        if sintoma29 != None:
            sql += f'AND sintoma29 = {sintoma29} '
        else:
            sql += 'AND sintoma29 is null '
        if sintoma30 != None:
            sql += f'AND sintoma30 = {sintoma30} '
        else:
            sql += 'AND sintoma30 is null '
        if municipio != None:
            sql += f' AND municipio = {municipio};'
        else:
            sql += ' AND municipio is null;'
        
        #print(sql)
        cursor1.execute(sql)
        #print(sql)

    
except Exception as e:
    print("ocurrio un error en el insert")
    print(e)
finally:
    conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')

    
    