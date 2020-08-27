
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
    sql = 'TRUNCATE TABLE staging.fact_secretaria;'
    print('antes de hacer el TRUNCATE de la fact de secretaria')
    
    cursor1.execute(sql)
    
    
    
    sql = '''SELECT catalogo, campo_clave_catalogo, columna_tabla_secretaria, rango_valores, columna_fact, tipo \
    FROM staging."catalogo_DIMS_MEDIDAS" \
    WHERE columna_tabla_secretaria IS NOT NULL AND columna_tabla_secretaria <> '';'''
   
    
    print('antes del select de datos_secretaria')

    cursor1.execute(sql)
    registros = cursor1.fetchall()
    print('despues del select')
     
    filas = [[]]
    filas.clear()
    
    campos_secretaria = ''
    campos_fact = ''
    left_joins = ''
    
    catalogo = ''
    campo_clave_catalogo = ''
    columna_tabla_secretaria = ''
    rango_valores = ''
    columna_fact = ''
    tipo = ''
    i = 0
    for fila in registros:
        campos = list(fila)
        #print(campos)
        catalogo = campos[0]
        campo_clave_catalogo = campos[1]
        columna_tabla_secretaria = campos[2]
        rango_valores = campos[3]
        columna_fact = campos[4]
        tipo = campos[5]
        i += 1  
        campos_fact += f'{columna_fact}, '
        #print(campos_fact)
        if rango_valores == 1:
            campos_secretaria += f'COALESCE(t{i}."CLAVE", 0), '
            left_joins += f'LEFT OUTER JOIN  staging."{catalogo}" t{i} ON ({columna_tabla_secretaria}  >= t{i}."MINIMO" AND {columna_tabla_secretaria} <= t{i}."MAXIMO") OR ({columna_tabla_secretaria}  >= t{i}."MINIMO" AND  t{i}."MAXIMO" IS NULL) '
        else:
            campos_secretaria += f'{columna_tabla_secretaria}, '
        
        
    #print(campos_fact)
    #print(campos_secretaria)
    #print(left_joins)
    
    sql = f'''insert into staging.fact_secretaria ({campos_fact} fecha_carga, cantidad, cantidad_muertos) select {campos_secretaria} CURRENT_DATE, count(distinct uid_registro), sum(case when fecha_def is null then 0 else 1 end) from staging.datos_secretaria_limpios {left_joins} group by {campos_secretaria} CURRENT_DATE;'''
    #print(sql)
    
    cursor1.execute(sql)
    print('despue del insert inicial en la tabla de la fact_secretaria... ')
    conexion.commit()
    registros_insertados = cursor1.rowcount
    print(f'Registros insertados: {registros_insertados}')
    
    
except Exception as e:
    print("ocurrio un error en el insert")
    print(e)
finally:
    conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')

    
    