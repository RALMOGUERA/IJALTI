import psycopg2
from datetime import datetime as dt, date
import requests

#aca se conecta a la DB

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
'''

filas = [[]]
filas.clear()
i = 0
try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    
    
    #aqui se busca la infromacion del id_tamiz, del resultado del laboratorio y de la direccion de la persona para pasarle luego la funcion de geolocalizacion  
    sql = '''select drl.id_tamiz, drl.fecha_muestra, lab."DESCRIPCION" as resultado, \
    coalesce(drl.calle, '') as calle, coalesce(drl.num_ext, '') as numero_exterior, \
    coalesce(col.colonia_str, '') as colonia, coalesce(cp.codigo_postal, '') as codigo_postal, \
    coalesce(mun.municipio_str, '') as municipio, coalesce(esta.estado_str, '') as estado \
    from staging.datos_radar_limpios drl \
    inner join staging."catalogo_RESULTADO_LABORATORIO" lab \
    on lab."CLAVE" = drl.resultado_laboratorio \
    left outer join staging."catalogo_COLONIAS" col \
    on col.clave_colonia = drl.clave_colonia \
    left outer join staging."catalogo_CODIGOS_POSTALES" cp \
    on cp.codigo_postal = drl.cp \
    left outer join staging."catalogo_MUNICIPIOS" mun \
    on mun.clave_municipio = drl.clave_municipio \
    left outer join staging."catalogo_ESTADOS" esta \
    on esta.clave_estado = drl.clave_estado \
    where drl.id_tamiz not in (select geo.id_tamiz from staging.geolocalizacion2 geo);'''
        
    cursor1.execute(sql)
    
    ubicaciones = list(cursor1.fetchall())


    sql = 'INSERT INTO staging.geolocalizacion2 \
            (resultado_laboratorio, geo, fecha, id_tamiz) \
            VALUES (%s, %s, %s, %s);'
    
    
    for fila in ubicaciones:
        
        id_tamiz = fila[0]
        fecha_muestra = fila[1]
        resultado = fila[2]
        calle = fila[3]
        numero_exterior = fila[4]
        colonia = fila[5]
        codigo_postal = fila[6]
        municipio = fila[7]
        estado = fila[8]
        
        direccion = calle +' ' + numero_exterior + ', ' + colonia + ', ' + codigo_postal + ' ' + municipio + ', ' + estado        
        #direccion = "MARIA SALCEDO 720, EUROPA, 44810 GUADALAJARA, JALISCO"
        params = {  "f": 'json',"singleLine": direccion,}

        response = requests.get(f'https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates', 
        params=params)

        
        coordenadas = []
        coordenadas.clear()
        if response:
            #Esto genera una lista con longitud - latitud
            coordenadas = list(response.json()['candidates'][0]['location'].values())
            
        filas.append([resultado, coordenadas, fecha_muestra, id_tamiz])
        
    cursor1.executemany(sql, filas) #se usa executemany en lugar de execute y esto es porque lo que queremos agregar es la tupla
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
