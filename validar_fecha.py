from datetime import datetime as dt, date

def validar_fecha(fecha_inicio, fecha_fin, f_validar):

    FECHA = date(1900,1,1)
    
    if fecha_inicio == None:
        fecha_inicio = date(1900, 1, 1)
    if fecha_fin == None:
        fecha_fin = dt.now().date()

    if f_validar.find('/') >= 0:
        data_fecha = f_validar.replace('"', '').replace("'", "")
        data_fecha = data_fecha[:10].split(sep='/')
    elif f_validar.find('-') >= 0:
        data_fecha = f_validar.replace('"', '').replace("'", "")
        data_fecha = data_fecha[:10].split(sep='-')
    else:
        data_fecha = None
            
    try: 
        if data_fecha != None and len(data_fecha) == 3:
            if len(data_fecha[2]) >= 4:
                anio = data_fecha[2].replace('"', '').replace("'", "")
                anio = int(anio[:4])
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[0].replace('"', '').replace("'", ""))
            elif len(data_fecha[2]) == 2:
                anio = int(data_fecha[0].replace('"', '').replace("'", ""))
                mes = int(data_fecha[1].replace('"', '').replace("'", ""))
                dia = int(data_fecha[2].replace('"', '').replace("'", ""))
        else:
            data_fecha = None

        if data_fecha != None:
            FECHA = date(anio, mes, dia)
            if FECHA < fecha_inicio or FECHA > fecha_fin:
                FECHA = None
        else:
            FECHA = None
    except ValueError:
        FECHA = None

    return FECHA

'''
fecha_inicial = date(2020, 3, 14)
fecha_final = dt.now().date()

print(f'fecha_inicial: {fecha_inicial}')
print(f'fecha_final: {fecha_final}')
print('la fecha 1 a validar es 2020-03-20')

print(validar_fecha(fecha_inicial, fecha_final, '2020-03-20'))

print('la fecha 2 a validar es 2020-01-20')

print(validar_fecha(fecha_inicial, fecha_final, '2020-01-20'))
'''