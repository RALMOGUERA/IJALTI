import psycopg2
from datetime import datetime as dt, date
import datetime
import pandas as pd
#aqui hay que instalar xlrd para poder leer desde un archivo excel, la instaruccion de instalacion es:
#pip install xlrd

def quitarNat(campo):
    if type(campo) == pd._libs.tslibs.nattype.NaTType:
        campo = None
    return campo

def ConvFech(str_date):
    
    fool = None
    
    if str_date:            
        str_date = str_date.replace('-','/')
        #fool = datetime.datetime.strptime(str_date, '%Y-%m-%d %H:%M:%S')
        fool = datetime.datetime.strptime(str_date, '%d/%m/%Y')
        return fool
    else:
        return #str_date



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

archivo3 = pd.read_excel('C:\\Clientes\IJALTI - Coronavirus\\Datos Abiertos Secretaria e Ijalti\\DATOS PARA SS 26-08-20.xlsx')


archivo3 = archivo3.where(archivo3.notnull(),None)
columnas = archivo3.columns


id_tamiz = archivo3['id_tamiz'].tolist()
fecha_llamada = archivo3['fecha_llamada'].tolist()
hora_llamada = archivo3['hora_llamada'].tolist()
emergencia = archivo3['emergencia'].tolist()
acepto_muestra = archivo3['acepto_muestra'].tolist()
muestra_domicilio = archivo3['muestra_domicilio'].tolist()
apellido_paterno = archivo3['apellido_paterno'].tolist()
apellido_materno = archivo3['apellido_materno'].tolist()
nombres = archivo3['nombres'].tolist()
sexo = archivo3['sexo'].tolist()
fecha_nacimiento = archivo3['fecha_nacimiento'].tolist()
edad = archivo3['edad'].tolist()
nacionalidad = archivo3['nacionalidad'].tolist()
pais_origen = archivo3['pais_origen'].tolist()
entidad_nacimiento = archivo3['entidad_nacimiento'].tolist()
curp = archivo3['curp'].tolist()
telefono_uno = archivo3['telefono_uno'].tolist()
telefono_dos = archivo3['telefono_dos'].tolist()
ocupacion = archivo3['ocupacion'].tolist()
embarazo = archivo3['embarazo'].tolist()
meses_embarazo = archivo3['meses_embarazo'].tolist()
puerperio = archivo3['puerperio'].tolist()
dias_puerperio = archivo3['dias_puerperio'].tolist()
entidad = archivo3['entidad'].tolist()
calle = archivo3['calle'].tolist()
num_ext = archivo3['num_ext'].tolist()
num_int = archivo3['num_int'].tolist()
codigo_postal = archivo3['codigo_postal'].tolist()
colonia = archivo3['colonia'].tolist()
ciudad = archivo3['ciudad'].tolist()
estado = archivo3['estado'].tolist()
migrante = archivo3['migrante'].tolist()
fecha_viaje = archivo3['fecha_viaje'].tolist()
lugar_viaje = archivo3['lugar_viaje'].tolist()
municipio_viaje = archivo3['municipio_viaje'].tolist()
estado_viaje = archivo3['estado_viaje'].tolist()
pais_viaje = archivo3['pais_viaje'].tolist()
fecha_llegada = archivo3['fecha_llegada'].tolist()
aerolinea = archivo3['aerolinea'].tolist()
viaje_fuera = archivo3['viaje_fuera'].tolist()
contacto_covid = archivo3['contacto_covid'].tolist()
valoracion = archivo3['valoracion'].tolist()
fecha_valoracion = archivo3['fecha_valoracion'].tolist()
total_sintomas = archivo3['total_sintomas'].tolist()
laboratorio = archivo3['laboratorio'].tolist()
fecha_muestra = archivo3['fecha_muestra'].tolist()
resultado_laboratorio = archivo3['resultado_laboratorio'].tolist()
sin_muestra = archivo3['sin_muestra'].tolist()
respiratoria = archivo3['respiratoria'].tolist()
labios_morados = archivo3['labios_morados'].tolist()
dolor_pecho = archivo3['dolor_pecho'].tolist()
pararse = archivo3['pararse'].tolist()
convulsionese = archivo3['convulsionese'].tolist()
fiebre = archivo3['fiebre'].tolist()
fecha_fiebre = archivo3['fecha_fiebre'].tolist()
tos_seca = archivo3['tos_seca'].tolist()
fecha_tos_seca = archivo3['fecha_tos_seca'].tolist()
congestion_nasal = archivo3['congestion_nasal'].tolist()
fecha_congestion_nasal = archivo3['fecha_congestion_nasal'].tolist()
dolor_toracico_pecho = archivo3['dolor_toracico_pecho'].tolist()
fecha_dolor_pecho = archivo3['fecha_dolor_pecho'].tolist()
cansancio_fatiga = archivo3['cansancio_fatiga'].tolist()
fecha_fatiga = archivo3['fecha_fatiga'].tolist()
tos_con_espectoracion_flema = archivo3['tos_con_espectoracion_flema'].tolist()
fecha_tos_flema = archivo3['fecha_tos_flema'].tolist()
dificultad_respiratoria = archivo3['dificultad_respiratoria'].tolist()
fecha_dificultad_respi = archivo3['fecha_dificultad_respi'].tolist()
ataque_al_estado_general = archivo3['ataque_al_estado_general'].tolist()
fecha_ataque_edo_gral = archivo3['fecha_ataque_edo_gral'].tolist()
dolor_de_cabeza = archivo3['dolor_de_cabeza'].tolist()
fecha_dolor_de_cabeza = archivo3['fecha_dolor_de_cabeza'].tolist()
irritabilidad = archivo3['irritabilidad'].tolist()
fecha_irritabilidad = archivo3['fecha_irritabilidad'].tolist()
escalofrios = archivo3['escalofrios'].tolist()
fecha_escalofrios = archivo3['fecha_escalofrios'].tolist()
dolor_muscular = archivo3['dolor_muscular'].tolist()
fecha_dolor_muscular = archivo3['fecha_dolor_muscular'].tolist()
dolor_en_huesos = archivo3['dolor_en_huesos'].tolist()
fecha_dolor_huesos = archivo3['fecha_dolor_huesos'].tolist()
escurrimiento_nasal = archivo3['escurrimiento_nasal'].tolist()
fecha_escurrimiento_nasal = archivo3['fecha_escurrimiento_nasal'].tolist()
ardor_de_garganta = archivo3['ardor_de_garganta'].tolist()
fecha_ardor_garganta = archivo3['fecha_ardor_garganta'].tolist()
gripa_o_resfriado = archivo3['gripa_o_resfriado'].tolist()
fecha_gripa = archivo3['fecha_gripa'].tolist()
conjuntivitis = archivo3['conjuntivitis'].tolist()
fecha_conjuntivitis = archivo3['fecha_conjuntivitis'].tolist()
diarrea = archivo3['diarrea'].tolist()
fecha_diarrea = archivo3['fecha_diarrea'].tolist()
vomito = archivo3['vomito'].tolist()
fecha_vomito = archivo3['fecha_vomito'].tolist()
dolor_abdominal = archivo3['dolor_abdominal'].tolist()
fecha_dolor_abdominal = archivo3['fecha_dolor_abdominal'].tolist()
otro = archivo3['otro'].tolist()
fecha_otro = archivo3['fecha_otro'].tolist()
respiracion_rapida = archivo3['respiracion_rapida'].tolist()
fecha_respiracion_rapida = archivo3['fecha_respiracion_rapida'].tolist()
convulsiones = archivo3['convulsiones'].tolist()
fecha_convulsiones = archivo3['fecha_convulsiones'].tolist()
diabetes = archivo3['diabetes'].tolist()
enf_pulmonar = archivo3['enf_pulmonar'].tolist()
asma = archivo3['asma'].tolist()
inmunosupresion = archivo3['inmunosupresion'].tolist()
vih_sida = archivo3['vih_sida'].tolist()
enfermedad_cardiaca = archivo3['enfermedad_cardiaca'].tolist()
obesidad = archivo3['obesidad'].tolist()
hipertension_arterial = archivo3['hipertension_arterial'].tolist()
insuf_renal_cronica = archivo3['insuf_renal_cronica'].tolist()
tabaquismo = archivo3['tabaquismo'].tolist()
cancer = archivo3['cancer'].tolist()
enfermedad_hepatica = archivo3['enfermedad_hepatica'].tolist()
desconoce = archivo3['desconoce'].tolist()
ninguno = archivo3['ninguno'].tolist()
otra_enfermedad = archivo3['otra_enfermedad'].tolist()
personal_de_salud = archivo3['personal_de_salud'].tolist()
email = archivo3['email'].tolist()
fech_prim_sint = archivo3['fech_prim_sint'].tolist()

fila = []
fila.clear()
filas = [[]]
filas.clear()

num_filas = range(len(id_tamiz))
#num_filas = range(1)

#print(id_tamiz)

for i in num_filas:
    id_tamiz[i] = quitarNat(id_tamiz[i])
    fecha_llamada[i] = quitarNat(fecha_llamada[i])
    try:
        fecha_llamada[i]=ConvFech(fecha_llamada[i])
    except:
        pass

    hora_llamada[i] = quitarNat(hora_llamada[i])
    emergencia[i] = quitarNat(emergencia[i])
    acepto_muestra[i] = quitarNat(acepto_muestra[i])
    muestra_domicilio[i] = quitarNat(muestra_domicilio[i])
    apellido_paterno[i] = quitarNat(apellido_paterno[i])
    apellido_materno[i] = quitarNat(apellido_materno[i])
    nombres[i] = quitarNat(nombres[i])
    sexo[i] = quitarNat(sexo[i])
    fecha_nacimiento[i] = quitarNat(fecha_nacimiento[i])
    try:
        fecha_nacimiento[i]=ConvFech(fecha_nacimiento[i])
    except:
        pass
    
    edad[i] = quitarNat(edad[i])
    nacionalidad[i] = quitarNat(nacionalidad[i])
    pais_origen[i] = quitarNat(pais_origen[i])
    entidad_nacimiento[i] = quitarNat(entidad_nacimiento[i])
    curp[i] = quitarNat(curp[i])
    telefono_uno[i] = quitarNat(telefono_uno[i])
    telefono_dos[i] = quitarNat(telefono_dos[i])
    ocupacion[i] = quitarNat(ocupacion[i])
    embarazo[i] = quitarNat(embarazo[i])
    meses_embarazo[i] = quitarNat(meses_embarazo[i])
    puerperio[i] = quitarNat(puerperio[i])
    dias_puerperio[i] = quitarNat(dias_puerperio[i])
    entidad[i] = quitarNat(entidad[i])
    calle[i] = quitarNat(calle[i])
    num_ext[i] = quitarNat(num_ext[i])
    num_int[i] = quitarNat(num_int[i])
    codigo_postal[i] = quitarNat(codigo_postal[i])
    colonia[i] = quitarNat(colonia[i])
    ciudad[i] = quitarNat(ciudad[i])
    estado[i] = quitarNat(estado[i])
    migrante[i] = quitarNat(migrante[i])
    fecha_viaje[i] = quitarNat(fecha_viaje[i])
    try:
        fecha_viaje[i]=ConvFech(fecha_viaje[i])
    except:
        pass
    lugar_viaje[i] = quitarNat(lugar_viaje[i])
    municipio_viaje[i] = quitarNat(municipio_viaje[i])
    estado_viaje[i] = quitarNat(estado_viaje[i])
    pais_viaje[i] = quitarNat(pais_viaje[i])
    fecha_llegada[i] = quitarNat(fecha_llegada[i])
    try:
        fecha_llegada[i]=ConvFech(fecha_llegada[i])
    except:
        pass
    aerolinea[i] = quitarNat(aerolinea[i])
    viaje_fuera[i] = quitarNat(viaje_fuera[i])
    contacto_covid[i] = quitarNat(contacto_covid[i])
    valoracion[i] = quitarNat(valoracion[i])
    fecha_valoracion[i] = quitarNat(fecha_valoracion[i])
    try:
        fecha_valoracion[i]=ConvFech(fecha_valoracion[i])
    except:
        pass
    total_sintomas[i] = quitarNat(total_sintomas[i])
    laboratorio[i] = quitarNat(laboratorio[i])
    fecha_muestra[i] = quitarNat(fecha_muestra[i])
    try:
        fecha_muestra[i]=ConvFech(fecha_muestra[i])
    except:
        pass
    resultado_laboratorio[i] = quitarNat(resultado_laboratorio[i])
    sin_muestra[i] = quitarNat(sin_muestra[i])
    respiratoria[i] = quitarNat(respiratoria[i])
    labios_morados[i] = quitarNat(labios_morados[i])
    dolor_pecho[i] = quitarNat(dolor_pecho[i])
    pararse[i] = quitarNat(pararse[i])
    convulsionese[i] = quitarNat(convulsionese[i])
    fiebre[i] = quitarNat(fiebre[i])
    fecha_fiebre[i] = quitarNat(fecha_fiebre[i])
    try:
        fecha_fiebre[i]=ConvFech(fecha_fiebre[i])
    except:
        pass
    tos_seca[i] = quitarNat(tos_seca[i])
    fecha_tos_seca[i] = quitarNat(fecha_tos_seca[i])
    try:
        fecha_tos_seca[i]=ConvFech(fecha_tos_seca[i])
    except:
        pass
    congestion_nasal[i] = quitarNat(congestion_nasal[i])
    fecha_congestion_nasal[i] = quitarNat(fecha_congestion_nasal[i])
    try:
        fecha_congestion_nasal[i]=ConvFech(fecha_congestion_nasal[i])
    except:
        pass
    dolor_toracico_pecho[i] = quitarNat(dolor_toracico_pecho[i])
    fecha_dolor_pecho[i] = quitarNat(fecha_dolor_pecho[i])
    try:
        fecha_dolor_pecho[i]=ConvFech(fecha_dolor_pecho[i])
    except:
        pass
    cansancio_fatiga[i] = quitarNat(cansancio_fatiga[i])
    fecha_fatiga[i] = quitarNat(fecha_fatiga[i])
    try:
        fecha_fatiga[i]=ConvFech(fecha_fatiga[i])
    except:
        pass
    tos_con_espectoracion_flema[i] = quitarNat(tos_con_espectoracion_flema[i])
    fecha_tos_flema[i] = quitarNat(fecha_tos_flema[i])
    try:
        fecha_tos_flema[i]=ConvFech(fecha_tos_flema[i])
    except:
        pass
    dificultad_respiratoria[i] = quitarNat(dificultad_respiratoria[i])
    fecha_dificultad_respi[i] = quitarNat(fecha_dificultad_respi[i])
    try:
        fecha_dificultad_respi[i]=ConvFech(fecha_dificultad_respi[i])
    except:
        pass
    ataque_al_estado_general[i] = quitarNat(ataque_al_estado_general[i])
    fecha_ataque_edo_gral[i] = quitarNat(fecha_ataque_edo_gral[i])
    try:
        fecha_ataque_edo_gral[i]=ConvFech(fecha_ataque_edo_gral[i])
    except:
        pass
    dolor_de_cabeza[i] = quitarNat(dolor_de_cabeza[i])
    fecha_dolor_de_cabeza[i] = quitarNat(fecha_dolor_de_cabeza[i])
    try:
        fecha_dolor_de_cabeza[i]=ConvFech(fecha_dolor_de_cabeza[i])
    except:
        pass
    irritabilidad[i] = quitarNat(irritabilidad[i])
    fecha_irritabilidad[i] = quitarNat(fecha_irritabilidad[i])
    try:
        fecha_irritabilidad[i]=ConvFech(fecha_irritabilidad[i])
    except:
        pass
    escalofrios[i] = quitarNat(escalofrios[i])
    fecha_escalofrios[i] = quitarNat(fecha_escalofrios[i])
    try:
        fecha_escalofrios[i]=ConvFech(fecha_escalofrios[i])
    except:
        pass
    dolor_muscular[i] = quitarNat(dolor_muscular[i])
    fecha_dolor_muscular[i] = quitarNat(fecha_dolor_muscular[i])
    try:
        fecha_dolor_muscular[i]=ConvFech(fecha_dolor_muscular[i])
    except:
        pass
    dolor_en_huesos[i] = quitarNat(dolor_en_huesos[i])
    fecha_dolor_huesos[i] = quitarNat(fecha_dolor_huesos[i])
    try:
        fecha_dolor_huesos[i]=ConvFech(fecha_dolor_huesos[i])
    except:
        pass
    escurrimiento_nasal[i] = quitarNat(escurrimiento_nasal[i])
    fecha_escurrimiento_nasal[i] = quitarNat(fecha_escurrimiento_nasal[i])
    try:
        fecha_escurrimiento_nasal[i]=ConvFech(fecha_escurrimiento_nasal[i])
    except:
        pass
    ardor_de_garganta[i] = quitarNat(ardor_de_garganta[i])
    fecha_ardor_garganta[i] = quitarNat(fecha_ardor_garganta[i])
    try:
        fecha_ardor_garganta[i]=ConvFech(fecha_ardor_garganta[i])
    except:
        pass
    gripa_o_resfriado[i] = quitarNat(gripa_o_resfriado[i])
    fecha_gripa[i] = quitarNat(fecha_gripa[i])
    try:
        fecha_gripa[i]=ConvFech(fecha_gripa[i])
    except:
        pass
    conjuntivitis[i] = quitarNat(conjuntivitis[i])
    fecha_conjuntivitis[i] = quitarNat(fecha_conjuntivitis[i])
    try:
        fecha_conjuntivitis[i]=ConvFech(fecha_conjuntivitis[i])
    except:
        pass
    diarrea[i] = quitarNat(diarrea[i])
    fecha_diarrea[i] = quitarNat(fecha_diarrea[i])
    try:
        fecha_diarrea[i]=ConvFech(fecha_diarrea[i])
    except:
        pass
    vomito[i] = quitarNat(vomito[i])
    fecha_vomito[i] = quitarNat(fecha_vomito[i])
    try:
        fecha_vomito[i]=ConvFech(fecha_vomito[i])
    except:
        pass
    dolor_abdominal[i] = quitarNat(dolor_abdominal[i])
    fecha_dolor_abdominal[i] = quitarNat(fecha_dolor_abdominal[i])
    try:
        fecha_dolor_abdominal[i]=ConvFech(fecha_dolor_abdominal[i])
    except:
        pass
    otro[i] = quitarNat(otro[i])
    fecha_otro[i] = quitarNat(fecha_otro[i])
    try:
        fecha_otro[i]=ConvFech(fecha_otro[i])
    except:
        pass
    respiracion_rapida[i] = quitarNat(respiracion_rapida[i])
    fecha_respiracion_rapida[i] = quitarNat(fecha_respiracion_rapida[i])
    try:
        fecha_respiracion_rapida[i]=ConvFech(fecha_respiracion_rapida[i])
    except:
        pass
    convulsiones[i] = quitarNat(convulsiones[i])
    fecha_convulsiones[i] = quitarNat(fecha_convulsiones[i])
    try:
        fecha_convulsiones[i]=ConvFech(fecha_convulsiones[i])
    except:
        pass
    diabetes[i] = quitarNat(diabetes[i])
    enf_pulmonar[i] = quitarNat(enf_pulmonar[i])
    asma[i] = quitarNat(asma[i])
    inmunosupresion[i] = quitarNat(inmunosupresion[i])
    vih_sida[i] = quitarNat(vih_sida[i])
    enfermedad_cardiaca[i] = quitarNat(enfermedad_cardiaca[i])
    obesidad[i] = quitarNat(obesidad[i])
    hipertension_arterial[i] = quitarNat(hipertension_arterial[i])
    insuf_renal_cronica[i] = quitarNat(insuf_renal_cronica[i])
    tabaquismo[i] = quitarNat(tabaquismo[i])
    cancer[i] = quitarNat(cancer[i])
    enfermedad_hepatica[i] = quitarNat(enfermedad_hepatica[i])
    desconoce[i] = quitarNat(desconoce[i])
    ninguno[i] = quitarNat(ninguno[i])
    otra_enfermedad[i] = quitarNat(otra_enfermedad[i])
    personal_de_salud[i] = quitarNat(personal_de_salud[i])
    email[i] = quitarNat(email[i])
    fech_prim_sint[i] = quitarNat(fech_prim_sint[i])
    try:
        fech_prim_sint[i]=ConvFech(fech_prim_sint[i])
    except:
        pass
    campos = [id_tamiz[i], fecha_llamada[i], hora_llamada[i], emergencia[i], acepto_muestra[i], muestra_domicilio[i], apellido_paterno[i], apellido_materno[i], nombres[i], sexo[i], fecha_nacimiento[i], edad[i], nacionalidad[i], pais_origen[i], entidad_nacimiento[i], curp[i], telefono_uno[i], telefono_dos[i], ocupacion[i], embarazo[i], meses_embarazo[i], puerperio[i], dias_puerperio[i], entidad[i], calle[i], num_ext[i], num_int[i], codigo_postal[i], colonia[i], ciudad[i], estado[i], migrante[i], fecha_viaje[i], lugar_viaje[i], municipio_viaje[i], estado_viaje[i], pais_viaje[i], fecha_llegada[i], aerolinea[i], viaje_fuera[i], contacto_covid[i], valoracion[i], fecha_valoracion[i], total_sintomas[i], laboratorio[i], fecha_muestra[i], resultado_laboratorio[i], sin_muestra[i], respiratoria[i], labios_morados[i], dolor_pecho[i], pararse[i], convulsionese[i], fiebre[i], fecha_fiebre[i], tos_seca[i], fecha_tos_seca[i], congestion_nasal[i], fecha_congestion_nasal[i], dolor_toracico_pecho[i], fecha_dolor_pecho[i], cansancio_fatiga[i], fecha_fatiga[i], tos_con_espectoracion_flema[i], fecha_tos_flema[i], dificultad_respiratoria[i], fecha_dificultad_respi[i], ataque_al_estado_general[i], fecha_ataque_edo_gral[i], dolor_de_cabeza[i], fecha_dolor_de_cabeza[i], irritabilidad[i], fecha_irritabilidad[i], escalofrios[i], fecha_escalofrios[i], dolor_muscular[i], fecha_dolor_muscular[i], dolor_en_huesos[i], fecha_dolor_huesos[i], escurrimiento_nasal[i], fecha_escurrimiento_nasal[i], ardor_de_garganta[i], fecha_ardor_garganta[i], gripa_o_resfriado[i], fecha_gripa[i], conjuntivitis[i], fecha_conjuntivitis[i], diarrea[i], fecha_diarrea[i], vomito[i], fecha_vomito[i], dolor_abdominal[i], fecha_dolor_abdominal[i], otro[i], fecha_otro[i], respiracion_rapida[i], fecha_respiracion_rapida[i], convulsiones[i], fecha_convulsiones[i], diabetes[i], enf_pulmonar[i], asma[i], inmunosupresion[i], vih_sida[i], enfermedad_cardiaca[i], obesidad[i], hipertension_arterial[i], insuf_renal_cronica[i], tabaquismo[i], cancer[i], enfermedad_hepatica[i], desconoce[i], ninguno[i], otra_enfermedad[i], personal_de_salud[i], email[i], fech_prim_sint[i]]
    #print(campos)
    filas.append(campos)  

#print(filas)          

#aqui voy a hacer el insert en la tabla de datos_radar en postgresql 
try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    sql = 'TRUNCATE TABLE staging.datos_radar'
    cursor1.execute(sql)
    print('despues del truncate')
    sql = 'INSERT INTO staging.datos_radar \
    (id_tamiz, fecha_llamada, hora_llamada, emergencia, acepto_muestra, muestra_domicilio, apellido_paterno, apellido_materno, nombres, sexo, fecha_nacimiento, edad, nacionalidad, pais_origen, entidad_nacimiento, curp, telefono_uno, telefono_dos, ocupacion, embarazo, meses_embarazo, puerperio, dias_puerperio, entidad, calle, num_ext, num_int, codigo_postal, colonia, ciudad, estado, migrante, fecha_viaje, lugar_viaje, municipio_viaje, estado_viaje, pais_viaje, fecha_llegada, aerolinea, viaje_fuera, contacto_covid, valoracion, fecha_valoracion, total_sintomas, laboratorio, fecha_muestra, resultado_laboratorio, sin_muestra, respiratoria, labios_morados, dolor_pecho, pararse, convulsionese, fiebre, fecha_fiebre, tos_seca, fecha_tos_seca, congestion_nasal, fecha_congestion_nasal, dolor_toracico_pecho, fecha_dolor_pecho, cansancio_fatiga, fecha_fatiga, tos_con_espectoracion_flema, fecha_tos_flema, dificultad_respiratoria, fecha_dificultad_respi, ataque_al_estado_general, fecha_ataque_edo_gral, dolor_de_cabeza, fecha_dolor_de_cabeza, irritabilidad, fecha_irritabilidad, escalofrios, fecha_escalofrios, dolor_muscular, fecha_dolor_muscular, dolor_en_huesos, fecha_dolor_huesos, escurrimiento_nasal, fecha_escurrimiento_nasal, ardor_de_garganta, fecha_ardor_garganta, gripa_o_resfriado, fecha_gripa, conjuntivitis, fecha_conjuntivitis, diarrea, fecha_diarrea, vomito, fecha_vomito, dolor_abdominal, fecha_dolor_abdominal, otro, fecha_otro, respiracion_rapida, fecha_respiracion_rapida, convulsiones, fecha_convulsiones, diabetes, enf_pulmonar, asma, inmunosupresion, vih_sida, enfermedad_cardiaca, obesidad, hipertension_arterial, insuf_renal_cronica, tabaquismo, cancer, enfermedad_hepatica, desconoce, ninguno, otra_enfermedad, personal_de_salud, email, fech_prim_sint) \
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);'
    print('antes de hacer el insert en datos_radar')    
    cursor1.executemany(sql, filas) 
    print('despues de hacer el insert en datos_radar')
    
except Exception as e:
    print("ocurrio un error en el insert")
    print(Exception)
finally:
    conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')