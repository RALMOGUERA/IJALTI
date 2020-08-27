#esto hay que hacerlo en DB para poder usar las funciones que necesito
#CREATE EXTENSION pg_trgm;
#CREATE EXTENSION fuzzystrmatch;

import psycopg2
from datetime import datetime as dt, date
import validar_fecha as vf


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

                            
fecha_inicio = date(2020, 3, 14)
fecha_fin = dt.now().date()


filas = [[]]
filas.clear()
i = 0
try:
    print(f'comenzo a ejecutarse: {dt.now()}')
    cursor1 = conexion.cursor()
    
    #aqui se borra la data que esta en el historico de datos_radar_hist que tenga la misma fecha que esta en data_radar
    sql = 'DELETE FROM staging.datos_radar_hist drh WHERE drh.fecha_procesamiento = CURRENT_DATE;'
    cursor1.execute(sql)
    
    #aqui se insertan los valores en la tabla de historicos
    sql = 'INSERT INTO staging.datos_radar_hist \
    (fecha_procesamiento, id_tamiz, fecha_llamada, hora_llamada, emergencia, acepto_muestra, muestra_domicilio, apellido_paterno, apellido_materno, nombres, sexo, fecha_nacimiento, edad, nacionalidad, pais_origen, entidad_nacimiento, curp, telefono_uno, telefono_dos, ocupacion, embarazo, meses_embarazo, puerperio, dias_puerperio, entidad, calle, num_ext, num_int, codigo_postal, colonia, ciudad, estado, migrante, fecha_viaje, lugar_viaje, municipio_viaje, estado_viaje, pais_viaje, fecha_llegada, aerolinea, viaje_fuera, contacto_covid, valoracion, fecha_valoracion, total_sintomas, laboratorio, fecha_muestra, resultado_laboratorio, sin_muestra, respiratoria, labios_morados, dolor_pecho, pararse, convulsionese, fiebre, fecha_fiebre, tos_seca, fecha_tos_seca, congestion_nasal, fecha_congestion_nasal, dolor_toracico_pecho, fecha_dolor_pecho, cansancio_fatiga, fecha_fatiga, tos_con_espectoracion_flema, fecha_tos_flema, dificultad_respiratoria, fecha_dificultad_respi, ataque_al_estado_general, fecha_ataque_edo_gral, dolor_de_cabeza, fecha_dolor_de_cabeza, irritabilidad, fecha_irritabilidad, escalofrios, fecha_escalofrios, dolor_muscular, fecha_dolor_muscular, dolor_en_huesos, fecha_dolor_huesos, escurrimiento_nasal, fecha_escurrimiento_nasal, ardor_de_garganta, fecha_ardor_garganta, gripa_o_resfriado, fecha_gripa, conjuntivitis, fecha_conjuntivitis, diarrea, fecha_diarrea, vomito, fecha_vomito, dolor_abdominal, fecha_dolor_abdominal, otro, fecha_otro, respiracion_rapida, fecha_respiracion_rapida, convulsiones, fecha_convulsiones, diabetes, enf_pulmonar, asma, inmunosupresion, vih_sida, enfermedad_cardiaca, obesidad, hipertension_arterial, insuf_renal_cronica, tabaquismo, cancer, enfermedad_hepatica, desconoce, ninguno, otra_enfermedad, personal_de_salud, email, fech_prim_sint, signo_alerta) \
    SELECT \
    CURRENT_DATE, id_tamiz, fecha_llamada, hora_llamada, emergencia, acepto_muestra, muestra_domicilio, apellido_paterno, apellido_materno, nombres, sexo, fecha_nacimiento, edad, nacionalidad, pais_origen, entidad_nacimiento, curp, telefono_uno, telefono_dos, ocupacion, embarazo, meses_embarazo, puerperio, dias_puerperio, entidad, calle, num_ext, num_int, codigo_postal, colonia, ciudad, estado, migrante, fecha_viaje, lugar_viaje, municipio_viaje, estado_viaje, pais_viaje, fecha_llegada, aerolinea, viaje_fuera, contacto_covid, valoracion, fecha_valoracion, total_sintomas, laboratorio, fecha_muestra, resultado_laboratorio, sin_muestra, respiratoria, labios_morados, dolor_pecho, pararse, convulsionese, fiebre, fecha_fiebre, tos_seca, fecha_tos_seca, congestion_nasal, fecha_congestion_nasal, dolor_toracico_pecho, fecha_dolor_pecho, cansancio_fatiga, fecha_fatiga, tos_con_espectoracion_flema, fecha_tos_flema, dificultad_respiratoria, fecha_dificultad_respi, ataque_al_estado_general, fecha_ataque_edo_gral, dolor_de_cabeza, fecha_dolor_de_cabeza, irritabilidad, fecha_irritabilidad, escalofrios, fecha_escalofrios, dolor_muscular, fecha_dolor_muscular, dolor_en_huesos, fecha_dolor_huesos, escurrimiento_nasal, fecha_escurrimiento_nasal, ardor_de_garganta, fecha_ardor_garganta, gripa_o_resfriado, fecha_gripa, conjuntivitis, fecha_conjuntivitis, diarrea, fecha_diarrea, vomito, fecha_vomito, dolor_abdominal, fecha_dolor_abdominal, otro, fecha_otro, respiracion_rapida, fecha_respiracion_rapida, convulsiones, fecha_convulsiones, diabetes, enf_pulmonar, asma, inmunosupresion, vih_sida, enfermedad_cardiaca, obesidad, hipertension_arterial, insuf_renal_cronica, tabaquismo, cancer, enfermedad_hepatica, desconoce, ninguno, otra_enfermedad, personal_de_salud, email, fech_prim_sint, signo_alerta \
    FROM staging.datos_radar;'
    
    cursor1.execute(sql)
    
    sql = 'TRUNCATE TABLE staging.datos_radar_limpios_parcial'
    cursor1.execute(sql)
    
    sql = '''insert into staging.datos_radar_limpios_parcial \
    (id_tamiz, fecha_llamada, hora_llamada, emergencia, acepto_muestra, muestra_domicilio, \
    apellido_paterno, apellido_materno, nombres, sexo, fecha_nacimiento, edad, nacionalidad, \
    pais_origen, entidad_nacimiento, curp, telefono_uno, telefono_dos, ocupacion, embarazo, \
    meses_embarazo, puerperio, dias_puerperio, entidad, calle, num_ext, num_int, codigo_postal, \
    colonia, ciudad, estado, migrante, fecha_viaje, lugar_viaje, municipio_viaje, estado_viaje, \
    pais_viaje, fecha_llegada, aerolinea, viaje_fuera, contacto_covid, valoracion, fecha_valoracion, \
    total_sintomas, laboratorio, fecha_muestra, resultado_laboratorio, sin_muestra, respiratoria, \
    labios_morados, dolor_pecho, pararse, convulsionese, fiebre, fecha_fiebre, tos_seca, \
    fecha_tos_seca, congestion_nasal, fecha_congestion_nasal, dolor_toracico_pecho, \
    fecha_dolor_pecho, cansancio_fatiga, fecha_fatiga, tos_con_espectoracion_flema, fecha_tos_flema, \
    dificultad_respiratoria, fecha_dificultad_respi, ataque_al_estado_general, fecha_ataque_edo_gral, \
    dolor_de_cabeza, fecha_dolor_de_cabeza, irritabilidad, fecha_irritabilidad, escalofrios, \
    fecha_escalofrios, dolor_muscular, fecha_dolor_muscular, dolor_en_huesos, fecha_dolor_huesos, \
    escurrimiento_nasal, fecha_escurrimiento_nasal, ardor_de_garganta, fecha_ardor_garganta, \
    gripa_o_resfriado, fecha_gripa, conjuntivitis, fecha_conjuntivitis, diarrea, fecha_diarrea, \
    vomito, fecha_vomito, dolor_abdominal, fecha_dolor_abdominal, otro, fecha_otro, respiracion_rapida, \
    fecha_respiracion_rapida, convulsiones, fecha_convulsiones, diabetes, enf_pulmonar, asma, \
    inmunosupresion, vih_sida, enfermedad_cardiaca, obesidad, hipertension_arterial, insuf_renal_cronica, \
    tabaquismo, cancer, enfermedad_hepatica, desconoce, ninguno, otra_enfermedad, personal_de_salud, \
    email, fech_prim_sint, signo_alerta) \
    SELECT \
    dr.id_tamiz, coalesce(staging.is_date(dr.fecha_llamada), ''), coalesce(dr.hora_llamada, ''), \
    COALESCE(emer."CLAVE", 0) as emergencia, \
    COALESCE(acept_mues."CLAVE", 0) as acepto_muestra, \
    COALESCE(mues_domi."CLAVE", 0) as muestra_domicilio, \
    coalesce(translate(replace(upper(trim(dr.apellido_paterno)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU'), '') as apellido_paterno, \
    coalesce(translate(replace(upper(trim(dr.apellido_materno)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU'), '') as apellido_materno, \
    coalesce(translate(replace(upper(trim(dr.nombres)), '  ', ' '),'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU'), '') as nombres, \
    COALESCE(sexo."CLAVE", 0) as sexo, \
    staging.is_date(dr.fecha_nacimiento), dr.edad, 
    translate(replace(upper(trim(dr.nacionalidad)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as nacionalidad, \
    translate(replace(upper(trim(dr.pais_origen)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as pais_origen, \
    translate(replace(upper(trim(dr.entidad_nacimiento)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as entidad_nacimiento, \
    coalesce(upper(dr.curp), '') as curp, dr.telefono_uno, dr.telefono_dos, \
    translate(replace(upper(trim(dr.ocupacion)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as ocupacion, \
    COALESCE(emba."CLAVE", 0) as embarazo, \
    dr.meses_embarazo, \
    COALESCE(puer."CLAVE", 0) as puerperio, \
    dr.dias_puerperio, \
    translate(replace(upper(trim(dr.entidad)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as entidad, \
    dr.calle, dr.num_ext, dr.num_int, dr.codigo_postal, \
    translate(replace(upper(trim(dr.colonia)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as colonia, \
    translate(replace(upper(trim(dr.ciudad)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as ciudad, \
    translate(replace(upper(trim(dr.estado)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as estado, \
    COALESCE(migra."CLAVE", 0) as migrante, \
    staging.is_date(dr.fecha_viaje), \
    translate(replace(upper(trim(dr.lugar_viaje)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`:-_<>','AAAEEEIIIOOOUUU') as lugar_viaje, \
    translate(replace(upper(trim(dr.municipio_viaje)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`:-_<>','AAAEEEIIIOOOUUU') as municipio_viaje, \
    translate(replace(upper(trim(dr.estado_viaje)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`:-_<>','AAAEEEIIIOOOUUU') as estado_viaje, \
    translate(replace(upper(trim(dr.pais_viaje)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`:-_<>','AAAEEEIIIOOOUUU') as pais_viaje, \
    staging.is_date(dr.fecha_llegada), \
    translate(replace(upper(trim(dr.aerolinea)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as aerolinea, \
    COALESCE(viaj_fuer."CLAVE", 0) as viaje_fuera, \
    COALESCE(cont_covi."CLAVE", 0) as contacto_covid, \
    COALESCE(valo."CLAVE", 0) as valoracion, \
    staging.is_date(dr.fecha_valoracion), dr.total_sintomas, \
    translate(replace(upper(trim(dr.laboratorio)), '  ', ' '), 'ÁÄÂÉËÊÍÏÎÓÖÔÚÜÛ|°¬!"#$%&/()=?\¿¨´+*~^{}[]`,;.:-_<>','AAAEEEIIIOOOUUU') as laboratorio, \
    staging.is_date(dr.fecha_muestra), \
    COALESCE(resu_labo."CLAVE", 0) as resultado_laboratorio, \
    COALESCE(sin_mues."CLAVE", 0) as sin_muestra, \
    COALESCE(resp."CLAVE", 0) as respiratoria, \
    COALESCE(labi_mora."CLAVE", 0) as labios_morados, \
    COALESCE(dolo_pech."CLAVE", 0) as dolor_pecho, \
    COALESCE(para."CLAVE", 0) as pararse, \
    COALESCE(conves."CLAVE", 0) as convulsionese, \
    COALESCE(fieb."CLAVE", 0) as fiebre, \
    staging.is_date(dr.fecha_fiebre), \
    COALESCE(tos_seca."CLAVE", 0) as tos_seca, \
    staging.is_date(dr.fecha_tos_seca), \
    COALESCE(cong_nasa."CLAVE", 0) as congestion_nasal, \
    staging.is_date(dr.fecha_congestion_nasal), \
    COALESCE(dolo_tora_pech."CLAVE", 0) as dolor_toracico_pecho, \
    staging.is_date(dr.fecha_dolor_pecho), \
    COALESCE(cans_fati."CLAVE", 0) as cansancio_fatiga, \
    staging.is_date(dr.fecha_fatiga), \
    COALESCE(tos_con_espe_flem."CLAVE", 0) as tos_con_espectoracion_flema, \
    staging.is_date(dr.fecha_tos_flema), \
    COALESCE(difi_resp."CLAVE", 0) as dificultad_respiratoria, \
    staging.is_date(dr.fecha_dificultad_respi), \
    COALESCE(ataq_al_esta_gene."CLAVE", 0) as ataque_al_estado_general, \
    staging.is_date(dr.fecha_ataque_edo_gral), \
    COALESCE(dolo_de_cabe."CLAVE", 0) as dolor_de_cabeza, \
    staging.is_date(dr.fecha_dolor_de_cabeza), \
    COALESCE(irri."CLAVE", 0) as irritabilidad, \
    staging.is_date(dr.fecha_irritabilidad), \
    COALESCE(esca."CLAVE", 0) as escalofrios, \
    staging.is_date(dr.fecha_escalofrios), \
    COALESCE(dolo_musc."CLAVE", 0) as dolor_muscular, \
    staging.is_date(dr.fecha_dolor_muscular), \
    COALESCE(dolo_en_hues."CLAVE", 0) as dolor_en_huesos, \
    staging.is_date(dr.fecha_dolor_huesos), \
    COALESCE(escu_nasa."CLAVE", 0) as escurrimiento_nasal, \
    staging.is_date(dr.fecha_escurrimiento_nasal), \
    COALESCE(ardo_de_garg."CLAVE", 0) as ardor_de_garganta, \
    staging.is_date(dr.fecha_ardor_garganta), \
    COALESCE(grip_o_resf."CLAVE", 0) as gripa_o_resfriado, \
    staging.is_date(dr.fecha_gripa), \
    COALESCE(conj."CLAVE", 0) as conjuntivitis, \
    staging.is_date(dr.fecha_conjuntivitis), \
    COALESCE(diar."CLAVE", 0) as diarrea, \
    staging.is_date(dr.fecha_diarrea), \
    COALESCE(vomi."CLAVE", 0) as vomito, \
    staging.is_date(dr.fecha_vomito), \
    COALESCE(dolo_abdo."CLAVE", 0) as dolor_abdominal, \
    staging.is_date(dr.fecha_dolor_abdominal), \
    COALESCE(otro."CLAVE", 0) as otro, \
    staging.is_date(dr.fecha_otro), \
    COALESCE(resp_rapi."CLAVE", 0) as respiracion_rapida, \
    staging.is_date(dr.fecha_respiracion_rapida), \
    COALESCE(convu."CLAVE", 0) as convulsiones, \
    staging.is_date(dr.fecha_convulsiones), \
    COALESCE(diab."CLAVE", 0) as diabetes, \
    COALESCE(enf_pulm."CLAVE", 0) as enf_pulmonar, \
    COALESCE(asma."CLAVE", 0) as asma, \
    COALESCE(inmu."CLAVE", 0) as inmunosupresion, \
    COALESCE(vih_sida."CLAVE", 0) as vih_sida, \
    COALESCE(enfe_card."CLAVE", 0) as enfermedad_cardiaca, \
    COALESCE(obes."CLAVE", 0) as obesidad, \
    COALESCE(hipe_arte."CLAVE", 0) as hipertension_arterial, \
    COALESCE(insu_rena_cron."CLAVE", 0) as insuf_renal_cronica, \
    COALESCE(taba."CLAVE", 0) as tabaquismo, \
    COALESCE(canc."CLAVE", 0) as cancer, \
    COALESCE(enfe_hepa."CLAVE", 0) as enfermedad_hepatica, \
    COALESCE(desco."CLAVE", 0) as desconoce, \
    COALESCE(ning."CLAVE", 0) as ninguno, \
    COALESCE(otra_enfe."CLAVE", 0) as otra_enfermedad, \
    COALESCE(pers_de_salu."CLAVE", 0) as personal_de_salud, \
    dr.email, staging.is_date(dr.fech_prim_sint), \
    COALESCE(sign_aler."CLAVE", 0) as signo_alerta \
    FROM staging.datos_radar dr \
    LEFT OUTER JOIN staging."catalogo_SEXO" sexo ON TRIM(dr.sexo) = sexo."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" emer ON TRIM(dr.emergencia) = emer."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" emba ON TRIM(dr.embarazo) = emba."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" puer ON TRIM(dr.puerperio) = puer."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" migra ON TRIM(dr.migrante) = migra."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" viaj_fuer ON TRIM(dr.viaje_fuera) = viaj_fuer."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" cont_covi ON TRIM(dr.contacto_covid) = cont_covi."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" valo ON TRIM(dr.valoracion) = valo."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" sin_mues ON TRIM(dr.sin_muestra) = sin_mues."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" resp ON TRIM(dr.respiratoria) = resp."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" labi_mora ON TRIM(dr.labios_morados) = labi_mora."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_pech ON TRIM(dr.dolor_pecho) = dolo_pech."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" para ON TRIM(dr.pararse) = para."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" conves ON TRIM(dr.convulsionese) = conves."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" fieb ON TRIM(dr.fiebre) = fieb."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" tos_seca ON TRIM(dr.tos_seca) = tos_seca."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" cong_nasa ON TRIM(dr.congestion_nasal) = cong_nasa."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_tora_pech ON TRIM(dr.dolor_toracico_pecho) = dolo_tora_pech."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" cans_fati ON TRIM(dr.cansancio_fatiga) = cans_fati."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" tos_con_espe_flem ON TRIM(dr.tos_con_espectoracion_flema) = tos_con_espe_flem."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" difi_resp ON TRIM(dr.dificultad_respiratoria) = difi_resp."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" ataq_al_esta_gene ON TRIM(dr.ataque_al_estado_general) = ataq_al_esta_gene."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_de_cabe ON TRIM(dr.dolor_de_cabeza) = dolo_de_cabe."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" irri ON TRIM(dr.irritabilidad) = irri."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" esca ON TRIM(dr.escalofrios) = esca."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_musc ON TRIM(dr.dolor_muscular) = dolo_musc."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_en_hues ON TRIM(dr.dolor_en_huesos) = dolo_en_hues."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" escu_nasa ON TRIM(dr.escurrimiento_nasal) = escu_nasa."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" ardo_de_garg ON TRIM(dr.ardor_de_garganta) = ardo_de_garg."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" grip_o_resf ON TRIM(dr.gripa_o_resfriado) = grip_o_resf."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" conj ON TRIM(dr.conjuntivitis) = conj."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" diar ON TRIM(dr.diarrea) = diar."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" vomi ON TRIM(dr.vomito) = vomi."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" dolo_abdo ON TRIM(dr.dolor_abdominal) = dolo_abdo."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" otro ON TRIM(dr.otro) = otro."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" resp_rapi ON TRIM(dr.respiracion_rapida) = resp_rapi."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" convu ON TRIM(dr.convulsiones) = convu."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" diab ON TRIM(dr.diabetes) = diab."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" enf_pulm ON TRIM(dr.enf_pulmonar) = enf_pulm."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" asma ON TRIM(dr.asma) = asma."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" inmu ON TRIM(dr.inmunosupresion) = inmu."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" vih_sida ON TRIM(dr.vih_sida) = vih_sida."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" enfe_card ON TRIM(dr.enfermedad_cardiaca) = enfe_card."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" obes ON TRIM(dr.obesidad) = obes."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" hipe_arte ON TRIM(dr.hipertension_arterial) = hipe_arte."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" insu_rena_cron ON TRIM(dr.insuf_renal_cronica) = insu_rena_cron."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" taba ON TRIM(dr.tabaquismo) = taba."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" canc ON TRIM(dr.cancer) = canc."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" enfe_hepa ON TRIM(dr.enfermedad_hepatica) = enfe_hepa."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" desco ON TRIM(dr.desconoce) = desco."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" ning ON TRIM(dr.ninguno) = ning."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" otra_enfe ON TRIM(dr.otra_enfermedad) = otra_enfe."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" pers_de_salu ON TRIM(dr.personal_de_salud) = pers_de_salu."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" acept_mues ON TRIM(dr.acepto_muestra) = acept_mues."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_SI_NO_ABRV" sign_aler ON TRIM(dr.signo_alerta) = sign_aler."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_MUESTRA_DOMICILIO" mues_domi ON TRIM(dr.muestra_domicilio) = mues_domi."CLAVE_STR" \
    LEFT OUTER JOIN staging."catalogo_RESULTADO_LABORATORIO" resu_labo ON TRIM(dr.resultado_laboratorio) = resu_labo."CLAVE_STR";'''

    cursor1.execute(sql)
    
    #aca voy a actualizar el estado de las cosas que dicen que la ciudad es PUERTO VALLARTA a el estado de JALISCO
    sql = '''update staging.datos_radar_limpios_parcial a \
    set estado_nuevo = 'JALISCO' \
    where ciudad = 'PUERTO VALLARTA';'''
    
    cursor1.execute(sql)
    
    #aca se hace el update de los datos de los estados y de los codigos postales que si existen
    sql = '''update staging.datos_radar_limpios_parcial a \
    set estado_nuevo = COALESCE(cE.estado_str, staging.estado_str2(drlp.estado), 'JALISCO'), \
    cp = coalesce(cCP.codigo_postal, ''), \
    laboratorio = coalesce(lab."CLAVE", 0)::varchar, \
    nacionalidad = coalesce(nac."CLAVE", 0)::varchar, \
    entidad_nacimiento = COALESCE(en.estado_str, staging.estado_str2(drlp.entidad_nacimiento)), \
    estado_viaje = COALESCE(ev.estado_str, staging.estado_str2(drlp.estado_viaje)) \
    from staging.datos_radar_limpios_parcial drlp \
    left outer join staging."catalogo_ESTADOS_ABRV" cEA \
    on drlp.estado = cEA.estado_str \
    left outer join staging."catalogo_ESTADOS" cE \
    on cEA.codigo_estado = cE.codigo_estado \
    left outer join staging."catalogo_CODIGOS_POSTALES" cCP \
    on cCP.codigo_postal = drlp.codigo_postal \
    left outer join staging."catalogo_LABORATORIO" lab \
    on lab."CLAVE_STR" = drlp.laboratorio \
    left outer join staging."catalogo_NACIONALIDAD_ABRV" nac \
    on nac."CLAVE_STR" = drlp.nacionalidad \
    left outer join staging."catalogo_ESTADOS_ABRV" ent_nac \
    on drlp.entidad_nacimiento = ent_nac.estado_str \
    left outer join staging."catalogo_ESTADOS" en \
    on ent_nac.codigo_estado = en.codigo_estado \
    left outer join staging."catalogo_ESTADOS_ABRV" est_viaje \
    on drlp.estado_viaje = est_viaje.estado_str \
    left outer join staging."catalogo_ESTADOS" ev \
    on est_viaje.codigo_estado = ev.codigo_estado \
    where drlp.id_tamiz = a.id_tamiz;'''
    
    cursor1.execute(sql)
    
    #aca se buscan las claves de las cosas que si estan en la BD
    sql='''update staging.datos_radar_limpios_parcial a \
    set clave_estado = coalesce(cC.clave_estado, cE.clave_estado), \
	clave_municipio = coalesce(cC.clave_municipio, 0), \
	clave_ciudad = coalesce(cC.clave_ciudad, 0), \
	cp = coalesce(cC.codigo_postal, drlp.cp, ''), \
	clave_colonia = coalesce(cC.clave_colonia, 0::bigint), \
    clave_entidad_nacimiento = coalesce(en.clave_estado, 0), \
    clave_estado_viaje = coalesce(ev.clave_estado, 0), \
    clave_municipio_viaje = coalesce(munvia.clave_municipio, 0), \
    clave_pais_origen = coalesce(paori.clave_pais, 0), \
    clave_pais_viaje = coalesce(pavia.clave_pais, 0), \
    edad_calc = case when extract(year from age(to_date(left(drlp.fecha_nacimiento, 10), 'YYYY-MM-DD'))) <= 120 and extract(year from age(to_date(left(drlp.fecha_nacimiento, 10), 'YYYY-MM-DD'))) >= 0 then extract(year from age(to_date(left(drlp.fecha_nacimiento, 10), 'YYYY-MM-DD'))) else case when staging.is_integer(drlp.edad) <= 120 and staging.is_integer(drlp.edad) > 0 then staging.is_integer(drlp.edad) else NULL end  end \
    from staging.datos_radar_limpios_parcial drlp \
    left outer join staging."catalogo_COLONIAS" cC \
    on cC.colonia_str = drlp.colonia and cC.codigo_postal = drlp.cp \
    left outer join staging."catalogo_ESTADOS" cE \
    on cE.estado_str = drlp.estado_nuevo \
    left outer join staging."catalogo_ESTADOS" en \
    on en.estado_str = drlp.entidad_nacimiento \
    left outer join staging."catalogo_ESTADOS" ev \
    on ev.estado_str = drlp.estado_viaje \
    left outer join staging."catalogo_MUNICIPIOS" munvia \
    on munvia.estado_str = drlp.estado_viaje and munvia.municipio_str = drlp.municipio_viaje \
    left outer join staging."catalogo_PAISES_ABRV" paori \
    on paori.pais_str = drlp.pais_origen \
    left outer join staging."catalogo_PAISES_ABRV" pavia \
    on pavia.pais_str = drlp.pais_viaje \
    where drlp.id_tamiz = a.id_tamiz;'''	

    cursor1.execute(sql)

    #aca se busca la info de la ubicacion con la funcion, pero solo pata aquellas filas cuyo cp este bien
    sql='''select id_tamiz || ' ; ' || staging.ubicacion_cp_verificado(cp, colonia) \
    from staging.datos_radar_limpios_parcial \
    where (cp != '' and cp is not null) and (clave_colonia is null or clave_colonia = 0::bigint);'''

    cursor1.execute(sql)
    ubicaciones_funcion = list(cursor1.fetchall())

    sql='''update staging.datos_radar_limpios_parcial \
    set cp = %s, \
    clave_colonia = %s, \
	clave_ciudad = %s, \
	clave_municipio = %s, \
    clave_estado = %s \
    where id_tamiz = %s;'''

    i = 0    
    for fila in ubicaciones_funcion:
        
        #aca debo hacer lo de separar los campos que retornan el select para poder hacer el update                
        print(f'fila2: {fila}')
        if fila != None and fila[0] != None:
            campitos = fila[0].split(' ; ')
            id_tamiz = campitos[0]
            cp = campitos[1]
            clave_colonia = campitos[2]
            clave_ciudad = campitos[3]
            clave_municipio = campitos[4]
            clave_estado = campitos[5]
            valor = float(campitos[6]) 
            if valor<2.55:
                clave_colonia = 0
                
            filas.append([cp, clave_colonia, clave_ciudad, clave_municipio, clave_estado, id_tamiz])

        
    cursor1.executemany(sql, filas) #se usa executemany en lugar de execute y esto es porque lo que queremos agregar es la tupla
    conexion.commit()
   
    ########################## estado JALISCO BEGIN ##################################
    
    #aca se busca la info de la ubicacion con la funcion, pero solo pata aquellas filas cuyo eatado es JALISCO y no tiene la colonia
    sql='''select id_tamiz || ' ; ' || staging.ubicacion_cp_no_verificado_JALISCO(cp, colonia, ciudad) \
    from staging.datos_radar_limpios_parcial \
    where (estado = 'JALISCO' or estado_nuevo = 'JALISCO') and (clave_colonia is null or clave_colonia = 0::bigint);'''

    cursor1.execute(sql)
    ubicaciones_funcion = list(cursor1.fetchall())

    sql='''update staging.datos_radar_limpios_parcial \
    set cp = %s, \
    clave_colonia = %s, \
	clave_ciudad = %s, \
	clave_municipio = %s, \
    clave_estado = %s \
    where id_tamiz = %s;'''

    i = 0    
    for fila in ubicaciones_funcion:
        
        #aca debo hacer lo de separar los campos que retornan el select para poder hacer el update                
        print(f'fila: {fila}')
        if fila != None and fila[0] != None:
            campitos = fila[0].split(' ; ')
            id_tamiz = campitos[0]
            cp = campitos[1]
            clave_colonia = campitos[2]
            clave_ciudad = campitos[3]
            clave_municipio = campitos[4]
            clave_estado = campitos[5]
            valor = float(campitos[6]) 
            #ACA TENGO QUE VER CUAL ES EL MEJOR VALOR PSRS DECIR QUE ESTA BIEN ESE VALOR...
            if valor<2.55:
                clave_colonia = 0
                
            filas.append([cp, clave_colonia, clave_ciudad, clave_municipio, clave_estado, id_tamiz])

        
    cursor1.executemany(sql, filas) #se usa executemany en lugar de execute y esto es porque lo que queremos agregar es la tupla

    ########################## estado JALISCO END ####################################

    sql = 'TRUNCATE TABLE staging.datos_radar_limpios'
    cursor1.execute(sql)
    
    #aqui solo falta convertir las fechas desde varchar a date para insertar en la data final limpia
    sql = '''insert into staging.datos_radar_limpios \
    (id_tamiz, fecha_llamada, hora_llamada, emergencia, acepto_muestra, muestra_domicilio,  \
    apellido_paterno, apellido_materno, nombres, sexo, fecha_nacimiento, edad, nacionalidad,  \
    pais_origen, entidad_nacimiento, curp, telefono_uno, telefono_dos, ocupacion, embarazo,  \
    meses_embarazo, puerperio, dias_puerperio, entidad, calle, num_ext, num_int,  \
    codigo_postal, colonia, ciudad, estado, migrante, fecha_viaje, lugar_viaje,  \
    municipio_viaje, estado_viaje, pais_viaje, fecha_llegada, aerolinea, viaje_fuera,  \
    contacto_covid, valoracion, fecha_valoracion, total_sintomas, signo_alerta, laboratorio,  \
    fecha_muestra, resultado_laboratorio, sin_muestra, respiratoria, labios_morados, \
    dolor_pecho, pararse, convulsionese, fiebre, fecha_fiebre, tos_seca, fecha_tos_seca, \
    congestion_nasal, fecha_congestion_nasal, dolor_toracico_pecho, fecha_dolor_pecho, \
    cansancio_fatiga, fecha_fatiga, tos_con_espectoracion_flema, fecha_tos_flema, \
    dificultad_respiratoria, fecha_dificultad_respi, ataque_al_estado_general, \
    fecha_ataque_edo_gral, dolor_de_cabeza, fecha_dolor_de_cabeza, irritabilidad, \
    fecha_irritabilidad, escalofrios, fecha_escalofrios, dolor_muscular, fecha_dolor_muscular, \
    dolor_en_huesos, fecha_dolor_huesos, escurrimiento_nasal, fecha_escurrimiento_nasal, \
    ardor_de_garganta, fecha_ardor_garganta, gripa_o_resfriado, fecha_gripa, conjuntivitis, \
    fecha_conjuntivitis, diarrea, fecha_diarrea, vomito, fecha_vomito, dolor_abdominal, \
    fecha_dolor_abdominal, otro, fecha_otro, respiracion_rapida, fecha_respiracion_rapida, \
    convulsiones, fecha_convulsiones, diabetes, enf_pulmonar, asma, inmunosupresion, vih_sida, \
    enfermedad_cardiaca, obesidad, hipertension_arterial, insuf_renal_cronica, tabaquismo, \
    cancer, enfermedad_hepatica, desconoce, ninguno, otra_enfermedad, personal_de_salud, \
    email, fech_prim_sint, cp, clave_estado, clave_municipio, clave_ciudad, clave_colonia, \
    clave_entidad_nacimiento, clave_estado_viaje, clave_municipio_viaje, \
    clave_pais_viaje, clave_pais_origen, edad_calc) \
    SELECT dr4.id_tamiz, to_date(left(dr4.fecha_llamada, 10), 'YYYY-MM-DD'), dr4.hora_llamada,  \
    dr4.emergencia::integer, dr4.acepto_muestra::integer, dr4.muestra_domicilio::integer, dr4.apellido_paterno,  \
    dr4.apellido_materno, dr4.nombres, dr4.sexo::integer,  \
    to_date(replace(left(dr4.fecha_nacimiento, 10), '-', '/'), 'YYYY-MM-DD'),  staging.is_integer(dr4.edad),  \
    dr4.nacionalidad::integer, dr4.pais_origen, dr4.entidad_nacimiento, dr4.curp, dr4.telefono_uno,  \
    dr4.telefono_dos, dr4.ocupacion, dr4.embarazo::integer, staging.is_integer(dr4.meses_embarazo), dr4.puerperio::integer,  \
    staging.is_integer(dr4.dias_puerperio), dr4.entidad, dr4.calle, dr4.num_ext, dr4.num_int, dr4.codigo_postal,  \
    dr4.colonia, dr4.ciudad, dr4.estado, dr4.migrante::integer, to_date(left(dr4.fecha_viaje, 10), 'YYYY-MM-DD'),  \
    dr4.lugar_viaje, dr4.municipio_viaje, dr4.estado_viaje, dr4.pais_viaje,  \
    to_date(left(dr4.fecha_llegada, 10), 'YYYY-MM-DD'), dr4.aerolinea, dr4.viaje_fuera::integer,  \
    dr4.contacto_covid::integer, dr4.valoracion::integer, to_date(left(dr4.fecha_valoracion, 10), 'YYYY-MM-DD'),  \
    dr4.total_sintomas::integer, dr4.signo_alerta::integer, dr4.laboratorio::integer, to_date(left(dr4.fecha_muestra, 10), 'YYYY-MM-DD'),  \
    dr4.resultado_laboratorio::integer, dr4.sin_muestra::integer, dr4.respiratoria::integer, dr4.labios_morados::integer,  \
    dr4.dolor_pecho::integer, dr4.pararse::integer, dr4.convulsionese::integer, dr4.fiebre::integer,  \
    to_date(left(dr4.fecha_fiebre, 10), 'YYYY-MM-DD'), dr4.tos_seca::integer,  \
    to_date(left(dr4.fecha_tos_seca, 10), 'YYYY-MM-DD'), dr4.congestion_nasal::integer,  \
    to_date(left(dr4.fecha_congestion_nasal, 10), 'YYYY-MM-DD'), dr4.dolor_toracico_pecho::integer,  \
    to_date(left(dr4.fecha_dolor_pecho, 10), 'YYYY-MM-DD'), dr4.cansancio_fatiga::integer,  \
    to_date(left(dr4.fecha_fatiga, 10), 'YYYY-MM-DD'), dr4.tos_con_espectoracion_flema::integer,  \
    to_date(left(dr4.fecha_tos_flema, 10), 'YYYY-MM-DD'), dr4.dificultad_respiratoria::integer,  \
    to_date(left(dr4.fecha_dificultad_respi, 10), 'YYYY-MM-DD'), dr4.ataque_al_estado_general::integer,  \
    to_date(left(dr4.fecha_ataque_edo_gral, 10), 'YYYY-MM-DD'), dr4.dolor_de_cabeza::integer,  \
    to_date(left(dr4.fecha_dolor_de_cabeza, 10), 'YYYY-MM-DD'), dr4.irritabilidad::integer,  \
    to_date(left(dr4.fecha_irritabilidad, 10), 'YYYY-MM-DD'), dr4.escalofrios::integer,  \
    to_date(left(dr4.fecha_escalofrios, 10), 'YYYY-MM-DD'), dr4.dolor_muscular::integer,  \
    to_date(left(dr4.fecha_dolor_muscular, 10), 'YYYY-MM-DD'), dr4.dolor_en_huesos::integer,  \
    to_date(left(dr4.fecha_dolor_huesos, 10), 'YYYY-MM-DD'), dr4.escurrimiento_nasal::integer,  \
    to_date(left(dr4.fecha_escurrimiento_nasal, 10), 'YYYY-MM-DD'), dr4.ardor_de_garganta::integer,  \
    to_date(left(dr4.fecha_ardor_garganta, 10), 'YYYY-MM-DD'), dr4.gripa_o_resfriado::integer,  \
    to_date(left(dr4.fecha_gripa, 10), 'YYYY-MM-DD'), dr4.conjuntivitis::integer,  \
    to_date(left(dr4.fecha_conjuntivitis, 10), 'YYYY-MM-DD'), dr4.diarrea::integer,  \
    to_date(left(dr4.fecha_diarrea, 10), 'YYYY-MM-DD'), dr4.vomito::integer,  \
    to_date(left(dr4.fecha_vomito, 10), 'YYYY-MM-DD'), dr4.dolor_abdominal::integer,  \
    to_date(left(dr4.fecha_dolor_abdominal, 10), 'YYYY-MM-DD'), dr4.otro::integer,  \
    to_date(left(dr4.fecha_otro, 10), 'YYYY-MM-DD'), dr4.respiracion_rapida::integer,  \
    to_date(left(dr4.fecha_respiracion_rapida, 10), 'YYYY-MM-DD'), dr4.convulsiones::integer,  \
    to_date(left(dr4.fecha_convulsiones, 10), 'YYYY-MM-DD'), dr4.diabetes::integer,  \
    dr4.enf_pulmonar::integer, dr4.asma::integer, dr4.inmunosupresion::integer, dr4.vih_sida::integer, dr4.enfermedad_cardiaca::integer,  \
    dr4.obesidad::integer, dr4.hipertension_arterial::integer, dr4.insuf_renal_cronica::integer, dr4.tabaquismo::integer,  \
    dr4.cancer::integer, dr4.enfermedad_hepatica::integer, dr4.desconoce::integer, dr4.ninguno::integer, dr4.otra_enfermedad::integer,  \
    dr4.personal_de_salud::integer, dr4.email, to_date(left(dr4.fech_prim_sint, 10), 'YYYY-MM-DD'),  \
    dr4.cp, dr4.clave_estado::integer, dr4.clave_municipio::integer, dr4.clave_ciudad::integer, dr4.clave_colonia::bigint, \
    dr4.clave_entidad_nacimiento::integer, dr4.clave_estado_viaje::integer, dr4.clave_municipio_viaje::integer, \
    dr4.clave_pais_viaje::integer, dr4.clave_pais_origen::integer, dr4.edad_calc::integer \
    FROM \
    staging.datos_radar_limpios_parcial dr4 \
    INNER JOIN \
    (SELECT  \
    dr2.apellido_paterno, \
    dr2.apellido_materno, \
    dr2.nombres, \
    dr2.curp, \
    dr2.resultado_laboratorio, \
    dr2.fecha_llamada, \
    MAX(dr2.hora_llamada) AS hora_llamada \
    FROM staging.datos_radar_limpios_parcial dr2 \
    INNER JOIN \
    (SELECT  \
    drlp.apellido_paterno, \
    drlp.apellido_materno, \
    drlp.nombres, \
    drlp.curp, \
    drlp.resultado_laboratorio, \
    MAX(drlp.fecha_llamada) AS fecha_llamada \
    FROM staging.datos_radar_limpios_parcial drlp \
    WHERE drlp.fecha_llamada IS NOT null and drlp.fecha_llamada != '' \
    GROUP BY \
    drlp.apellido_paterno, \
    drlp.apellido_materno, \
    drlp.nombres, \
    drlp.curp, \
    drlp.resultado_laboratorio) dr1 \
    ON dr2.apellido_paterno = dr1.apellido_paterno  \
    AND dr2.apellido_materno = dr1.apellido_materno \
    AND dr2.nombres = dr1.nombres \
    AND dr2.curp = dr1.curp \
    AND dr2.resultado_laboratorio = dr1.resultado_laboratorio \
    AND dr2.fecha_llamada = dr1.fecha_llamada \
    WHERE dr2.fecha_llamada IS NOT null and dr2.fecha_llamada != '' \
    GROUP BY  \
    dr2.apellido_paterno, \
    dr2.apellido_materno, \
    dr2.nombres, \
    dr2.curp, \
    dr2.resultado_laboratorio, \
    dr2.fecha_llamada) dr3 \
    ON \
    dr3.apellido_paterno = dr4.apellido_paterno \
    AND dr3.apellido_materno = dr4.apellido_materno \
    AND dr3.nombres = dr4.nombres \
    AND dr3.curp = dr4.curp \
    AND dr3.resultado_laboratorio = dr4.resultado_laboratorio \
    AND dr3.fecha_llamada = dr4.fecha_llamada \
    AND dr3.hora_llamada = dr4.hora_llamada;'''

    #aqui llama al insert...
    cursor1.execute(sql)
    
    #aqui se refrescan las vistas:
    sql = 'REFRESH MATERIALIZED VIEW staging.fecha_municipio_acum WITH DATA;'
    #cursor1.execute(sql)
    
    sql = 'REFRESH MATERIALIZED VIEW staging.fechas_x_municipios WITH DATA;'
    #cursor1.execute(sql)

    sql = 'REFRESH MATERIALIZED VIEW staging.vista_11 WITH DATA;'
    #cursor1.execute(sql)
    
    sql = 'REFRESH MATERIALIZED VIEW staging.vista_12 WITH DATA;'
    #cursor1.execute(sql)

    sql = 'REFRESH MATERIALIZED VIEW staging.vista_13 WITH DATA;'
    #cursor1.execute(sql)

    sql = 'REFRESH MATERIALIZED VIEW staging.vista_17_por_dia WITH DATA;'
    #cursor1.execute(sql)

except Exception as e:
    print("ocurrio un error en el insert")
    print(e)
finally:
    conexion.commit()
    cursor1.close()
    conexion.close()
    print(f'termino de ejecutarse: {dt.now()}')

    
    