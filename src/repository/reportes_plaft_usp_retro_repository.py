from ..utils.database_executes import execute_query_to_df,bulk_insert_from_df,execute_query_no_results,execute_query_with_results
from ..utils.logger import logger
import pandas as pd
from datetime import datetime

def obtener_polizas_alloy():
    logger.info('obtener_polizas_alloy - inicio')
    try:
        dfAlloy = execute_query_to_df("SELECT idproducto, evento, idpoliza, idoperacion, idenviosme FROM interseguror.impmas WHERE idenviosme IS NULL AND tipoenvio = 'E' AND evento IN('Emitir', 'EmitirRapida', 'EmitirPropuesta') AND NOT (APMATERNO IS NULL AND APPATERNO IS NULL AND SEGUNDONOMBRE IS NULL AND PRIMERNOMBRE IS NULL) AND email is not null", 'pg')
        logger.info(f"obtener_polizas_alloy - => {len(dfAlloy)}")
    except Exception as e:
        logger.error(f"Error en obtener_polizas_alloy: {str(e)}")
        dfAlloy = pd.DataFrame()  # Devolver un DataFrame vacío o manejar de otra manera
    logger.info('obtener_polizas_alloy - fin')
    return dfAlloy

def obtener_polizas_sme():
  logger.info('obtener_polizas_sme - inicio')
  dfSme = execute_query_to_df("SELECT idproducto ,evento ,idpoliza ,idoperacion ,idenviosme FROM dbo.sme_impmas WHERE idenviosme IS NOT NULL", 'mssql')
  logger.info(f"obtener_polizas_sme - => {len(dfSme.to_dict(orient='records'))}")
  logger.info('obtener_polizas_sme - fin')
  return dfSme

def insertar_polizas_temporal(df):
  logger.info('insertar_polizas_temporal - inicio')
  columns = ('idproducto', 'idpoliza', 'idenviosme', 'idoperacion', 'evento')
  bulk_insert_from_df(df, 'impmas_temp_envio','interseguror',columns,'pg')
  logger.info('insertar_polizas_temporal - fin')
  return True

def limpiar_temporal(tabla):
  logger.info(f'limpiar_temporal {tabla} - inicio')
  execute_query_no_results("TRUNCATE TABLE " + tabla + " RESTART IDENTITY CASCADE;",'pg')
  logger.info(f'limpiar_temporal {tabla} - fin')
  return True

def update_impmas_desde_temp():
  logger.info('update_impmas_desde_temp - inicio')
  res=execute_query_with_results("UPDATE interseguror.impmas AS dest SET idenviosme = src.idenviosme FROM interseguror.impmas_temp_envio AS src WHERE dest.idproducto = src.idproducto AND dest.idpoliza = src.idpoliza AND dest.idoperacion = src.idoperacion AND dest.evento = src.evento AND dest.idenviosme IS DISTINCT FROM src.idenviosme;",'pg')
  logger.info('update_impmas_desde_temp - fin')
  return res


def registrar_log_interno(p_observacion, p_nivel=1):
    logger.info(f'registrar_log_interno - {p_observacion} - inicio') 

    # Variables locales
    v_sangria = ""
    v_observacion_final = ""

    # Definir el nivel de sangría
    if p_nivel == 1:
        v_sangria = ""
    elif p_nivel == 2:
        v_sangria = " "
    elif p_nivel == 3:
        v_sangria = "   "

    # Crear la observación final
    v_observacion_final = f"{v_sangria}{p_observacion}"

    execute_query_no_results(f"""
        INSERT INTO INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO (
            ID_LOG_PRO_INTERNO,
            OBSERVACION,
            FECHA_PROCESO ) 
        VALUES (
            NEXTVAL('INTERSEGUROR.SEQ_PLAFT_LOG_PRO_INT'),
            '{ v_observacion_final }',
            '{ datetime.now() }'
        );
    """,'pg')

    logger.info(f'registrar_log_interno - {p_observacion} - fin')

    return True


def usp_limpiar_indices():

    logger.info(f'usp_limpiar_indices - inicio') 

    # Lista de nombres de índices a verificar y eliminar si existen
    index_names = [
        'idx_plf_trans_01',
        'idx_plf_trans_02',
        'idx_plf_trans_03',
        'idx_plf_trans_04',
        'idx_plf_trans_05',
        'idx_plf_trans_06',
        'idx_plf_trans_07',
        'idx_plf_trans_08',
        'idx_plf_trans_09'
    ]
    
    # Esquema donde se encuentran los índices
    schema = 'interseguror'
    
    for index_name in index_names:
        try:
            logger.info(f"Verificando existencia del índice: {schema}.{index_name}")
            
            # Consulta para verificar si el índice existe utilizando pg_indexes
            query_check_index = f"""
                SELECT COUNT(1) 
                FROM pg_indexes 
                WHERE schemaname = '{schema}' 
                  AND indexname = '{index_name}';
            """
            
            # Ejecutar la consulta y obtener el resultado
            V_CANT_REG = execute_query_with_results(query_check_index, 'pg')
            
            # Verificar si el índice existe
            if V_CANT_REG and V_CANT_REG[0][0] > 0:
                logger.info(f"El índice {schema}.{index_name} existe. Procediendo a eliminarlo.")
                
                # Consulta para eliminar el índice
                query_drop_index = f"DROP INDEX {schema}.{index_name};"
                
                # Ejecutar la consulta de eliminación
                execute_query_no_results(query_drop_index, 'pg')
                
                logger.info(f"Índice {schema}.{index_name} eliminado exitosamente.")
            else:
                logger.info(f"El índice {schema}.{index_name} no existe. No se requiere eliminación.")
        
        except Exception as e:
            logger.error(f"Error al procesar el índice {schema}.{index_name}: {e}")
            continue
    
    logger.info(f'usp_limpiar_indices - fin') 


def usp_crear_indices():

    logger.info('Ejecutando función para crear índices en interseguror.plaft_transaccional')
    
    # Lista de scripts que representan las consultas para crear índices
    scripts = [
        'create index idx_plf_trans_01 on interseguror.plaft_transaccional (departamento_eval)',
        'create index idx_plf_trans_02 on interseguror.plaft_transaccional (numero_documento_EVAL)',
        'create index idx_plf_trans_03 on interseguror.plaft_transaccional (id_departamento)',
        'create index idx_plf_trans_04 on interseguror.plaft_transaccional (cod_tipo_documento_eval)',
        'create index idx_plf_trans_05 on interseguror.plaft_transaccional (cod_tipo_documento_eval, numero_documento_eval)'
    ]
    
    # Ejecutar cada script usando el método execute_query_no_results
    for script in scripts:
        try:
            logger.info(f'Ejecutando script: {script}')
            execute_query_no_results(script, 'pg')
        except Exception as e:
            logger.error(f'Error al ejecutar script: {script}, Error: {str(e)}')
            raise e

    logger.info('Todos los índices se han creado correctamente.')


def update_actividad_economica_transaccional():
    logger.info(f'update_actividad_economica_transaccional - inicio') 

    execute_query_no_results(f"""
        UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
        SET    COD_ACTIVIDAD_ECONOMICA = NULLIF(TRIM(T.COD_ACTIVIDAD_ECONOMICA),'');
    """,'pg')

    logger.info(f'update_actividad_economica_transaccional - fin')

    return True

    
def usp_retro_det_activo():

    logger.info(f'usp_retro_det_activo - inicio') 

    query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
    execute_query_no_results(query_truncate_table, 'pg')

    # 1.1. LIMPIAR INDICES
    #registrar_log_interno('1.1. LIMPIAR INDICES - INICIO', 2)

    #usp_limpiar_indices()

    #registrar_log_interno('1.1. LIMPIAR INDICES - FIN', 2)

    # 1.2. CREANDO TABLA TEMPORAL
    registrar_log_interno('1.2. CREANDO TABLA TEMPORAL - INICIO', 2)
    
    query_create_table = """
    INSERT INTO interseguror.plaft_transaccional_tmp
    SELECT
        ID_REP_GENERAL, NUMERO_POLIZA, LINEA_NEGOCIO, COD_MONEDA, MONTO_PRIMA,
        COD_FRECUENCIA_PAGO, MONTO_PRIMA_TOTAL, NOMBRE_RAZON_SOCIAL, APE_PATERNO,
        APE_MATERNO, COD_TIPO_DOCUMENTO, NUMERO_DOCUMENTO, TIPO_CLIENTE,
        NACIONALIDAD, TIPO_PERSONA, COD_RAMO, COD_SUBRAMO, COD_PRODUCTO,
        COD_PRODUCTO_SBS, COD_ACTIVIDAD_ECONOMICA, ACTIVIDAD_ECONOMICA,
        ID_REGIMEN, FEC_EMISION_POLIZA, FEC_INICIO_VIGENCIA, FEC_FIN_VIGENCIA,
        ORIGEN, DEPARTAMENTO, NACIONALIDAD_EVAL, DEPARTAMENTO_EVAL, ID_DEPARTAMENTO,
        GLOSA_PRODUCTO, ID_RIESGO_SBS, ID_PRODUCTO_KEY, FECHA_NACIMIENTO,
        EDAD_ACTUARIAL, PERIODO_PAGO, MONTO_PRIMA_RECAUDADA,
        ID_ACTIVIDAD_ECONOMICA_EVAL, MONTO_PRIMA_TOTAL_SOLES, COD_TIPO_DOCUMENTO_EVAL,
        NUMERO_DOCUMENTO_EVAL, OBSERVACION, '-' as REGLAS, FECHA_CREACION,
        FECHA_MODIFICACION, EXISTE_EN_PLAFT, 1 as ACTIVO, NOMBRE_RAZON_SOCIAL_EVAL,
        APE_PATERNO_EVAL, APE_MATERNO_EVAL, COD_ACTIVIDAD_ECONOMICA_EVAL,
        ID_REGIMEN_EVAL, NUMERO_POLIZA_MATRIZ_EVAL, ARCHIVO_EXCEL_ORIGEN, ESTADO_POLIZA
    FROM interseguror.plaft_transaccional;
    """
    execute_query_no_results(query_create_table, 'pg')

    registrar_log_interno('1.2. CREANDO TABLA TEMPORAL - FIN', 2)

    # Vacía la tabla PLAFT_TRANSACCIONAL
    query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional;"
    execute_query_no_results(query_truncate_table, 'pg')

    # 1.3. RECARGANDO LA TABLA
    registrar_log_interno('1.3. RECARGANDO LA TABLA - INICIO', 2)
    
    query_insert_data = """
    INSERT INTO interseguror.plaft_transaccional
    SELECT * FROM interseguror.plaft_transaccional_tmp;
    """
    execute_query_no_results(query_insert_data, 'pg')

    registrar_log_interno('1.3. RECARGANDO LA TABLA - FIN', 2)

    query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
    execute_query_no_results(query_truncate_table, 'pg')

    logger.info(f'usp_retro_det_activo - fin')


def usp_retro_det_add_aseg_dit():

    logger.info(f'usp_retro_det_add_aseg_dit - inicio') 

    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    # 2.2. ELIMINANDO REGISTROS ESPECÍFICOS
    try:
        registrar_log_interno('2.2 ELIMINANDO LOS ASEGURADOS REGISTRADOS EN PROCESO - INICIO', 2)

        # Eliminar registros donde OBSERVACION LIKE '%REGISTRA ASEGURADO POR DESG_TARJ_INDIV%'
        query_delete = f"""
            DELETE FROM interseguror.plaft_transaccional 
            WHERE OBSERVACION LIKE '%REGISTRA ASEGURADO POR DESG_TARJ_INDIV%';
        """
        execute_query_no_results(query_delete, 'pg')
        logger.info("Registros específicos eliminados exitosamente.")

        registrar_log_interno('2.2 ELIMINANDO LOS ASEGURADOS REGISTRADOS EN PROCESO - FIN', 2)

        logger.info("Eliminación de registros específicos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la eliminación de registros específicos: {e}")
        raise

    # 2.3. CARGANDO TEMPORAL
    try:
        logger.info("Iniciando carga temporal - Inicio")

        registrar_log_interno('2.3 CARGANDO TEMPORTAL - INICIO', 2)
        
        query_insert_temp = f"""
            INSERT INTO interseguror.plaft_transaccional_tmp
            SELECT
            ID_REP_GENERAL ,
            numero_poliza                ,
            linea_negocio                ,
            cod_moneda                   ,
            monto_prima                  ,
            cod_frecuencia_pago          ,
            monto_prima_total            ,
            nombre_razon_social          ,
            ape_paterno                  ,
            ape_materno                  ,
            cod_tipo_documento           ,
            numero_documento             ,
            tipo_cliente                 ,
            nacionalidad                 ,
            tipo_persona                 ,
            cod_ramo                     ,
            cod_subramo                  ,
            cod_producto                 ,
            cod_producto_sbs             ,
            cod_actividad_economica      ,
            actividad_economica          ,
            id_regimen                   ,
            fec_emision_poliza           ,
            fec_inicio_vigencia          ,
            fec_fin_vigencia             ,
            origen                       ,
            departamento                 ,
            nacionalidad_eval            ,
            departamento_eval            ,
            id_departamento              ,
            glosa_producto               ,
            id_riesgo_sbs                ,
            id_producto_key              ,
            fecha_nacimiento             ,
            edad_actuarial               ,
            periodo_pago                 ,
            monto_prima_recaudada        ,
            id_actividad_economica_eval  ,
            monto_prima_total_soles      ,
            cod_tipo_documento_eval      ,
            numero_documento_eval        ,
            observacion                  ,
            '-' as reglas              ,
            fecha_creacion               ,
            fecha_modificacion           ,
            existe_en_plaft              ,
            1 as activo                  ,
            nombre_razon_social_eval     ,
            ape_paterno_eval             ,
            ape_materno_eval             ,
            cod_actividad_economica_eval ,
            id_regimen_eval              ,
            numero_poliza_matriz_eval    ,
            archivo_excel_origen         ,
            estado_poliza
            FROM
            PLAFT_TRANSACCIONAL T
            UNION ALL
            SELECT
            ID_REP_GENERAL ,
            numero_poliza                ,
            linea_negocio                ,
            cod_moneda                   ,
            monto_prima                  ,
            cod_frecuencia_pago          ,
            monto_prima_total            ,
            nombre_razon_social          ,
            ape_paterno                  ,
            ape_materno                  ,
            cod_tipo_documento           ,
            numero_documento             ,
            'ASEGURADO' AS tipo_cliente  ,
            nacionalidad                 ,
            tipo_persona                 ,
            cod_ramo                     ,
            cod_subramo                  ,
            cod_producto                 ,
            cod_producto_sbs             ,
            cod_actividad_economica      ,
            actividad_economica          ,
            id_regimen                   ,
            fec_emision_poliza           ,
            fec_inicio_vigencia          ,
            fec_fin_vigencia             ,
            origen                       ,
            departamento                 ,
            nacionalidad_eval            ,
            departamento_eval            ,
            id_departamento              ,
            glosa_producto               ,
            id_riesgo_sbs                ,
            id_producto_key              ,
            fecha_nacimiento             ,
            edad_actuarial               ,
            periodo_pago                 ,
            monto_prima_recaudada        ,
            id_actividad_economica_eval  ,
            monto_prima_total_soles      ,
            cod_tipo_documento_eval      ,
            numero_documento_eval        ,
            'REGISTRA ASEGURADO POR DESG_TARJ_INDIV' observacion                  ,
            '-' as reglas              ,
            fecha_creacion               ,
            fecha_modificacion           ,
            existe_en_plaft              ,
            1 as activo                  ,
            nombre_razon_social_eval     ,
            ape_paterno_eval             ,
            ape_materno_eval             ,
            cod_actividad_economica_eval ,
            id_regimen_eval              ,
            numero_poliza_matriz_eval    ,
            archivo_excel_origen         ,
            estado_poliza
            FROM
            INTERSEGUROR.PLAFT_TRANSACCIONAL T
            WHERE T.tipo_cliente = 'CONTRATANTE' AND
                  T.GLOSA_PRODUCTO = 'DesgTarjetasIndividual'  ;
        """
        execute_query_no_results(query_insert_temp, 'pg')
        
        registrar_log_interno('2.3 CARGANDO TEMPORTAL - FIN', 2)

        logger.info("Carga temporal completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise

    # 2.3. MODIFICACIÓN Y TRUNCADO DE LA TABLA PRINCIPAL
    try:
        # Truncar la tabla principal
        query_truncate = f"TRUNCATE TABLE interseguror.plaft_transaccional;"
        execute_query_no_results(query_truncate, 'pg')
        logger.info(f"Tabla interseguror.plaft_transaccional truncada exitosamente.")
    except Exception as e:
        logger.error(f"Error durante la modificación o truncado de la tabla principal: {e}")
        raise

    # 2.4. RECARGANDO DATOS
    try:
        logger.info("Iniciando recarga de datos - Inicio")
        
        registrar_log_interno('2.4 RECARGANDO DATOS - INICIO', 2)
        
        # Insertar datos desde la tabla temporal a la tabla principal
        query_insert = f"""
            INSERT INTO interseguror.plaft_transaccional
            SELECT * FROM interseguror.plaft_transaccional_tmp;
        """
        execute_query_no_results(query_insert, 'pg')
        logger.info(f"Datos recargados exitosamente en la tabla interseguror.plaft_transaccional.")
        
        registrar_log_interno('2.4 RECARGANDO DATOS - FIN', 2)

        logger.info("Recarga de datos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la recarga de datos: {e}")
        raise

    # 3. Verifica si existe la tabla PLAFT_TRANSACCIONAL_TMP y la elimina si existe
    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')

        logger.info(f"La tabla interseguror.plaft_transaccional_tmp ha sido truncada")
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    logger.info(f'usp_retro_det_add_aseg_dit - fin') 



def usp_retro_det_add_aseg_soat():

    logger.info(f'usp_retro_det_add_aseg_soat - inicio') 

    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    # 3.2. ELIMINAR REGISTRADOS EN ESTE PROCESO(NO VIENE DE DATOS DE LA EXTRACCION)
    try:
        registrar_log_interno('3.2 ELIMINAR REGISTRADOS EN ESTE PROCESO - INICIO', 2)

        # Eliminar registros donde OBSERVACION LIKE '%REGISTRA ASEGURADO POR SOAT%'
        query_delete = f"""
            DELETE FROM interseguror.plaft_transaccional 
            WHERE OBSERVACION LIKE '%REGISTRA ASEGURADO POR SOAT%';
        """
        execute_query_no_results(query_delete, 'pg')
        logger.info("Registros específicos eliminados exitosamente.")

        registrar_log_interno('3.2 ELIMINAR REGISTRADOS EN ESTE PROCESO - FIN', 2)

        logger.info("Eliminación de registros específicos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la eliminación de registros específicos: {e}")
        raise

    # 3.3. CARGANDO TEMPORAL
    try:
        logger.info("Iniciando carga temporal - Inicio")

        registrar_log_interno('3.3. CARGANDO TEMPORAL - INICIO', 2)
        
        # Crear la tabla temporal PLAFT_TRANSACCIONAL_TMP
        query_create_temp = f"""
          INSERT INTO interseguror.plaft_transaccional_tmp
          SELECT
          ID_REP_GENERAL ,
          numero_poliza                ,
          linea_negocio                ,
          cod_moneda                   ,
          monto_prima                  ,
          cod_frecuencia_pago          ,
          monto_prima_total            ,
          nombre_razon_social          ,
          ape_paterno                  ,
          ape_materno                  ,
          cod_tipo_documento           ,
          numero_documento             ,
          tipo_cliente                 ,
          nacionalidad                 ,
          tipo_persona                 ,
          cod_ramo                     ,
          cod_subramo                  ,
          cod_producto                 ,
          cod_producto_sbs             ,
          cod_actividad_economica      ,
          actividad_economica          ,
          id_regimen                   ,
          fec_emision_poliza           ,
          fec_inicio_vigencia          ,
          fec_fin_vigencia             ,
          origen                       ,
          departamento                 ,
          nacionalidad_eval            ,
          departamento_eval            ,
          id_departamento              ,
          glosa_producto               ,
          id_riesgo_sbs                ,
          id_producto_key              ,
          fecha_nacimiento             ,
          edad_actuarial               ,
          periodo_pago                 ,
          monto_prima_recaudada        ,
          id_actividad_economica_eval  ,
          monto_prima_total_soles      ,
          cod_tipo_documento_eval      ,
          numero_documento_eval        ,
          observacion                  ,
          '-' as reglas              ,
          fecha_creacion               ,
          fecha_modificacion           ,
          existe_en_plaft              ,
          1 as activo                  ,
          nombre_razon_social_eval     ,
          ape_paterno_eval             ,
          ape_materno_eval             ,
          cod_actividad_economica_eval ,
          id_regimen_eval              ,
          numero_poliza_matriz_eval    ,
          archivo_excel_origen         ,
          estado_poliza
          FROM
          PLAFT_TRANSACCIONAL T
          UNION ALL
          SELECT
          ID_REP_GENERAL ,
          numero_poliza                ,
          linea_negocio                ,
          cod_moneda                   ,
          monto_prima                  ,
          cod_frecuencia_pago          ,
          monto_prima_total            ,
          nombre_razon_social          ,
          ape_paterno                  ,
          ape_materno                  ,
          cod_tipo_documento           ,
          numero_documento             ,
          'ASEGURADO' AS tipo_cliente  ,
          nacionalidad                 ,
          tipo_persona                 ,
          cod_ramo                     ,
          cod_subramo                  ,
          cod_producto                 ,
          cod_producto_sbs             ,
          cod_actividad_economica      ,
          actividad_economica          ,
          id_regimen                   ,
          fec_emision_poliza           ,
          fec_inicio_vigencia          ,
          fec_fin_vigencia             ,
          origen                       ,
          departamento                 ,
          nacionalidad_eval            ,
          departamento_eval            ,
          id_departamento              ,
          glosa_producto               ,
          id_riesgo_sbs                ,
          id_producto_key              ,
          fecha_nacimiento             ,
          edad_actuarial               ,
          periodo_pago                 ,
          monto_prima_recaudada        ,
          id_actividad_economica_eval  ,
          monto_prima_total_soles      ,
          cod_tipo_documento_eval      ,
          numero_documento_eval        ,
          'REGISTRA ASEGURADO POR SOAT' observacion                  ,
          '-' as reglas              ,
          fecha_creacion               ,
          fecha_modificacion           ,
          existe_en_plaft              ,
          1 as activo                  ,
          nombre_razon_social_eval     ,
          ape_paterno_eval             ,
          ape_materno_eval             ,
          cod_actividad_economica_eval ,
          id_regimen_eval              ,
          numero_poliza_matriz_eval    ,
          archivo_excel_origen         ,
          estado_poliza
          FROM
          INTERSEGUROR.PLAFT_TRANSACCIONAL T
          WHERE T.tipo_cliente = 'CONTRATANTE' AND
                T.COD_RAMO = '6.0' AND T.COD_SUBRAMO = '2.0' AND T.COD_PRODUCTO = '15.0';
        """
        execute_query_no_results(query_create_temp, 'pg')
        
        registrar_log_interno('3.3. CARGANDO TEMPORAL - FIN', 2)

        logger.info("Carga temporal completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise

    # 3.3. MODIFICACIÓN Y TRUNCADO DE LA TABLA PRINCIPAL
    try:
        # Truncar la tabla principal
        query_truncate = f"TRUNCATE TABLE interseguror.plaft_transaccional;"
        execute_query_no_results(query_truncate, 'pg')
        logger.info(f"Tabla interseguror.plaft_transaccional truncada exitosamente.")
    except Exception as e:
        logger.error(f"Error durante la modificación o truncado de la tabla principal: {e}")
        raise

    # 3.4. RECARGANDO DATOS
    try:
        logger.info("Iniciando recarga de datos - Inicio")
        
        registrar_log_interno('3.4. RECARGANDO TABLA - INICIO', 2)
        
        # Insertar datos desde la tabla temporal a la tabla principal
        query_insert = f"""
            INSERT INTO interseguror.plaft_transaccional
            SELECT * FROM interseguror.plaft_transaccional_tmp;
        """
        execute_query_no_results(query_insert, 'pg')
        logger.info(f"Datos recargados exitosamente en la tabla interseguror.plaft_transaccional.")
        
        registrar_log_interno('3.4. RECARGANDO TABLA - FIN', 2)

        logger.info("Recarga de datos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la recarga de datos: {e}")
        raise

    # 3.5. Verifica si existe la tabla PLAFT_TRANSACCIONAL_TMP y la elimina si existe
    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')

        logger.info(f"La tabla interseguror.plaft_transaccional_tmp ha sido truncada")
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    logger.info(f'usp_retro_det_add_aseg_soat - fin') 


def usp_retro_det_add_contra_pbi():

    logger.info(f'usp_retro_det_add_contra_pbi - inicio') 

    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    try:
        logger.info("Iniciando carga temporal tmp_polizas_sin_contra_pbip - Inicio")
        
        query_create_temp = f"""
        INSERT INTO interseguror.plaft_transaccional_tmp
        select distinct t.numero_poliza
        from   interseguror.plaft_transaccional t
        where  t.glosa_producto = 'ProtBlindajeIndividualPlus'
        except
        select distinct t.numero_poliza
        from   interseguror.plaft_transaccional t
        where  t.glosa_producto = 'ProtBlindajeIndividualPlus'
               and t.tipo_cliente = 'CONTRATANTE';
        """
        execute_query_no_results(query_create_temp, 'pg')
        logger.info(f"Tabla temporal interseguror.plaft_transaccional_tmp cargada exitosamente.")
        

        logger.info("Carga temporal completada tmp_polizas_sin_contra_pbip - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise


    try:
        logger.info(f"Iniciando insercion data interseguror.plaft_transaccional desde interseguror.plaft_transaccional - Inicio")
        
        query_create_temp = f"""
            insert into interseguror.plaft_transaccional(
              id_rep_general              ,
              numero_poliza               ,
              linea_negocio               ,
              cod_moneda                  ,
              monto_prima                 ,
              cod_frecuencia_pago         ,
              monto_prima_total           ,
              nombre_razon_social         ,
              ape_paterno                 ,
              ape_materno                 ,
              cod_tipo_documento          ,
              numero_documento            ,
              TIPO_CLIENTE                ,
              nacionalidad                ,
              tipo_persona                ,
              cod_ramo                    ,
              cod_subramo                 ,
              cod_producto                ,
              cod_producto_sbs            ,
              cod_actividad_economica     ,
              actividad_economica         ,
              id_regimen                  ,
              fec_emision_poliza          ,
              fec_inicio_vigencia         ,
              fec_fin_vigencia            ,
              origen                      ,
              departamento                ,
              nacionalidad_eval           ,
              departamento_eval           ,
              id_departamento             ,
              glosa_producto              ,
              id_riesgo_sbs               ,
              id_producto_key             ,
              fecha_nacimiento            ,
              edad_actuarial              ,
              periodo_pago                ,
              monto_prima_recaudada       ,
              id_actividad_economica_eval ,
              monto_prima_total_soles     ,
              REGLAS,
              activo

              )
        select
        id_rep_general              ,
        numero_poliza               ,
        linea_negocio               ,
        cod_moneda                  ,
        monto_prima                 ,
        cod_frecuencia_pago         ,
        monto_prima_total           ,
        nombre_razon_social         ,
        ape_paterno                 ,
        ape_materno                 ,
        cod_tipo_documento          ,
        numero_documento            ,
        'CONTRATANTE'               ,
        nacionalidad                ,
        tipo_persona                ,
        cod_ramo                    ,
        cod_subramo                 ,
        cod_producto                ,
        cod_producto_sbs            ,
        cod_actividad_economica     ,
        actividad_economica         ,
        id_regimen                  ,
        fec_emision_poliza          ,
        fec_inicio_vigencia         ,
        fec_fin_vigencia            ,
        origen                      ,
        departamento                ,
        nacionalidad_eval           ,
        departamento_eval           ,
        id_departamento             ,
        glosa_producto              ,
        id_riesgo_sbs               ,
        id_producto_key             ,
        fecha_nacimiento            ,
        edad_actuarial              ,
        periodo_pago                ,
        monto_prima_recaudada       ,
        id_actividad_economica_eval ,
        monto_prima_total_soles     ,
        'R001',
        activo
        from interseguror.plaft_transaccional t
        where t.numero_poliza in
        ( select  p.numero_poliza
          from    interseguror.plaft_transaccional_tmp p
        );
        """
        execute_query_no_results(query_create_temp, 'pg')
        
        logger.info(f"Iniciando insercion data interseguror.plaft_transaccional desde interseguror.plaft_transaccional_tmp - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise


    logger.info(f'usp_retro_det_add_contra_pbi - fin') 



def usp_retro_det_poliza_matriz():

    logger.info(f'usp_retro_det_poliza_matriz - inicio') 

    schema = 'interseguror'
    sequence_matriz = 'seq_plaft_log_pro_int'
    sequence_transaccional = 'seq_plaft_transaccional'
    table_main = 'plaft_transaccional'
    table_producto = 'plaft_d_producto'
    table_tmp = 'tmp_polizas_matrices_x_contratantes'

    try:        
        query_insert = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('{schema}.{sequence_matriz}'),'matriz-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante la insercion del log: {e}")
        raise


    try:        
        query_update = f"""
            UPDATE {schema}.{table_main} T
            set NUMERO_POLIZA_MATRIZ_EVAL = SWF_substr(T.numero_poliza,0,SWF_INSTR(T.numero_poliza,'-') -1) :: numeric
            FROM(SELECT P.ID_PRODUCTO_KEY FROM {schema}.{table_producto} P WHERE P.IND_COLECTIVO = 1) PP
            where T.ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY AND T.ORIGEN <> 'EXCEL';
        """
        execute_query_no_results(query_update, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante actualizacion de la tabla plaft_trasaccional: {e}")
        raise


    try:        
        query_insert = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('{schema}.{sequence_matriz}'),'matriz-FIN', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante la insercion del log: {e}")
        raise

    try:
        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')
    except Exception as e:
        logger.error(f"Error al truncar la tabla interseguror.plaft_transaccional_tmp: {e}")
        raise

    try:
        logger.info(f"Iniciando carga temporal {schema}.{table_tmp} - Inicio")
        
        query_create_temp = f"""
        INSERT INTO {schema}.{table_tmp}
        SELECT DISTINCT T.NUMERO_POLIZA_MATRIZ_EVAL, T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL
        FROM   {schema}.{table_main} T
        WHERE  T.NUMERO_POLIZA_MATRIZ_EVAL > 0
               AND T.ACTIVO = 1
               AND T.TIPO_CLIENTE = 'CONTRATANTE'
               AND T.ORIGEN <> 'EXCEL';
        """
        execute_query_no_results(query_create_temp, 'pg')
        logger.info(f"Tabla temporal {schema}.{table_tmp} cargada exitosamente.")
        

        logger.info(f"Carga temporal completada {schema}.{table_tmp} - Fin")
    except Exception as e:
        logger.error(f"Error durante la carga de la tabla temporal: {e}")
        raise


    try:        
        query_update = f"""
            UPDATE {schema}.{table_main} T set ACTIVO = 0,REGLAS = CONCAT(T.REGLAS,'-R0041') FROM(
              SELECT DISTINCT TMP.NUMERO_POLIZA_MATRIZ_EVAL FROM {schema}.{table_tmp} TMP
              ) PP where PP.NUMERO_POLIZA_MATRIZ_EVAL = T.NUMERO_POLIZA_MATRIZ_EVAL 
              AND T.ORIGEN <> 'EXCEL' AND T.TIPO_CLIENTE = 'CONTRATANTE';
        """
        execute_query_no_results(query_update, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante actualizacion de la tabla {schema}.{table_main}: {e}")
        raise

    try:        
        query_delete = f"""
            DELETE FROM {schema}.{table_main} T WHERE T.REGLAS LIKE '%-R040%';
        """
        execute_query_no_results(query_delete, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante delete de la tabla {schema}.{table_main}: {e}")
        raise


    try:        
        query_delete = f"""
            DELETE FROM {schema}.{table_main} T WHERE T.OBSERVACION LIKE '%REGISTRO DE CONTRATANTE PARA COLECTIVOS%';
        """
        execute_query_no_results(query_delete, 'pg')
        
    except Exception as e:
        logger.error(f"Error durante delete de la tabla {schema}.{table_main}: {e}")
        raise


    try:        
        query_select_tmp = f"""
            SELECT * FROM {schema}.{table_tmp};
        """
        tmp_records = execute_query_to_df(query_select_tmp, 'pg')

        for index, item in tmp_records.iterrows():
            numero_poliza_matriz_eval = item['numero_poliza_matriz_eval']
            cod_tipo_documento_eval = item['cod_tipo_documento_eval']
            numero_documento_eval = item['numero_documento_eval']
            
            logger.info(f"Iniciando inserción para poliza_matriz_eval: {numero_poliza_matriz_eval}")
            
            query_insert = f"""
                INSERT INTO {schema}.{table_main} (
                    ID_REP_GENERAL,
                    NUMERO_POLIZA,
                    LINEA_NEGOCIO,
                    COD_MONEDA,
                    MONTO_PRIMA,
                    COD_FRECUENCIA_PAGO,
                    MONTO_PRIMA_TOTAL,
                    NOMBRE_RAZON_SOCIAL,
                    APE_PATERNO,
                    APE_MATERNO,
                    COD_TIPO_DOCUMENTO,
                    NUMERO_DOCUMENTO,
                    TIPO_CLIENTE,
                    NACIONALIDAD,
                    TIPO_PERSONA,
                    COD_RAMO,
                    COD_SUBRAMO,
                    COD_PRODUCTO,
                    COD_PRODUCTO_SBS,
                    COD_ACTIVIDAD_ECONOMICA,
                    ACTIVIDAD_ECONOMICA,
                    ID_REGIMEN,
                    FEC_EMISION_POLIZA,
                    FEC_INICIO_VIGENCIA,
                    FEC_FIN_VIGENCIA,
                    ORIGEN,
                    DEPARTAMENTO,
                    NACIONALIDAD_EVAL,
                    DEPARTAMENTO_EVAL,
                    ID_DEPARTAMENTO,
                    GLOSA_PRODUCTO,
                    ID_RIESGO_SBS,
                    ID_PRODUCTO_KEY,
                    FECHA_NACIMIENTO,
                    EDAD_ACTUARIAL,
                    PERIODO_PAGO,
                    MONTO_PRIMA_RECAUDADA,
                    ID_ACTIVIDAD_ECONOMICA_EVAL,
                    MONTO_PRIMA_TOTAL_SOLES,
                    COD_TIPO_DOCUMENTO_EVAL,
                    NUMERO_DOCUMENTO_EVAL,
                    OBSERVACION,
                    REGLAS,
                    FECHA_CREACION,
                    FECHA_MODIFICACION,
                    EXISTE_EN_PLAFT,
                    ACTIVO,
                    NOMBRE_RAZON_SOCIAL_EVAL,
                    APE_PATERNO_EVAL,
                    APE_MATERNO_EVAL,
                    COD_ACTIVIDAD_ECONOMICA_EVAL,
                    ID_REGIMEN_EVAL,
                    NUMERO_POLIZA_MATRIZ_EVAL
                )
                SELECT
                    NEXTVAL('{schema}.{sequence_transaccional}'),
                    '{numero_poliza_matriz_eval}',
                    T.LINEA_NEGOCIO,
                    T.COD_MONEDA,
                    T.MONTO_PRIMA,
                    T.COD_FRECUENCIA_PAGO,
                    T.MONTO_PRIMA_TOTAL,
                    T.NOMBRE_RAZON_SOCIAL,
                    T.APE_PATERNO,
                    T.APE_MATERNO,
                    T.COD_TIPO_DOCUMENTO,
                    T.NUMERO_DOCUMENTO,
                    T.TIPO_CLIENTE,
                    T.NACIONALIDAD,
                    T.TIPO_PERSONA,
                    T.COD_RAMO,
                    T.COD_SUBRAMO,
                    T.COD_PRODUCTO,
                    T.COD_PRODUCTO_SBS,
                    T.COD_ACTIVIDAD_ECONOMICA,
                    T.ACTIVIDAD_ECONOMICA,
                    T.ID_REGIMEN,
                    T.FEC_EMISION_POLIZA,
                    T.FEC_INICIO_VIGENCIA,
                    T.FEC_FIN_VIGENCIA,
                    T.ORIGEN,
                    T.DEPARTAMENTO,
                    T.NACIONALIDAD_EVAL,
                    T.DEPARTAMENTO_EVAL,
                    T.ID_DEPARTAMENTO,
                    T.GLOSA_PRODUCTO,
                    T.ID_RIESGO_SBS,
                    T.ID_PRODUCTO_KEY,
                    T.FECHA_NACIMIENTO,
                    T.EDAD_ACTUARIAL,
                    T.PERIODO_PAGO,
                    T.MONTO_PRIMA_RECAUDADA,
                    T.ID_ACTIVIDAD_ECONOMICA_EVAL,
                    T.MONTO_PRIMA_TOTAL_SOLES,
                    T.COD_TIPO_DOCUMENTO_EVAL,
                    T.NUMERO_DOCUMENTO_EVAL,
                    'REGISTRO DE CONTRATANTE PARA COLECTIVOS',
                    '-R040',
                    T.FECHA_CREACION,
                    T.FECHA_MODIFICACION,
                    T.EXISTE_EN_PLAFT,
                    1,
                    T.NOMBRE_RAZON_SOCIAL_EVAL,
                    T.APE_PATERNO_EVAL,
                    T.APE_MATERNO_EVAL,
                    T.COD_ACTIVIDAD_ECONOMICA_EVAL,
                    T.ID_REGIMEN_EVAL,
                    '{numero_poliza_matriz_eval}'
                FROM {schema}.{table_main} T
                WHERE 
                    T.NUMERO_POLIZA_MATRIZ_EVAL = '{numero_poliza_matriz_eval}'
                    AND T.COD_TIPO_DOCUMENTO_EVAL = '{cod_tipo_documento_eval}'
                    AND T.NUMERO_DOCUMENTO_EVAL = '{numero_documento_eval}'
                    AND T.TIPO_CLIENTE = 'CONTRATANTE'
                LIMIT 1;
            """
            execute_query_no_results(query_insert, 'pg')
            logger.info(f"Inserción para poliza_matriz_eval: {numero_poliza_matriz_eval} completada.")


    except Exception as e:
        logger.error(f"Error durante la insercion para la tabla {schema}.{table_main}: {e}")
        raise
    

    logger.info(f'usp_retro_det_poliza_matriz - fin') 



def usp_retro_det_calc_acti_econo():

    logger.info(f'usp_retro_det_calc_acti_econo - inicio') 

    try:
        logger.info(f"Truncando la tabla interseguror.tmp_actividad_ecomica_acsele")
        query_truncate_table = "TRUNCATE TABLE interseguror.tmp_actividad_ecomica_acsele;"
        execute_query_no_results(query_truncate_table, 'pg')
        
        query_create_temp = f"""
            INSERT INTO interseguror.tmp_actividad_ecomica_acsele
          select TRANSFORMADORFILAID as CODIGO, DESCRIPTION as DESCRIPCION
          from interseguro.transformadorfila where PROPERTYID = 3706454;
        """
        execute_query_no_results(query_create_temp, 'pg')        

        query_update = f"""
            UPDATE interseguror.plaft_transaccional T set COD_ACTIVIDAD_ECONOMICA = PP.CODIGO FROM(
             SELECT * FROM interseguror.tmp_actividad_ecomica_acsele
           ) PP where UPPER(NULLIF(TRIM(coalesce(T.ACTIVIDAD_ECONOMICA,'-')),'')) = 
           UPPER(NULLIF(TRIM(PP.DESCRIPCION),'''')) AND T.ORIGEN IN('AS400','EXCEL');
        """
        execute_query_no_results(query_update, 'pg')

    except Exception as e:
        logger.error(f"Error en usp_retro_det_calc_acti_econo: {str(e)}")
        raise

    logger.info(f'usp_retro_det_calc_acti_econo - fin') 



def usp_retro_det_ini_valores():

    logger.info(f'usp_retro_det_ini_valores - inicio') 

    try:

        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')
        
        query_alter_sequence = f"""
            alter sequence interseguror.seq_plaft_transaccional restart start with 1;
        """
        execute_query_no_results(query_alter_sequence, 'pg')    
 
        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-01', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg') 

        query_create_temp = f"""
            INSERT INTO INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP
            SELECT
            NEXTVAL('INTERSEGUROR.SEQ_PLAFT_TRANSACCIONAL') as ID_REP_GENERAL ,
            NUMERO_POLIZA                ,
            LINEA_NEGOCIO                ,
            NULLIF(trim(COD_MONEDA),'') as COD_MONEDA    ,
            MONTO_PRIMA                  ,
            NULLIF(trim(COD_FRECUENCIA_PAGO),'') as COD_FRECUENCIA_PAGO  ,
            MONTO_PRIMA_TOTAL            ,
            NOMBRE_RAZON_SOCIAL          ,
            APE_PATERNO                  ,
            APE_MATERNO                  ,
            COD_TIPO_DOCUMENTO           ,
            NUMERO_DOCUMENTO             ,
            TIPO_CLIENTE                 ,
            NACIONALIDAD                 ,
            TIPO_PERSONA                 ,
            COD_RAMO                     ,
            COD_SUBRAMO                  ,
            COD_PRODUCTO                 ,
            NULLIF(TRIM(T.COD_PRODUCTO_SBS),'') AS COD_PRODUCTO_SBS             ,
            COD_ACTIVIDAD_ECONOMICA      ,
            NULLIF(TRIM(T.ACTIVIDAD_ECONOMICA),'') AS ACTIVIDAD_ECONOMICA          ,
            ID_REGIMEN                   ,
            FEC_EMISION_POLIZA           ,
            FEC_INICIO_VIGENCIA          ,
            FEC_FIN_VIGENCIA             ,
            ORIGEN                       ,
            DEPARTAMENTO                 ,
            UPPER(NULLIF(TRIM(coalesce(T.NACIONALIDAD,'DESCONOCIDA')),'')) AS NACIONALIDAD_EVAL            ,
            UPPER(NULLIF(TRIM(T.DEPARTAMENTO),'')) AS DEPARTAMENTO_EVAL            ,
            ID_DEPARTAMENTO              ,
            GLOSA_PRODUCTO               ,
            ID_RIESGO_SBS                ,
            ID_PRODUCTO_KEY              ,
            FECHA_NACIMIENTO             ,
            EDAD_ACTUARIAL               ,
            PERIODO_PAGO                 ,
            MONTO_PRIMA_RECAUDADA        ,
            -1 AS ID_ACTIVIDAD_ECONOMICA_EVAL  ,
            MONTO_PRIMA_TOTAL_SOLES      ,
            NULLIF(TRIM(coalesce(T.COD_TIPO_DOCUMENTO,'-1')),'') AS COD_TIPO_DOCUMENTO_EVAL      ,
            NULLIF(TRIM(coalesce(T.NUMERO_DOCUMENTO,'0')),'') AS NUMERO_DOCUMENTO_EVAL        ,
            OBSERVACION                  ,
            REGLAS                       ,
            FECHA_CREACION               ,
            FECHA_MODIFICACION           ,
            0 AS EXISTE_EN_PLAFT         ,
            ACTIVO                       ,
            NULLIF(TRIM(T.NOMBRE_RAZON_SOCIAL),'') AS NOMBRE_RAZON_SOCIAL_EVAL     ,
            NULLIF(TRIM(T.APE_PATERNO),'')  AS APE_PATERNO_EVAL             ,
            NULLIF(TRIM(T.APE_MATERNO),'') AS  APE_MATERNO_EVAL             ,
            NULLIF(TRIM(
             CASE WHEN coalesce(T.COD_ACTIVIDAD_ECONOMICA,'-1') = '-' THEN '-1'
              ELSE coalesce(T.COD_ACTIVIDAD_ECONOMICA,'-1') END
            ),    '') AS COD_ACTIVIDAD_ECONOMICA_EVAL ,
            ID_REGIMEN AS ID_REGIMEN_EVAL              ,
            NUMERO_POLIZA_MATRIZ_EVAL    ,
            ARCHIVO_EXCEL_ORIGEN         ,
            ESTADO_POLIZA
            FROM INTERSEGUROR.PLAFT_TRANSACCIONAL T;
        """
        execute_query_no_results(query_create_temp, 'pg') 

        query_insert_4 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-01-1', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_4, 'pg') 

        query_insert_5 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-02', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_5, 'pg')

        query_truncate = f"""
            TRUNCATE TABLE INTERSEGUROR.PLAFT_TRANSACCIONAL;
        """
        execute_query_no_results(query_truncate, 'pg') 

        query_insert_6 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-03', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_6, 'pg')

        query_insert_7 = f"""
            INSERT INTO INTERSEGUROR.PLAFT_TRANSACCIONAL
                    SELECT * FROM INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP;
        """
        execute_query_no_results(query_insert_7, 'pg') 

        query_insert_8 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-04', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_8, 'pg')

        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table_2 = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table_2, 'pg')
        
    except Exception as e:
        logger.error(f"Error en usp_retro_det_ini_valores: {str(e)}")
        raise

    logger.info(f'usp_retro_det_ini_valores - fin') 



def usp_retro_desactivar_aseg_previ():

    logger.info(f'usp_retro_desactivar_aseg_previ - inicio') 

    try:
        query_insert_1 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'INICIO-DESACTIVAR-ASEG-PREVI', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_1, 'pg')   

        # Obtener V_ID_PRODUCTO
        get_producto_query = """
            SELECT D.ID_PRODUCTO_KEY
            FROM INTERSEGUROR.PLAFT_D_PRODUCTO D
            WHERE D.COD_RAMO = '4'
            AND D.COD_SUBRAMO = '01'
            AND D.COD_PRODUCTO = '01';
        """
        df_producto = execute_query_to_df(get_producto_query, 'pg')
        if df_producto.empty:
            raise ValueError("No se encontró el ID_PRODUCTO")

        v_id_producto = df_producto.iloc[0, 0]
        logger.info(f"ID_PRODUCTO obtenido: {v_id_producto}")

        update_transaccional_query = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET ACTIVO = 0, REGLAS = CONCAT(T.REGLAS, ' - R060')
            FROM (
                SELECT P.ID_PRODUCTO_KEY 
                FROM INTERSEGUROR.PLAFT_D_PRODUCTO P 
                WHERE P.ID_PRODUCTO_KEY = {v_id_producto}
            ) PP
            WHERE T.ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY 
            AND T.TIPO_CLIENTE = 'ASEGURADO';
        """
        execute_query_no_results(update_transaccional_query, 'pg')

        query_insert_2 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'FIN-DESACTIVAR-ASEG-PREVI', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_2, 'pg')  
        
    except Exception as e:
        logger.error(f"Error en usp_retro_desactivar_aseg_previ: {str(e)}")
        raise

    logger.info(f'usp_retro_desactivar_aseg_previ - fin') 


def usp_retro_det_val_tipo_y_num_doc():

    logger.info(f'usp_retro_det_val_tipo_y_num_doc - inicio') 

    try:
        update_transaccional_query_1 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    COD_TIPO_DOCUMENTO_EVAL = 'DNI',REGLAS = CONCAT(T.REGLAS,'-','R002')
            WHERE  LENGTH(NULLIF(TRIM(TRANSLATE(T.NUMERO_DOCUMENTO_EVAL,' +-.0123456789',' ')),
            '')) is null
            AND T.COD_TIPO_DOCUMENTO_EVAL = '-1'
            AND length(T.NUMERO_DOCUMENTO_EVAL) = 8;
        """
        execute_query_no_results(update_transaccional_query_1, 'pg')

        update_transaccional_query_2 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    COD_TIPO_DOCUMENTO_EVAL = 'DNI',REGLAS = CONCAT(T.REGLAS,'-','R002')
            WHERE  LENGTH(NULLIF(TRIM(TRANSLATE(T.NUMERO_DOCUMENTO_EVAL,' +-.0123456789',' ')),
            '')) is null
            AND T.COD_TIPO_DOCUMENTO_EVAL = '-1'
            AND length(T.NUMERO_DOCUMENTO_EVAL) = 8;
        """
        execute_query_no_results(update_transaccional_query_2, 'pg')

        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'tipo de documento', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg')

        update_transaccional_query_3 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    NUMERO_DOCUMENTO_EVAL =  SWF_substr(CONCAT('000',T.NUMERO_DOCUMENTO_EVAL),-8),REGLAS = CONCAT(T.REGLAS,'-','R003')
            WHERE  T.COD_TIPO_DOCUMENTO_EVAL  = 'DNI'
            AND LENGTH(T.NUMERO_DOCUMENTO_EVAL) = 7
            AND LENGTH(NULLIF(TRIM(TRANSLATE(NUMERO_DOCUMENTO_EVAL,' +-.0123456789',' ')),'')) is null;
        """
        execute_query_no_results(update_transaccional_query_3, 'pg')

        update_transaccional_query_4 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    COD_TIPO_DOCUMENTO_EVAL = 'DNI',REGLAS = CONCAT(T.REGLAS,'-','R004')
            WHERE  T.COD_TIPO_DOCUMENTO_EVAL = 'LE'
            AND LENGTH(T.NUMERO_DOCUMENTO_EVAL) = 8
            AND LENGTH(NULLIF(TRIM(TRANSLATE(NUMERO_DOCUMENTO_EVAL,' +-.0123456789',' ')),'')) is null;
        """
        execute_query_no_results(update_transaccional_query_4, 'pg')

        update_transaccional_query_5 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T SET
            COD_TIPO_DOCUMENTO_EVAL = 'DNI',REGLAS = CONCAT(T.REGLAS,'-','R020')
            WHERE  UPPER(T.GLOSA_PRODUCTO) IN('DESGRAVAMENTARJETAS','DESGRAVAMENPERSONAL')
            AND T.COD_TIPO_DOCUMENTO_EVAL = '-1';
        """
        execute_query_no_results(update_transaccional_query_5, 'pg')

        query_insert_4 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'tipo de documento2', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_4, 'pg')

        update_transaccional_query_6 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set COD_TIPO_DOCUMENTO_EVAL = 'DNI',REGLAS = CONCAT(T.REGLAS,'-','R005')
            FROM(SELECT DISTINCT TT.NUMERO_DOCUMENTO_EVAL
            FROM(SELECT TU.NUMERO_DOCUMENTO_EVAL,
                                    COUNT(DISTINCT TU.COD_TIPO_DOCUMENTO_EVAL)
                FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL TU
                WHERE TU.NUMERO_DOCUMENTO_EVAL <> '0'
                GROUP BY TU.NUMERO_DOCUMENTO_EVAL
                HAVING COUNT(DISTINCT TU.COD_TIPO_DOCUMENTO_EVAL) > 1) TT
            INNER JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL TP ON TT.NUMERO_DOCUMENTO_EVAL = TP.NUMERO_DOCUMENTO_EVAL
            WHERE TP.COD_TIPO_DOCUMENTO_EVAL = 'DNI'
            AND TT.NUMERO_DOCUMENTO_EVAL <> '0'
            AND LENGTH(TT.NUMERO_DOCUMENTO_EVAL) = 8) B
            where B.NUMERO_DOCUMENTO_EVAL = T.NUMERO_DOCUMENTO_EVAL;
        """
        execute_query_no_results(update_transaccional_query_6, 'pg')

        query_insert_5 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'tipo de documento3', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_5, 'pg')

        update_transaccional_query_7 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set COD_TIPO_DOCUMENTO_EVAL = UNI.COD_TIPO_DOCUMENTO_EVAL,REGLAS = CONCAT(T.REGLAS,'-','R006')
            FROM(SELECT DISTINCT DUPLI.NUMERO_DOCUMENTO_EVAL, TD.COD_TIPO_DOCUMENTO_EVAL
            FROM(SELECT T.NUMERO_DOCUMENTO_EVAL, COUNT(DISTINCT T.COD_TIPO_DOCUMENTO_EVAL)
                FROM(SELECT DISTINCT T.NUMERO_DOCUMENTO_EVAL
                    FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL T
                    WHERE  T.COD_TIPO_DOCUMENTO_EVAL  = '-1'
                    AND T.NUMERO_DOCUMENTO_EVAL <> '0') TT
                LEFT JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL T
                ON TT.NUMERO_DOCUMENTO_EVAL = T.NUMERO_DOCUMENTO_EVAL
                WHERE T.COD_TIPO_DOCUMENTO_EVAL <> '-1'
                GROUP BY T.NUMERO_DOCUMENTO_EVAL
                HAVING COUNT(DISTINCT T.COD_TIPO_DOCUMENTO_EVAL) = 1) DUPLI
            INNER JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL TD ON DUPLI.NUMERO_DOCUMENTO_EVAL = TD.NUMERO_DOCUMENTO_EVAL
            WHERE TD.COD_TIPO_DOCUMENTO_EVAL <> '-1') UNI
            where T.NUMERO_DOCUMENTO_EVAL = UNI.NUMERO_DOCUMENTO_EVAL;
        """
        execute_query_no_results(update_transaccional_query_7, 'pg')

        query_insert_6 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'tipo de documento4', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_6, 'pg')


        # Actualización final con regla R007 y actualización basada en cliente con múltiples tipos de documento
        query = """
        SELECT DISTINCT T3.NUMERO_DOCUMENTO_EVAL, T3.COD_TIPO_DOCUMENTO_EVAL
        FROM INTERSEGUROR.PLAFT_TRANSACCIONAL T3
        INNER JOIN (
            SELECT DISTINCT T1.NUMERO_DOCUMENTO_EVAL, MAX(T1.FEC_INICIO_VIGENCIA) AS FEC_INICIO_VIGENCIA
            FROM INTERSEGUROR.PLAFT_TRANSACCIONAL T1
            INNER JOIN (
                SELECT NUMERO_DOCUMENTO_EVAL, COUNT(COD_TIPO_DOCUMENTO_EVAL) AS CANTIDAD
                FROM (
                    SELECT DISTINCT NUMERO_DOCUMENTO_EVAL, COD_TIPO_DOCUMENTO_EVAL
                    FROM INTERSEGUROR.PLAFT_TRANSACCIONAL
                    WHERE NUMERO_DOCUMENTO_EVAL <> '0'
                ) AS TABLA
                GROUP BY NUMERO_DOCUMENTO_EVAL
                HAVING COUNT(COD_TIPO_DOCUMENTO_EVAL) > 1
            ) T2 ON T2.NUMERO_DOCUMENTO_EVAL = T1.NUMERO_DOCUMENTO_EVAL
            WHERE T1.NUMERO_DOCUMENTO_EVAL <> '0'
            AND T1.COD_TIPO_DOCUMENTO_EVAL <> '-1'
            GROUP BY T1.NUMERO_DOCUMENTO_EVAL
        ) T4 ON T4.NUMERO_DOCUMENTO_EVAL = T3.NUMERO_DOCUMENTO_EVAL
        AND T4.FEC_INICIO_VIGENCIA = T3.FEC_INICIO_VIGENCIA
        """

        # Obtener resultados
        items = execute_query_with_results(query, 'pg')

        # Bucle para actualizar con los resultados del SELECT
        for item in items:
            execute_query_no_results(
                f"""UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL
                    SET COD_TIPO_DOCUMENTO_EVAL = '{item[1]}', REGLAS = CONCAT(REGLAS, '-', 'R007')
                    WHERE NUMERO_DOCUMENTO_EVAL = '{item[0]}'""", 
                'pg'
            )

        update_transaccional_query_8 = f"""
            update INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set    COD_TIPO_DOCUMENTO_EVAL = 'DNI'
            where  T.NUMERO_DOCUMENTO_EVAL = '25575911';
        """
        execute_query_no_results(update_transaccional_query_8, 'pg')


    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_tipo_y_num_doc: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_tipo_y_num_doc - fin') 



def usp_retro_det_val_nacionalidad():

    logger.info(f'usp_retro_det_val_nacionalidad - inicio') 

    try:

        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')

        query_insert_1 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'limpiar-indices-inicio', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_1, 'pg')   

        query_insert_2 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'limpiar-indices-fin', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_2, 'pg')   

        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'PASO_01', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg')

        query_create_table = """
            INSERT INTO INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP
            SELECT
                id_rep_general               ,
                numero_poliza                ,
                linea_negocio                ,
                cod_moneda                   ,
                monto_prima                  ,
                cod_frecuencia_pago          ,
                monto_prima_total            ,
                nombre_razon_social          ,
                ape_paterno                  ,
                ape_materno                  ,
                T.COD_TIPO_DOCUMENTO           ,
                numero_documento             ,
                tipo_cliente                 ,
                nacionalidad                 ,
                tipo_persona                 ,
                cod_ramo                     ,
                cod_subramo                  ,
                cod_producto                 ,
                cod_producto_sbs             ,
                cod_actividad_economica      ,
                actividad_economica          ,
                id_regimen                   ,
                fec_emision_poliza           ,
                fec_inicio_vigencia          ,
                fec_fin_vigencia             ,
                origen                       ,
                departamento                 ,
                CASE WHEN D.TIPO = 1 THEN 'PERUANO'
                    WHEN D.TIPO = 2 THEN 'EXTRANJERO'
                    WHEN D.TIPO <> 2 AND UPPER(T.GLOSA_PRODUCTO) IN('DESGRAVAMENTARJETAS','DESGRAVAMENPERSONAL') THEN 'PERUANO'
                    WHEN D.TIPO <> 2 AND NPERU.ID_NACIONALIDAD IS NOT NULL  THEN 'PERUANO'
                    WHEN D.TIPO NOT IN(1,2) AND NDESCO.ID_NACIONALIDAD IS NOT NULL  THEN 'DESCONOCIDA'
                    ELSE  'DESCONOCIDA'
                    END AS NACIONALIDAD_EVAL            ,
                departamento_eval            ,
                id_departamento              ,
                glosa_producto               ,
                id_riesgo_sbs                ,
                id_producto_key              ,
                fecha_nacimiento             ,
                edad_actuarial               ,
                periodo_pago                 ,
                monto_prima_recaudada        ,
                id_actividad_economica_eval  ,
                monto_prima_total_soles      ,
                cod_tipo_documento_eval      ,
                numero_documento_eval        ,
                observacion                  ,
                reglas                       ,
                fecha_creacion               ,
                fecha_modificacion           ,
                existe_en_plaft                ,
                activo                       ,
                nombre_razon_social_eval     ,
                ape_paterno_eval             ,
                ape_materno_eval             ,
                cod_actividad_economica_eval ,
                id_regimen_eval              ,
                numero_poliza_matriz_eval    ,
                archivo_excel_origen         ,
                estado_poliza
                FROM PLAFT_TRANSACCIONAL T
                LEFT JOIN  PLAFT_D_TIPO_DOCUMENTO D ON  coalesce(T.COD_TIPO_DOCUMENTO_EVAL,'-1') = D.COD_TIPO_DOCUMENTO
                LEFT JOIN PLAFT_D_NACIONALIDAD NPERU ON (coalesce(T.NACIONALIDAD_EVAL,'-1') = NPERU.NACIONALIDAD_ORIGEN  AND NPERU.ID_NACIONALIDAD_FINAL = 1)
                LEFT JOIN PLAFT_D_NACIONALIDAD NDESCO ON (coalesce(T.NACIONALIDAD_EVAL,'-') = NDESCO.NACIONALIDAD_ORIGEN  AND NDESCO.ID_NACIONALIDAD_FINAL = -1);
        """
        execute_query_no_results(query_create_table, 'pg')

        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional;"
        execute_query_no_results(query_truncate_table, 'pg')
        
        query_insert_data = """
            INSERT INTO interseguror.plaft_transaccional
            SELECT * FROM interseguror.plaft_transaccional_tmp;
        """
        execute_query_no_results(query_insert_data, 'pg')

        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = "TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')

    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_nacionalidad: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_nacionalidad - fin') 


def usp_retro_det_val_departamento():

    logger.info(f'usp_retro_det_val_departamento - inicio') 
    
    try:
        table_names = [
            'plaft_transaccional_tmp',
            'tmp_departamento_unico_01',
            'tmp_departamento_unico_02',
            'tmp_departamento_unico_03',
            'tmp_plaft_polizas_departamento',
            'tmp_plaft_polizas_departamento_unico',
            'tmp_plaft_polizas_departamento_unico_upd',
            'tmp_plaft_polizas_dep_01',
            'tmp_plaft_polizas_dep_unico_01',
            'tmp_plaft_polizas_dep_unico_upd_01',
            'tmp_plaft_clientes_sin_depa',
            'tmp_plaft_num_polizas_sin_depa',
            'tmp_plaft_num_polizas_upd_depa',
            'tmp_plaft_clientes_sin_depa_01',
            'plaft_tmp_vehi_sin_depa',
            'tmp_plaft_polizas_dep_10',
            'tmp_plaft_polizas_dep_unico_10',
            'tmp_plaft_polizas_dep_unico_upd_10'
        ]
    
        schema = 'interseguror'
    
        for table_name in table_names:
           
            logger.info(f"Truncando la tabla {schema}.{table_name}")
            query_truncate_table = f"TRUNCATE TABLE {schema}.{table_name};"
            execute_query_no_results(query_truncate_table, 'pg')


        query_create_table_1 = """
            INSERT INTO INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP
            SELECT
            ID_REP_GENERAL               ,
            NUMERO_POLIZA                ,
            LINEA_NEGOCIO                ,
            COD_MONEDA                   ,
            MONTO_PRIMA                  ,
            COD_FRECUENCIA_PAGO          ,
            MONTO_PRIMA_TOTAL            ,
            NOMBRE_RAZON_SOCIAL          ,
            APE_PATERNO                  ,
            APE_MATERNO                  ,
            T.COD_TIPO_DOCUMENTO           ,
            NUMERO_DOCUMENTO             ,
            TIPO_CLIENTE                 ,
            NACIONALIDAD                 ,
            TIPO_PERSONA                 ,
            COD_RAMO                     ,
            COD_SUBRAMO                  ,
            COD_PRODUCTO                 ,
            COD_PRODUCTO_SBS             ,
            COD_ACTIVIDAD_ECONOMICA      ,
            ACTIVIDAD_ECONOMICA          ,
            ID_REGIMEN                   ,
            FEC_EMISION_POLIZA           ,
            FEC_INICIO_VIGENCIA          ,
            FEC_FIN_VIGENCIA             ,
            ORIGEN                       ,
            DEPARTAMENTO                 ,
            NACIONALIDAD_EVAL            ,
            CASE WHEN UPPER(T.GLOSA_PRODUCTO) IN('DESGRAVAMENTARJETAS','DESGRAVAMENPERSONAL') THEN 'LIMA'
                WHEN T.ORIGEN = 'DIGITAL' THEN 'LIMA'
                WHEN T.DEPARTAMENTO_EVAL = 'PROV. CONST. DEL CALLAO' THEN 'CALLAO'
                WHEN  coalesce(T.DEPARTAMENTO_EVAL,'-') IN('PERU','VARIOS','-','.')   THEN 'NINGUNO'
                ELSE DEPARTAMENTO_EVAL END AS DEPARTAMENTO_EVAL           ,
            ID_DEPARTAMENTO              ,
            GLOSA_PRODUCTO               ,
            ID_RIESGO_SBS                ,
            ID_PRODUCTO_KEY              ,
            FECHA_NACIMIENTO             ,
            EDAD_ACTUARIAL               ,
            PERIODO_PAGO                 ,
            MONTO_PRIMA_RECAUDADA        ,
            ID_ACTIVIDAD_ECONOMICA_EVAL  ,
            MONTO_PRIMA_TOTAL_SOLES      ,
            COD_TIPO_DOCUMENTO_EVAL      ,
            NUMERO_DOCUMENTO_EVAL        ,
            OBSERVACION                  ,
            REGLAS                       ,
            FECHA_CREACION               ,
            FECHA_MODIFICACION           ,
            EXISTE_EN_PLAFT                ,
            ACTIVO                       ,
            NOMBRE_RAZON_SOCIAL_EVAL   ,
            APE_PATERNO_EVAL             ,
            APE_MATERNO_EVAL             ,
            COD_ACTIVIDAD_ECONOMICA_EVAL ,
            ID_REGIMEN_EVAL              ,
            NUMERO_POLIZA_MATRIZ_EVAL    ,
            ARCHIVO_EXCEL_ORIGEN         ,
            ESTADO_POLIZA
            FROM INTERSEGUROR.PLAFT_TRANSACCIONAL T;
        """
        execute_query_no_results(query_create_table_1, 'pg')

        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'DEPARTAMENTOOO-II-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg') 

        query_create_table_2 = """
            INSERT INTO INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_01
            SELECT DISTINCT T3.NUMERO_DOCUMENTO_EVAL, T3.DEPARTAMENTO_EVAL, T3.FEC_INICIO_VIGENCIA
                FROM INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T3
            INNER JOIN
            (
                SELECT DISTINCT T1.NUMERO_DOCUMENTO_EVAL, MAX(T1.FEC_INICIO_VIGENCIA) AS FEC_INICIO_VIGENCIA
                FROM INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
                INNER JOIN
                (
                    SELECT NUMERO_DOCUMENTO_EVAL, COUNT(DISTINCT coalesce(DEPARTAMENTO_EVAL,'-1')) AS CANTIDAD
                    FROM
                    (
                    SELECT DISTINCT NUMERO_DOCUMENTO_EVAL, DEPARTAMENTO_EVAL
                    FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP
                    WHERE  NUMERO_DOCUMENTO_EVAL <> '0'
                    ) AS TABAL
                    GROUP BY NUMERO_DOCUMENTO_EVAL
                    HAVING COUNT(DISTINCT coalesce(DEPARTAMENTO_EVAL,'-1')) > 1
                ) T2 ON T2.NUMERO_DOCUMENTO_EVAL = T1.NUMERO_DOCUMENTO_EVAL
                WHERE T1.NUMERO_DOCUMENTO_EVAL <> '0'
                    AND T1.DEPARTAMENTO_EVAL <> 'NINGUNO'
                GROUP BY T1.NUMERO_DOCUMENTO_EVAL
            ) T4 ON T3.NUMERO_DOCUMENTO_EVAL = T4.NUMERO_DOCUMENTO_EVAL
            AND T3.FEC_INICIO_VIGENCIA = T4.FEC_INICIO_VIGENCIA
            AND T3.DEPARTAMENTO_EVAL <> 'NINGUNO';
        """
        execute_query_no_results(query_create_table_2, 'pg')

        query_insert_4 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'DEPARTAMENTOOO-III-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_4, 'pg')

        query_create_table_3 = """
            INSERT INTO INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_02
            SELECT T.NUMERO_DOCUMENTO_EVAL, T.DEPARTAMENTO_EVAL,
                    ROW_NUMBER() OVER(PARTITION BY  T.NUMERO_DOCUMENTO_EVAL  ORDER BY T.NUMERO_DOCUMENTO_EVAL) AS NRO
            FROM   INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_01 T;
        """
        execute_query_no_results(query_create_table_3, 'pg')

        query_insert_5 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'DEPARTAMENTOOO-IV-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_5, 'pg')

        query_create_table_4 = """
            INSERT INTO INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_03
            select * from INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_02 where NRO = 1;
        """
        execute_query_no_results(query_create_table_4, 'pg')

        query_insert_6 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'Fin de tablas temporales', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_6, 'pg')

        query_create_table_5 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = P.DEPARTAMENTO_EVAL FROM(
            SELECT * FROM INTERSEGUROR.TMP_DEPARTAMENTO_UNICO_03
            ) P where PTT.NUMERO_DOCUMENTO_EVAL = P.NUMERO_DOCUMENTO_EVAL;
        """
        execute_query_no_results(query_create_table_5, 'pg')

        query_insert_7 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'departamento3-FIN', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_7, 'pg')

        query_create_table_6 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO
            select distinct numero_poliza
            from(
            select T1.NUMERO_POLIZA, count(distinct T1.DEPARTAMENTO_EVAL) as CANTIDAD
            from   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
            where T1.NUMERO_POLIZA not in
            (
            select distinct T1.NUMERO_POLIZA from INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
            where T1.TIPO_CLIENTE in('CONTRATANTE','ASEGURADO')
            )
            and T1.TIPO_CLIENTE in('BENEFICIARIO')
            and T1.COD_RAMO = '3' and T1.COD_SUBRAMO = '01' and T1.COD_PRODUCTO = '04'
            group by T1.NUMERO_POLIZA
            having count(distinct T1.DEPARTAMENTO_EVAL) > 1) AS TABAL;
        """
        execute_query_no_results(query_create_table_6, 'pg')

        query_create_table_7 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO_UNICO
            SELECT  T.NUMERO_POLIZA, COUNT(DISTINCT T.DEPARTAMENTO_EVAL) AS CANTIDAD FROM
            INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
            INNER JOIN INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO  TD ON T.NUMERO_POLIZA = TD.NUMERO_POLIZA
            WHERE T.DEPARTAMENTO_EVAL <> 'NINGUNO'
            GROUP BY T.NUMERO_POLIZA
            HAVING COUNT(DISTINCT T.DEPARTAMENTO_EVAL)  = 1;
        """
        execute_query_no_results(query_create_table_7, 'pg')

        query_create_table_8 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO_UNICO_UPD
            SELECT DISTINCT U.NUMERO_POLIZA, T.DEPARTAMENTO_EVAL
            FROM   INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO_UNICO U
                INNER JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T ON U.NUMERO_POLIZA = T.NUMERO_POLIZA
            WHERE  T.DEPARTAMENTO_EVAL <> 'NINGUNO';
        """
        execute_query_no_results(query_create_table_8, 'pg')

        query_create_table_9 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = DU.DEPARTAMENTO_EVAL,REGLAS = CONCAT(PTT.REGLAS,'-','R050') 
            FROM(SELECT * FROM INTERSEGUROR.TMP_PLAFT_POLIZAS_DEPARTAMENTO_UNICO_UPD) DU where PTT.NUMERO_POLIZA = DU.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_9, 'pg')

        query_insert_8 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'departamento4-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_8, 'pg')

        query_create_table_10 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_01
            select distinct numero_poliza
            from(
            select T1.NUMERO_POLIZA, count(distinct T1.DEPARTAMENTO_EVAL) as CANTIDAD
            from   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
            where T1.NUMERO_POLIZA not in
            (
            select distinct T1.NUMERO_POLIZA from INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
            where T1.TIPO_CLIENTE in('CONTRATANTE','ASEGURADO')
            )
            and T1.TIPO_CLIENTE in('BENEFICIARIO')
            and T1.COD_RAMO = '3' and T1.COD_SUBRAMO = '01' and T1.COD_PRODUCTO = '04'
            group by T1.NUMERO_POLIZA
            having count(distinct T1.DEPARTAMENTO_EVAL) > 1) AS TABAL;
        """
        execute_query_no_results(query_create_table_10, 'pg')

        query_create_table_11 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_01
            SELECT  T.NUMERO_POLIZA, max(T.ID_REP_GENERAL) as ID_REP_GENERAL
            FROM    INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
                    INNER JOIN INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_01 TD ON T.NUMERO_POLIZA = TD.NUMERO_POLIZA
            WHERE   T.DEPARTAMENTO_EVAL <> 'NINGUNO'
            GROUP BY T.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_11, 'pg')

        query_create_table_12 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_UPD_01
            SELECT DISTINCT U.NUMERO_POLIZA, T.DEPARTAMENTO_EVAL
            FROM   INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_01 U
                INNER JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T ON U.NUMERO_POLIZA = T.NUMERO_POLIZA
                AND T.ID_REP_GENERAL = U.ID_REP_GENERAL
            WHERE  T.DEPARTAMENTO_EVAL <> 'NINGUNO';
        """
        execute_query_no_results(query_create_table_12, 'pg')

        query_create_table_13 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = DU.DEPARTAMENTO_EVAL,REGLAS = CONCAT(PTT.REGLAS,'-','R050-1') 
            FROM(SELECT * FROM INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_UPD_01) DU where PTT.NUMERO_POLIZA = DU.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_13, 'pg')

        query_create_table_14 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_10
            select distinct numero_poliza
            from(
            select T1.NUMERO_POLIZA, count(distinct T1.DEPARTAMENTO_EVAL) as CANTIDAD
            from   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T1
            where T1.NUMERO_POLIZA in
            (
            '00000000000000068041-50806E','00000000000000001131-50803E'
            )
            and T1.ORIGEN = 'EXCEL'
            group by T1.NUMERO_POLIZA
            having count(distinct T1.DEPARTAMENTO_EVAL) > 1) AS TABAL;
        """
        execute_query_no_results(query_create_table_14, 'pg')

        query_create_table_15 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_10
            SELECT  T.NUMERO_POLIZA, max(T.ID_REP_GENERAL) as ID_REP_GENERAL
            FROM    INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
                    INNER JOIN INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_10 TD ON T.NUMERO_POLIZA = TD.NUMERO_POLIZA
            WHERE   T.DEPARTAMENTO_EVAL <> 'NINGUNO'
            GROUP BY T.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_15, 'pg')

        query_create_table_16 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_UPD_10
            SELECT DISTINCT U.NUMERO_POLIZA, T.DEPARTAMENTO_EVAL
            FROM   INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_10 U
                INNER JOIN INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T ON U.NUMERO_POLIZA = T.NUMERO_POLIZA
                AND T.ID_REP_GENERAL = U.ID_REP_GENERAL
            WHERE  T.DEPARTAMENTO_EVAL <> 'NINGUNO';
        """
        execute_query_no_results(query_create_table_16, 'pg')

        query_create_table_17 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = DU.DEPARTAMENTO_EVAL,REGLAS = CONCAT(PTT.REGLAS,'-','R057-1') 
            FROM(SELECT * FROM INTERSEGUROR.TMP_PLAFT_POLIZAS_DEP_UNICO_UPD_10) DU where PTT.NUMERO_POLIZA = DU.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_17, 'pg')

        query_insert_9 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'departamento4.0-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_9, 'pg')

        query_create_table_18 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA
            SELECT COD_TIPO_DOCUMENTO_EVAL, NUMERO_DOCUMENTO_EVAL FROM(
            SELECT T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, COUNT(1)
            FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
            WHERE  T.DEPARTAMENTO_EVAL = 'NINGUNO'
                AND T.TIPO_CLIENTE <> 'CONTRATANTE'
            GROUP BY T.COD_TIPO_DOCUMENTO_EVAL,T.NUMERO_DOCUMENTO_EVAL
            HAVING COUNT(1) = 1) AS TABAL;
        """
        execute_query_no_results(query_create_table_18, 'pg')

        query_create_table_19 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA_01
            SELECT DISTINCT T.NUMERO_POLIZA, T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, CAST('NINGUNO' AS VARCHAR(50)) AS DEPARTAMENTO_EVAL
            FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
                INNER JOIN INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA D ON
                (T.COD_TIPO_DOCUMENTO_EVAL = D.COD_TIPO_DOCUMENTO_EVAL AND T.NUMERO_DOCUMENTO_EVAL = D.NUMERO_DOCUMENTO_EVAL);
        """
        execute_query_no_results(query_create_table_19, 'pg')

        query_create_table_20 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_NUM_POLIZAS_SIN_DEPA
            SELECT DISTINCT T.NUMERO_POLIZA
            FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
                INNER JOIN INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA D ON
                (T.COD_TIPO_DOCUMENTO_EVAL = D.COD_TIPO_DOCUMENTO_EVAL AND T.NUMERO_DOCUMENTO_EVAL = D.NUMERO_DOCUMENTO_EVAL);
        """
        execute_query_no_results(query_create_table_20, 'pg')

        query_create_table_21 = """
            INSERT INTO INTERSEGUROR.TMP_PLAFT_NUM_POLIZAS_UPD_DEPA
            SELECT DISTINCT T.NUMERO_POLIZA, T.DEPARTAMENTO_EVAL
            FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
                INNER JOIN INTERSEGUROR.TMP_PLAFT_NUM_POLIZAS_SIN_DEPA D ON T.NUMERO_POLIZA = D.NUMERO_POLIZA
            WHERE  T.DEPARTAMENTO_EVAL <> 'NINGUNO'
                AND T.TIPO_CLIENTE = 'CONTRATANTE';
        """
        execute_query_no_results(query_create_table_21, 'pg')

        query_create_table_22 = """
            UPDATE INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA_01 TPC set DEPARTAMENTO_EVAL = U.DEPARTAMENTO_EVAL 
            FROM(SELECT * FROM INTERSEGUROR.TMP_PLAFT_NUM_POLIZAS_UPD_DEPA) U 
            where TPC.NUMERO_POLIZA = U.NUMERO_POLIZA;
        """
        execute_query_no_results(query_create_table_22, 'pg')

        query_create_table_23 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = D.DEPARTAMENTO_EVAL,REGLAS = CONCAT(PTT.REGLAS,'-','R058') 
            FROM(SELECT * FROM INTERSEGUROR.TMP_PLAFT_CLIENTES_SIN_DEPA_01 WHERE DEPARTAMENTO_EVAL <> 'NINGUNO'
            ) D where PTT.NUMERO_POLIZA = D.NUMERO_POLIZA AND PTT.COD_TIPO_DOCUMENTO_EVAL = D.COD_TIPO_DOCUMENTO_EVAL 
            AND PTT.NUMERO_DOCUMENTO_EVAL = D.NUMERO_DOCUMENTO_EVAL;
        """
        execute_query_no_results(query_create_table_23, 'pg')

        query_insert_10 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'departamento4.1-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_10, 'pg')

        query_insert_11 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'INICIO-DEPA-VEHICULAR', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_11, 'pg')

        query_create_table_24 = """
            INSERT INTO INTERSEGUROR.PLAFT_TMP_VEHI_SIN_DEPA
            SELECT T.ID_REP_GENERAL FROM INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP T
            WHERE T.COD_RAMO = '21.0' AND T.COD_SUBRAMO = '1.0' AND T.COD_PRODUCTO = '66.0'
            AND T.DEPARTAMENTO_EVAL = 'NINGUNO';
        """
        execute_query_no_results(query_create_table_24, 'pg')

        query_create_table_25 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set DEPARTAMENTO_EVAL = 'LIMA',REGLAS = CONCAT(PTT.REGLAS,' - R065') FROM(
            SELECT * FROM INTERSEGUROR.PLAFT_TMP_VEHI_SIN_DEPA
            ) PP where PTT.ID_REP_GENERAL = PP.ID_REP_GENERAL;
        """
        execute_query_no_results(query_create_table_25, 'pg')

        query_insert_12 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'FIN-DEPA-VEHICULAR', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_12, 'pg')

        query_create_table_26 = """
            update INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PT set DEPARTAMENTO_EVAL = 'EXTRANJERO' WHERE PT.NUMERO_POLIZA = '47077';
        """
        execute_query_no_results(query_create_table_26, 'pg')

        query_create_table_27 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PTT set ID_DEPARTAMENTO = PTT.ID_DEPARTAMENTO FROM(
            SELECT * FROM INTERSEGUROR.PLAFT_D_DEPARTAMENTO
          ) T where PTT.DEPARTAMENTO_EVAL = T.CODIGO;
        """
        execute_query_no_results(query_create_table_27, 'pg')

        query_insert_13 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'reglas..', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_13, 'pg')

        query_create_table_28 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP PT
            SET    ID_DEPARTAMENTO = -1,
                REGLAS = CONCAT(PT.REGLAS,'-','R018')
            WHERE  PT.ID_DEPARTAMENTO = '-1';
        """
        execute_query_no_results(query_create_table_28, 'pg')


        query_create_table_29 = """
            TRUNCATE TABLE INTERSEGUROR.PLAFT_TRANSACCIONAL;
        """
        execute_query_no_results(query_create_table_29, 'pg')

        query_insert_data = """
            INSERT INTO interseguror.plaft_transaccional
            SELECT * FROM interseguror.plaft_transaccional_tmp;
        """
        execute_query_no_results(query_insert_data, 'pg')

        query_insert_14 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'cargar-transaccional-fin', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_14, 'pg')

        query_insert_15 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'ELIMINANDO TABLAS', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_15, 'pg')

        table_names_delete = [
            'tmp_departamento_unico_01',
            'tmp_departamento_unico_02',
            'tmp_departamento_unico_03',
        ]
    
        schema = 'interseguror'
    
        for table_name in table_names_delete:
            
            logger.info(f"Truncando la tabla {schema}.{table_name}")
            query_truncate_table = f"TRUNCATE TABLE {schema}.{table_name};"
            execute_query_no_results(query_truncate_table, 'pg')

        query_insert_16 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'departamento4-FIN', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_16, 'pg')

        logger.info(f"Truncando la tabla interseguror.plaft_transaccional_tmp")
        query_truncate_table = f"TRUNCATE TABLE interseguror.plaft_transaccional_tmp;"
        execute_query_no_results(query_truncate_table, 'pg')

        query_insert_17 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'FIN-FIN', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_17, 'pg')

    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_departamento: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_departamento - fin') 


def usp_retro_det_val_prod_riesgo():

    logger.info(f'usp_retro_det_val_prod_riesgo - inicio') 

    try:
        update_transaccional_query_1 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY,ID_RIESGO_SBS = PP.ID_RIESGO_SBS
            FROM(SELECT
            coalesce(P.COD_RAMO,'0') AS COD_RAMO,
                        coalesce(P.COD_SUBRAMO,'0') AS COD_SUBRAMO,
                        coalesce(P.COD_PRODUCTO,'0') AS COD_PRODUCTO,
                        coalesce(P.DESC_PRODUCTO,'0') AS DESC_PRODUCTO,
                        P.ID_PRODUCTO_KEY,
                        P.ID_RIESGO_SBS
            FROM INTERSEGUROR.PLAFT_D_PRODUCTO P
            WHERE P.ORIGEN IN('ACSELE','EXPSERV','RVIADM','ADMWR')) PP
            where coalesce(T.COD_RAMO,'0') = PP.COD_RAMO 
            AND coalesce(T.COD_SUBRAMO,'0') = PP.COD_SUBRAMO 
            AND coalesce(T.COD_PRODUCTO,'0') = PP.COD_PRODUCTO 
            AND coalesce(T.GLOSA_PRODUCTO,'0') = PP.DESC_PRODUCTO;
        """
        execute_query_no_results(update_transaccional_query_1, 'pg')

        update_transaccional_query_2 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY,ID_RIESGO_SBS = PP.ID_RIESGO_SBS
            FROM(SELECT
            coalesce(P.COD_RAMO,'0') AS COD_RAMO,
                            coalesce(P.COD_PRODUCTO,'0') AS COD_PRODUCTO,
                            coalesce(P.DESC_PRODUCTO,'0') AS DESC_PRODUCTO,
                            P.ID_PRODUCTO_KEY,
                            P.ID_RIESGO_SBS
            FROM INTERSEGUROR.PLAFT_D_PRODUCTO P
            WHERE P.ORIGEN IN('AS400','DIGITAL')) PP
            where coalesce(T.COD_RAMO,'0') = PP.COD_RAMO 
            AND coalesce(T.COD_PRODUCTO,'0') = PP.COD_PRODUCTO 
            AND coalesce(T.GLOSA_PRODUCTO,'0') = PP.DESC_PRODUCTO;
        """
        execute_query_no_results(update_transaccional_query_2, 'pg')

        update_transaccional_query_3 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY,ID_RIESGO_SBS = PP.ID_RIESGO_SBS,
            GLOSA_PRODUCTO = PP.DESC_PRODUCTO
            FROM(SELECT
            coalesce(P.COD_RAMO,'0') AS COD_RAMO,
                            coalesce(P.COD_PRODUCTO,'0') AS COD_PRODUCTO,
                            coalesce(P.DESC_PRODUCTO,'0') AS DESC_PRODUCTO,
                            P.ID_PRODUCTO_KEY,
                            P.ID_RIESGO_SBS
            FROM INTERSEGUROR.PLAFT_D_PRODUCTO P
            WHERE P.ORIGEN IN('EXCEL')) PP
            where coalesce(T.COD_RAMO,'0') = PP.COD_RAMO 
            AND coalesce(T.COD_PRODUCTO,'0') = PP.COD_PRODUCTO;
        """
        execute_query_no_results(update_transaccional_query_3, 'pg')

    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_prod_riesgo: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_prod_riesgo - fin') 



def usp_retro_det_val_tipo_persona():

    logger.info(f'usp_retro_det_val_tipo_persona - inicio') 

    try:
        update_transaccional_query_1 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    TIPO_PERSONA = 'NATURAL'
            WHERE  T.TIPO_PERSONA = 'PERSONA NATURAL';
        """
        execute_query_no_results(update_transaccional_query_1, 'pg')

        update_transaccional_query_2 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
            SET    TIPO_PERSONA = 'JURIDICO'
            WHERE  T.TIPO_PERSONA =('PERSONA JURIDICA');
        """
        execute_query_no_results(update_transaccional_query_2, 'pg')

        update_transaccional_query_3 = f"""
            update INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set TIPO_PERSONA = 'JURIDICO'
            where COD_TIPO_DOCUMENTO_EVAL = 'RUCJ';
        """
        execute_query_no_results(update_transaccional_query_3, 'pg')

        update_transaccional_query_4 = f"""
            update INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set TIPO_PERSONA = 'NATURAL'
            where COD_TIPO_DOCUMENTO_EVAL = 'DNI';
        """
        execute_query_no_results(update_transaccional_query_4, 'pg')

        update_transaccional_query_5 = f"""
            update INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set TIPO_PERSONA = 'NATURAL'
            where COD_TIPO_DOCUMENTO_EVAL <> 'RUCJ' and T.TIPO_PERSONA = 'JURIDICO';
        """
        execute_query_no_results(update_transaccional_query_5, 'pg')

        update_transaccional_query_6 = f"""
            update INTERSEGUROR.PLAFT_TRANSACCIONAL T
            set TIPO_PERSONA = 'NATURAL'
            where COD_TIPO_DOCUMENTO_EVAL <> 'RUCJ';
        """
        execute_query_no_results(update_transaccional_query_6, 'pg')

    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_tipo_persona: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_tipo_persona - fin') 


def usp_retro_det_val_regimen():

    logger.info(f'usp_retro_det_val_regimen - inicio') 

    try:
        query_insert_1 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'EVAL-REGIMEN-INICIO', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_1, 'pg')   

        logger.info(f"Truncando la tabla interseguror.tmp_clientes_maximo_regimen")
        query_truncate_table = f"TRUNCATE TABLE interseguror.tmp_clientes_maximo_regimen;"
        execute_query_no_results(query_truncate_table, 'pg')

        query_insert_2 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'USP_RETRO_DET_VAL_REGIMEN - 01', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_2, 'pg') 

        update_transaccional_query_1 = f"""
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL TT
            set EXISTE_EN_PLAFT = PP.EXISTE_CLIENTE,ID_REGIMEN_EVAL = PP.ID_REGIMEN_FINAL,
            REGLAS = CONCAT(TT.REGLAS,'-','R019')
            FROM(SELECT  DISTINCT T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, LNR.CALIFICACION, LNR.CUMULO,
                                        P.ID_PRODUCTO_KEY,
                                        P.ID_REGIMEN AS ID_REGIMEN_INICIAL,
                                        CASE WHEN LNR.CALIFICACION IS NOT NULL AND LNR.CALIFICACION = 'Alto' THEN 3
            WHEN LNR.CALIFICACION IS NOT NULL AND LNR.CALIFICACION = 'Bajo' AND  P.ID_REGIMEN in(1,2) AND LNR.CUMULO > 1500 THEN 2
            WHEN LNR.CALIFICACION IS NULL THEN P.ID_REGIMEN
            ELSE 1 END AS ID_REGIMEN_FINAL,
                                        CASE WHEN LNR.CALIFICACION IS NOT NULL THEN 1
            ELSE 0 END AS EXISTE_CLIENTE
            FROM    INTERSEGUROR.PLAFT_TRANSACCIONAL T
            INNER JOIN INTERSEGUROR.PLAFT_D_PRODUCTO P
            ON T.ID_PRODUCTO_KEY = P.ID_PRODUCTO_KEY
            LEFT JOIN INTERSEGUROR.PLAFT_D_LISTA_NEGRA LNR
            ON  (T.COD_TIPO_DOCUMENTO_EVAL = LNR.COD_TIPO_DOCUMENTO
            AND T.NUMERO_DOCUMENTO_EVAL = LNR.NUMERO_DOCUMENTO)) PP
            where TT.COD_TIPO_DOCUMENTO_EVAL = PP.COD_TIPO_DOCUMENTO_EVAL 
            AND TT.NUMERO_DOCUMENTO_EVAL = PP.NUMERO_DOCUMENTO_EVAL 
            AND TT.ID_PRODUCTO_KEY = PP.ID_PRODUCTO_KEY;
        """
        execute_query_no_results(update_transaccional_query_1, 'pg')

        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'USP_RETRO_DET_VAL_REGIMEN - 02', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg') 

        query_for_loop_1 = """
            SELECT  DISTINCT T.NUMERO_DOCUMENTO_EVAL,
                             P.ID_PRODUCTO_KEY,
                             P.ID_REGIMEN AS ID_REGIMEN_INICIAL
            FROM    INTERSEGUROR.PLAFT_TRANSACCIONAL T
            INNER JOIN INTERSEGUROR.PLAFT_D_PRODUCTO P
            ON T.ID_PRODUCTO_KEY = P.ID_PRODUCTO_KEY
            WHERE T.COD_TIPO_DOCUMENTO_EVAL IS NULL
            AND T.ID_REGIMEN_EVAL IS NULL
            AND (T.ORIGEN IN('ACSELE','EXPSERV','DIGITAL','AS400','ADMWR','RVIADM')
            OR (T.ORIGEN = 'EXCEL' AND T.GLOSA_PRODUCTO IN('VidaGrupoComplementario','VidaLeyTTL')));
        """

        # Obtener resultados
        items_loop_1 = execute_query_with_results(query_for_loop_1, 'pg')

        # Bucle para actualizar con los resultados del SELECT
        for item in items_loop_1:
            execute_query_no_results(
                f"""UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
                    SET    ID_REGIMEN_EVAL = '{item[2]}',REGLAS = CONCAT(T.REGLAS,'-','R020')
                    WHERE  coalesce(T.NUMERO_DOCUMENTO_EVAL,'-') =  coalesce('{item[0]}','-')
                    AND T.ID_REGIMEN_EVAL IS NULL;
                    """, 
                'pg'
            )

        query_insert_4 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'USP_RETRO_DET_VAL_REGIMEN - 03', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_4, 'pg') 

        query_for_loop_2 = """
            SELECT
            P.ID_PRODUCTO_KEY,
            P.ID_REGIMEN AS ID_REGIMEN_INICIAL
            FROM    INTERSEGUROR.PLAFT_TRANSACCIONAL T
            INNER JOIN INTERSEGUROR.PLAFT_D_PRODUCTO P
            ON T.ID_PRODUCTO_KEY = P.ID_PRODUCTO_KEY
            WHERE T.COD_TIPO_DOCUMENTO IS NULL
            AND T.NUMERO_DOCUMENTO IS NULL
            AND T.ID_REGIMEN_EVAL IS NULL
            AND (T.ORIGEN IN('ACSELE','EXPSERV','DIGITAL','AS400','ADMWR','RVIADM')
            OR (T.ORIGEN = 'EXCEL' AND T.GLOSA_PRODUCTO IN('VidaGrupoComplementario','VidaLeyTTL')));
        """

        items_loop_2 = execute_query_with_results(query_for_loop_2, 'pg')

        # Bucle para actualizar con los resultados del SELECT
        for item in items_loop_2:
            execute_query_no_results(
                f"""UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T
                    SET    ID_REGIMEN_EVAL = '{item[1]}',REGLAS = CONCAT(T.REGLAS,'-','R021')
                    WHERE  T.ID_PRODUCTO_KEY = '{item[0]}'
                    AND T.ID_REGIMEN_EVAL IS NULL;
                    """, 
                'pg'
            )

        query_insert_5 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'USP_RETRO_DET_VAL_REGIMEN - 04', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_5, 'pg') 

        query_create_table = """
            INSERT INTO INTERSEGUROR.TMP_CLIENTES_MAXIMO_REGIMEN
            SELECT T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, MAX(T.ID_REGIMEN_EVAL) AS ID_REGIMEN_EVAL
            FROM   INTERSEGUROR.PLAFT_TRANSACCIONAL T
            WHERE  T.ACTIVO = 1
            GROUP BY T.COD_TIPO_DOCUMENTO_EVAL,T.NUMERO_DOCUMENTO_EVAL;
        """
        execute_query_no_results(query_create_table, 'pg')

        update_transaccional_query_2 = """
            UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T set ID_REGIMEN_EVAL = U.ID_REGIMEN_EVAL FROM(
                   SELECT * FROM INTERSEGUROR.TMP_CLIENTES_MAXIMO_REGIMEN
              ) U where T.COD_TIPO_DOCUMENTO_EVAL = U.COD_TIPO_DOCUMENTO_EVAL 
              AND T.NUMERO_DOCUMENTO_EVAL = U.NUMERO_DOCUMENTO_EVAL ;
        """
        execute_query_no_results(update_transaccional_query_2, 'pg')

        query_insert_6 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'EVAL-REGIMEN-FIN', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_6, 'pg')    

    except Exception as e:
        logger.error(f"Error en usp_retro_det_val_regimen: {str(e)}")
        raise

    logger.info(f'usp_retro_det_val_regimen - fin') 