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

    query_check_table = """
    SELECT COUNT(1) 
    FROM information_schema.tables 
    WHERE table_schema = 'interseguror' 
      AND table_name = 'plaft_transaccional_tmp';
    """
    V_CANT_REG = execute_query_with_results(query_check_table, 'pg')

    logger.info(f'Imprimiendo cantidad de registros check table {V_CANT_REG}') 

    if V_CANT_REG and V_CANT_REG[0][0] > 0:
        # Si la tabla existe, se elimina
        query_drop_table = "DROP TABLE IF EXISTS interseguror.plaft_transaccional_tmp CASCADE;"
        execute_query_no_results(query_drop_table, 'pg')

    # 1.1. LIMPIAR INDICES
    registrar_log_interno('1.1. LIMPIAR INDICES - INICIO', 2)

    usp_limpiar_indices()

    registrar_log_interno('1.1. LIMPIAR INDICES - FIN', 2)

    # 1.2. CREANDO TABLA TEMPORAL
    registrar_log_interno('1.2. CREANDO TABLA TEMPORAL - INICIO', 2)
    
    query_create_table = """
    CREATE TABLE interseguror.plaft_transaccional_tmp AS
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

    # Verifica si la tabla temporal aún existe y la elimina
    V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
    if V_CANT_REG and V_CANT_REG[0][0] > 0:
        query_drop_table = "DROP TABLE IF EXISTS interseguror.plaft_transaccional_tmp CASCADE;"
        execute_query_no_results(query_drop_table, 'pg')

    logger.info(f'usp_retro_det_activo - fin')


def usp_retro_det_add_aseg_dit():

    logger.info(f'usp_retro_det_add_aseg_dit - inicio') 

    schema = 'interseguror'
    temp_table = 'plaft_transaccional_tmp'
    main_table = 'plaft_transaccional'

    try:
        logger.info(f"Verificando existencia de la tabla {schema}.{temp_table}")
        query_check_table = f"""
            SELECT COUNT(1) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
              AND table_name = '{temp_table}';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{temp_table} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{temp_table} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{temp_table} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla {schema}.{temp_table}: {e}")
        raise

    # 2.1. ELIMINANDO INDICES
    try:
        logger.info("Iniciando limpieza de índices - Inicio")

        registrar_log_interno('2.1 LIMPIAR INDICES - INICIO', 2)

        usp_limpiar_indices()

        registrar_log_interno('2.1 LIMPIAR INDICES - FIN', 2)

        logger.info("Limpieza de índices completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la limpieza de índices: {e}")
        raise

    # 2.2. ELIMINANDO REGISTROS ESPECÍFICOS
    try:
        registrar_log_interno('2.2 ELIMINANDO LOS ASEGURADOS REGISTRADOS EN PROCESO - INICIO', 2)

        # Eliminar registros donde OBSERVACION LIKE '%REGISTRA ASEGURADO POR DESG_TARJ_INDIV%'
        query_delete = f"""
            DELETE FROM {schema}.{main_table} 
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
        
        # Crear la tabla temporal PLAFT_TRANSACCIONAL_TMP
        query_create_temp = f"""
            CREATE TABLE {schema}.{temp_table} AS
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
        execute_query_no_results(query_create_temp, 'pg')
        logger.info(f"Tabla temporal {schema}.{temp_table} creada exitosamente.")
        
        registrar_log_interno('2.3 CARGANDO TEMPORTAL - FIN', 2)

        logger.info("Carga temporal completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise

    # 2.3. MODIFICACIÓN Y TRUNCADO DE LA TABLA PRINCIPAL
    try:
        # Truncar la tabla principal
        query_truncate = f"TRUNCATE TABLE {schema}.{main_table};"
        execute_query_no_results(query_truncate, 'pg')
        logger.info(f"Tabla {schema}.{main_table} truncada exitosamente.")
    except Exception as e:
        logger.error(f"Error durante la modificación o truncado de la tabla principal: {e}")
        raise

    # 2.4. RECARGANDO DATOS
    try:
        logger.info("Iniciando recarga de datos - Inicio")
        
        registrar_log_interno('2.4 RECARGANDO DATOS - INICIO', 2)
        
        # Insertar datos desde la tabla temporal a la tabla principal
        query_insert = f"""
            INSERT INTO {schema}.{main_table}
            SELECT * FROM {schema}.{temp_table};
        """
        execute_query_no_results(query_insert, 'pg')
        logger.info(f"Datos recargados exitosamente en la tabla {schema}.{main_table}.")
        
        registrar_log_interno('2.4 RECARGANDO DATOS - FIN', 2)

        logger.info("Recarga de datos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la recarga de datos: {e}")
        raise

    # 3. Verifica si existe la tabla PLAFT_TRANSACCIONAL_TMP y la elimina si existe
    try:
        logger.info(f"Verificando existencia de la tabla temporal {schema}.{temp_table} para eliminarla.")
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{temp_table} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{temp_table} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{temp_table} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla temporal {schema}.{temp_table}: {e}")
        raise

    logger.info(f'usp_retro_det_add_aseg_dit - fin') 



def usp_retro_det_add_aseg_soat():

    logger.info(f'usp_retro_det_add_aseg_soat - inicio') 

    schema = 'interseguror'
    temp_table = 'plaft_transaccional_tmp'
    main_table = 'plaft_transaccional'

    try:
        logger.info(f"Verificando existencia de la tabla {schema}.{temp_table}")
        query_check_table = f"""
            SELECT COUNT(1) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
              AND table_name = '{temp_table}';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{temp_table} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{temp_table} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{temp_table} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla {schema}.{temp_table}: {e}")
        raise

    # 3.1. ELIMINANDO INDICES
    try:
        logger.info("Iniciando limpieza de índices - Inicio")

        registrar_log_interno('3.1 LIMPIANDO INDICES - INICIO', 2)

        usp_limpiar_indices()

        registrar_log_interno('3.1 LIMPIANDO INDICES - FIN', 2)

        logger.info("Limpieza de índices completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la limpieza de índices: {e}")
        raise

    # 3.2. ELIMINAR REGISTRADOS EN ESTE PROCESO(NO VIENE DE DATOS DE LA EXTRACCION)
    try:
        registrar_log_interno('3.2 ELIMINAR REGISTRADOS EN ESTE PROCESO - INICIO', 2)

        # Eliminar registros donde OBSERVACION LIKE '%REGISTRA ASEGURADO POR SOAT%'
        query_delete = f"""
            DELETE FROM {schema}.{main_table} 
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
            CREATE TABLE {schema}.{temp_table} AS
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
        logger.info(f"Tabla temporal {schema}.{temp_table} creada exitosamente.")
        
        registrar_log_interno('3.3. CARGANDO TEMPORAL - FIN', 2)

        logger.info("Carga temporal completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise

    # 3.3. MODIFICACIÓN Y TRUNCADO DE LA TABLA PRINCIPAL
    try:
        # Truncar la tabla principal
        query_truncate = f"TRUNCATE TABLE {schema}.{main_table};"
        execute_query_no_results(query_truncate, 'pg')
        logger.info(f"Tabla {schema}.{main_table} truncada exitosamente.")
    except Exception as e:
        logger.error(f"Error durante la modificación o truncado de la tabla principal: {e}")
        raise

    # 3.4. RECARGANDO DATOS
    try:
        logger.info("Iniciando recarga de datos - Inicio")
        
        registrar_log_interno('3.4. RECARGANDO TABLA - INICIO', 2)
        
        # Insertar datos desde la tabla temporal a la tabla principal
        query_insert = f"""
            INSERT INTO {schema}.{main_table}
            SELECT * FROM {schema}.{temp_table};
        """
        execute_query_no_results(query_insert, 'pg')
        logger.info(f"Datos recargados exitosamente en la tabla {schema}.{main_table}.")
        
        registrar_log_interno('3.4. RECARGANDO TABLA - FIN', 2)

        logger.info("Recarga de datos completada - Fin")
    except Exception as e:
        logger.error(f"Error durante la recarga de datos: {e}")
        raise

    # 3.5. Verifica si existe la tabla PLAFT_TRANSACCIONAL_TMP y la elimina si existe
    try:
        logger.info(f"Verificando existencia de la tabla temporal {schema}.{temp_table} para eliminarla.")
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{temp_table} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{temp_table} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{temp_table} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla temporal {schema}.{temp_table}: {e}")
        raise

    logger.info(f'usp_retro_det_add_aseg_soat - fin') 


def usp_retro_det_add_contra_pbi():

    logger.info(f'usp_retro_det_add_contra_pbi - inicio') 

    schema = 'interseguror'
    temp_table = 'tmp_polizas_sin_contra_pbip'
    main_table = 'plaft_transaccional'

    try:
        logger.info(f"Verificando existencia de la tabla {schema}.{temp_table}")
        query_check_table = f"""
            SELECT COUNT(1) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
              AND table_name = '{temp_table}';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{temp_table} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{temp_table} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{temp_table} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{temp_table} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla {schema}.{temp_table}: {e}")
        raise


    try:
        logger.info("Iniciando carga temporal tmp_polizas_sin_contra_pbip - Inicio")
        
        query_create_temp = f"""
            CREATE TABLE {schema}.{temp_table} as
        select distinct t.numero_poliza
        from   {schema}.{main_table} t
        where  t.glosa_producto = 'ProtBlindajeIndividualPlus'
        except
        select distinct t.numero_poliza
        from   {schema}.{main_table} t
        where  t.glosa_producto = 'ProtBlindajeIndividualPlus'
               and t.tipo_cliente = 'CONTRATANTE';
        """
        execute_query_no_results(query_create_temp, 'pg')
        logger.info(f"Tabla temporal {schema}.{temp_table} creada exitosamente.")
        

        logger.info("Carga temporal completada tmp_polizas_sin_contra_pbip - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
        raise


    try:
        logger.info(f"Iniciando insercion data {schema}.{main_table} desde {schema}.{temp_table} - Inicio")
        
        query_create_temp = f"""
            insert into {schema}.{main_table}(
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
        from {schema}.{main_table} t
        where t.numero_poliza in
        ( select  p.numero_poliza
          from    {schema}.{temp_table} p
        );
        """
        execute_query_no_results(query_create_temp, 'pg')
        
        logger.info(f"Iniciando insercion data {schema}.{main_table} desde {schema}.{temp_table} - Fin")
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
        logger.info(f"Verificando existencia de la tabla {schema}.{table_tmp}")
        query_check_table = f"""
            SELECT COUNT(1) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema}' 
              AND table_name = '{table_tmp}';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')
        
        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            logger.info(f"La tabla {schema}.{table_tmp} existe. Procediendo a eliminarla.")
            query_drop_table = f"DROP TABLE IF EXISTS {schema}.{table_tmp} CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
            logger.info(f"Tabla {schema}.{table_tmp} eliminada exitosamente.")
        else:
            logger.info(f"La tabla {schema}.{table_tmp} no existe. No se requiere eliminación.")
    except Exception as e:
        logger.error(f"Error al verificar o eliminar la tabla {schema}.{table_tmp}: {e}")
        raise


    try:
        logger.info(f"Iniciando carga temporal {schema}.{table_tmp} - Inicio")
        
        query_create_temp = f"""
            CREATE TABLE {schema}.{table_tmp} AS
        SELECT DISTINCT T.NUMERO_POLIZA_MATRIZ_EVAL, T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL
        FROM   {schema}.{table_main} T
        WHERE  T.NUMERO_POLIZA_MATRIZ_EVAL > 0
               AND T.ACTIVO = 1
               AND T.TIPO_CLIENTE = 'CONTRATANTE'
               AND T.ORIGEN <> 'EXCEL';
        """
        execute_query_no_results(query_create_temp, 'pg')
        logger.info(f"Tabla temporal {schema}.{table_tmp} creada exitosamente.")
        

        logger.info(f"Carga temporal completada {schema}.{table_tmp} - Fin")
    except Exception as e:
        logger.error(f"Error durante la creación de la tabla temporal: {e}")
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
        query_check_table = """
        SELECT COUNT(1) 
        FROM information_schema.tables 
        WHERE table_schema = 'interseguror' 
        AND table_name = 'tmp_actividad_ecomica_acsele';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')

        logger.info(f'Imprimiendo cantidad de registros check table {V_CANT_REG}') 

        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            # Si la tabla existe, se elimina
            query_drop_table = "DROP TABLE IF EXISTS interseguror.tmp_actividad_ecomica_acsele CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
        
        query_create_temp = f"""
            CREATE TABLE interseguror.tmp_actividad_ecomica_acsele as
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
        query_check_table = """
        SELECT COUNT(1) 
        FROM information_schema.tables 
        WHERE table_schema = 'interseguror' 
        AND table_name = 'plaft_transaccional_tmp';
        """
        V_CANT_REG = execute_query_with_results(query_check_table, 'pg')

        logger.info(f'Imprimiendo cantidad de registros check table {V_CANT_REG}') 

        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            # Si la tabla existe, se elimina
            query_drop_table = "DROP TABLE IF EXISTS interseguror.plaft_transaccional_tmp CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
        
        query_alter_sequence = f"""
            alter sequence interseguror.seq_plaft_transaccional restart start with 1;
        """
        execute_query_no_results(query_alter_sequence, 'pg')    

        query_insert_1 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'limpiar-inicices-inicio', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_1, 'pg')   

        usp_limpiar_indices()

        query_insert_2 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'limpiar-inicices-fin', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_2, 'pg')   

        query_insert_3 = f"""
            insert into INTERSEGUROR.LOG_PLAFT_PROCESO_INTERNO  values(NEXTVAL('interseguror.seq_plaft_log_pro_int'),'paso-01', '{ datetime.now() }');
        """
        execute_query_no_results(query_insert_3, 'pg') 

        query_create_temp = f"""
            CREATE TABLE INTERSEGUROR.PLAFT_TRANSACCIONAL_TMP
            AS
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

        query_check_table_2 = """
        SELECT COUNT(1) 
        FROM information_schema.tables 
        WHERE table_schema = 'interseguror' 
        AND table_name = 'plaft_transaccional_tmp';
        """
        V_CANT_REG = execute_query_with_results(query_check_table_2, 'pg')

        logger.info(f'Imprimiendo cantidad de registros check table {V_CANT_REG}') 

        if V_CANT_REG and V_CANT_REG[0][0] > 0:
            # Si la tabla existe, se elimina
            query_drop_table = "DROP TABLE IF EXISTS interseguror.plaft_transaccional_tmp CASCADE;"
            execute_query_no_results(query_drop_table, 'pg')
        
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


def actualizar_riesgo_sbs():
    logger.info('actualizar_riesgo_sbs - inicio')
    res=execute_query_with_results("update INTERSEGUROR.PLAFT_TRANSACCIONAL set ID_RIESGO_SBS = '74' where ID_RIESGO_SBS is null and GLOSA_PRODUCTO = 'DesgTarjetasIndividual';",'pg')
    logger.info('actualizar_riesgo_sbs - fin')
    return res

def excluir_polizas():
    logger.info('excluir_polizas - inicio')

    temp_tablas = [
        "PLAFT_POLIZAS_EXCLUIDOS",
        "CONTRATANTES_REPETIDOS",
        "ASEGURADOS_REPETIDOS",
        "BENEFICIARIO_REPETIDOS", 
        "PLAFT_TMP_POLIZAS_NO_VALIDAR" #ok
    ]

    for tabla in temp_tablas:
        execute_query_no_results("TRUNCATE TABLE " + tabla + ";",'pg')
    
    # Insertar en PLAFT_TMP_POLIZAS_NO_VALIDAR_1
    insert_plaft_tmp_pol_no_val_1 = """
        INSERT INTO interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR (NUMERO_POLIZA)
        SELECT DISTINCT t.NUMERO_POLIZA
        FROM interseguror.plaft_transaccional t
        WHERE t.reglas LIKE '%R050%';
    """
    execute_query_no_results(insert_plaft_tmp_pol_no_val_1, 'pg')

    # Insertar en PLAFT_TMP_POLIZAS_NO_VALIDAR_2
    insert_plaft_tmp_pol_no_val_2 = """
        INSERT INTO interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR
        SELECT DISTINCT numero_poliza
        FROM (
            SELECT t1.numero_poliza, COUNT(DISTINCT t1.departamento_eval) AS cantidad
            FROM interseguror.PLAFT_TRANSACCIONAL t1
            WHERE t1.numero_poliza NOT IN (
                SELECT DISTINCT t1.numero_poliza 
                FROM interseguror.PLAFT_TRANSACCIONAL t1
                WHERE t1.tipo_cliente IN ('CONTRATANTE')
            )
            AND t1.tipo_cliente IN ('ASEGURADO')
            AND t1.origen = 'EXCEL'
            GROUP BY t1.numero_poliza
            HAVING COUNT(DISTINCT t1.departamento_eval) > 1
        ) AS subquery;
    """
    execute_query_no_results(insert_plaft_tmp_pol_no_val_2, 'pg')

    # Insertar en PLAFT_TMP_POLIZAS_NO_VALIDAR_3
    insert_plaft_tmp_pol_no_val_3 = """
        INSERT INTO interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR
        SELECT DISTINCT T.NUMERO_POLIZA
        FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.COD_RAMO = '4' AND T.COD_SUBRAMO = '01' AND T.COD_PRODUCTO = '01'
        AND T.TIPO_CLIENTE = 'ASEGURADO';
    """
    execute_query_no_results(insert_plaft_tmp_pol_no_val_3, 'pg')

    # Insertar en PLAFT_TMP_POLIZAS_NO_VALIDAR_4
    insert_plaft_tmp_pol_no_val_4 = """
        INSERT INTO interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR
        SELECT DISTINCT numero_poliza
        FROM (
            SELECT t1.numero_poliza
            FROM interseguror.PLAFT_TRANSACCIONAL t1
            WHERE t1.numero_poliza IN ('00000000000000068041-50806E', '00000000000000001131-50803E')
            AND t1.origen = 'EXCEL'
        ) AS subquery;
    """
    execute_query_no_results(insert_plaft_tmp_pol_no_val_4, 'pg')

    # Insertar en PLAFT_TMP_POLIZAS_NO_VALIDAR_5
    insert_plaft_tmp_pol_no_val_5 = """
        INSERT INTO interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR
        SELECT DISTINCT T.NUMERO_POLIZA
        FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.COD_RAMO = '4'
        AND T.COD_SUBRAMO = '01'
        AND T.COD_PRODUCTO = '01'
        AND T.TIPO_CLIENTE = 'ASEGURADO';
    """
    execute_query_no_results(insert_plaft_tmp_pol_no_val_5, 'pg')

    insert_plaft_tmp_pol_exclu = """
        CREATE TABLE interseguror.PLAFT_POLIZAS_EXCLUIDOS AS
        SELECT DISTINCT T.NUMERO_POLIZA, 'TIPO DOCUMENTO NULO' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.COD_TIPO_DOCUMENTO_EVAL IS NULL
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'NUMERO DOCUMENTO NULO Y NUMERO POLIZA NULO' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.NUMERO_DOCUMENTO_EVAL IS NULL AND T.NUMERO_POLIZA IS NOT NULL
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'SIN DEPARTAMENTO' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.DEPARTAMENTO_EVAL = 'NINGUNO'
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'SIN ACTIVIDAD ECONOMICA REGIMEN <> 1 Y PERSONA TIPO JURIDICO' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.COD_ACTIVIDAD_ECONOMICA_EVAL = '-1' AND T.ID_REGIMEN_EVAL <> 1 AND T.TIPO_PERSONA = 'JURIDICO'
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'CLIENTES CON DIFERENTES TIPO DE DOCUMENTO(UNO ES RUC)' AS OBSERVACION
        FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.COD_TIPO_DOCUMENTO IN ('RUCJ', 'RUCN')
        AND EXISTS (
            SELECT 1 FROM interseguror.PLAFT_TRANSACCIONAL T2
            WHERE T2.NUMERO_DOCUMENTO_EVAL = T.NUMERO_DOCUMENTO_EVAL
            AND T2.ACTIVO = 1
            GROUP BY T2.NUMERO_DOCUMENTO_EVAL
            HAVING COUNT(DISTINCT T2.COD_TIPO_DOCUMENTO) > 1
        )
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'AS400: POLIZAS SINIESTRADAS(CON BENEFICIARIO)' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T WHERE T.ORIGEN = 'AS400'
        AND T.TIPO_CLIENTE = 'BENEFICIARIO'
        UNION
        SELECT DISTINCT NUMERO_POLIZA, 'EXCEL: CLIENTES DUPLICADOS POR POLIZA' AS OBSERVACION FROM (
            SELECT T.NUMERO_POLIZA
            FROM interseguror.PLAFT_TRANSACCIONAL T
            WHERE T.ORIGEN = 'EXCEL'
            GROUP BY T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, T.TIPO_CLIENTE, T.NUMERO_POLIZA
            HAVING COUNT(1) > 1
        ) AS P
        UNION
        SELECT DISTINCT T.NUMERO_POLIZA, 'EXCEL: REGISTROS SIN REGIMEN' AS OBSERVACION FROM interseguror.PLAFT_TRANSACCIONAL T
        WHERE T.ORIGEN = 'EXCEL' AND T.ID_REGIMEN_EVAL IS NULL
        UNION
        SELECT DISTINCT NUMERO_POLIZA, 'CLIENTES CON DIFERENTES DEPARTAMENTOS' AS OBSERVACION FROM (
            SELECT T.NUMERO_POLIZA
            FROM interseguror.PLAFT_TRANSACCIONAL T
            WHERE T.ACTIVO = 1
            AND T.NUMERO_POLIZA NOT IN (
                SELECT NUMERO_POLIZA FROM interseguror.PLAFT_TMP_POLIZAS_NO_VALIDAR
            )
            GROUP BY T.COD_TIPO_DOCUMENTO_EVAL, T.NUMERO_DOCUMENTO_EVAL, T.NUMERO_POLIZA
            HAVING COUNT(DISTINCT T.DEPARTAMENTO_EVAL) > 1
        ) AS D
        UNION
        SELECT DISTINCT NUMERO_POLIZA, 'RENTAS:POLIZA CON BENEFICIARIOS Y MAS DE UN DEPARTAMENTO' AS OBSERVACION
        FROM (
            SELECT T1.NUMERO_POLIZA
            FROM interseguror.PLAFT_TRANSACCIONAL T1
            WHERE T1.NUMERO_POLIZA NOT IN (
                SELECT NUMERO_POLIZA FROM interseguror.PLAFT_TRANSACCIONAL T1
                WHERE T1.TIPO_CLIENTE IN ('CONTRATANTE', 'ASEGURADO')
            )
            AND T1.TIPO_CLIENTE IN('BENEFICIARIO')
            GROUP BY T1.NUMERO_POLIZA
            HAVING COUNT(DISTINCT T1.DEPARTAMENTO_EVAL) > 1
        ) AS R
        UNION
        SELECT DISTINCT NUMERO_POLIZA, 'EXCEL:POLIZA CON ASEGURADOS Y MAS DE UN DEPARTAMENTO' AS OBSERVACION
        FROM (
            SELECT T1.NUMERO_POLIZA
            FROM interseguror.PLAFT_TRANSACCIONAL T1
            WHERE T1.NUMERO_POLIZA NOT IN (
                SELECT NUMERO_POLIZA FROM interseguror.PLAFT_TRANSACCIONAL T1
                WHERE T1.TIPO_CLIENTE IN ('CONTRATANTE')
            )
            AND T1.TIPO_CLIENTE IN('ASEGURADO')
            GROUP BY T1.NUMERO_POLIZA
            HAVING COUNT(DISTINCT T1.DEPARTAMENTO_EVAL) > 1
        ) AS A;
    """
    execute_query_no_results(insert_plaft_tmp_pol_exclu, 'pg')

    # Actualizar en plaft_transaccional
    update_plaft_transaccional = """
        WITH excluded_polizas AS (
            SELECT DISTINCT P.NUMERO_POLIZA
            FROM interseguror.PLAFT_POLIZAS_EXCLUIDOS P
        )
        UPDATE interseguror.PLAFT_TRANSACCIONAL TT
        SET 
            ACTIVO = 0,
            REGLAS = TT.REGLAS || '-' || 'R026'
        FROM excluded_polizas PP
        WHERE PP.NUMERO_POLIZA = TT.NUMERO_POLIZA;
    """
    execute_query_no_results(update_plaft_transaccional, 'pg')

    # Insertar en CONTRATANTES_REPETIDOS
    insert_contratantes_repetidos = """
        INSERT INTO interseguror.CONTRATANTES_REPETIDOS (cod_tipo_documento_eval, numero_documento_eval, numero_poliza, cantidad)
        SELECT t.cod_tipo_documento_eval, t.numero_documento_eval, t.numero_poliza, COUNT(1) AS cantidad
        FROM interseguror.plaft_transaccional t
        WHERE t.tipo_cliente = 'CONTRATANTE'
        GROUP BY t.cod_tipo_documento_eval, t.numero_documento_eval, t.numero_poliza
        HAVING COUNT(1) > 1;
    """
    execute_query_no_results(insert_contratantes_repetidos, 'pg')

    update_plaft_transaccional_2 = """
        WITH CTE AS (
            SELECT MIN(t.id_rep_general) as id_rep_general,
                t.cod_tipo_documento_eval,
                t.numero_documento_eval
            FROM interseguror.plaft_transaccional t
            INNER JOIN interseguror.CONTRATANTES_REPETIDOS cr ON t.cod_tipo_documento_eval = cr.cod_tipo_documento_eval
                                                            AND t.numero_documento_eval = cr.numero_documento_eval
                                                            AND t.numero_poliza = cr.numero_poliza
            WHERE t.tipo_cliente = 'CONTRATANTE'
            GROUP BY t.cod_tipo_documento_eval, t.numero_documento_eval
        )
        UPDATE interseguror.plaft_transaccional t
        SET 
            activo = 0,
            reglas = t.reglas || '-' || 'R037'
        FROM CTE
        WHERE t.id_rep_general = CTE.id_rep_general;
    """
    execute_query_no_results(update_plaft_transaccional_2, 'pg')

    #Insert beneficarios repetidos
    insert_beneficiarios_repetidos = """
        INSERT INTO interseguror.BENEFICIARIO_REPETIDOS (cod_tipo_documento_eval, numero_documento_eval, numero_poliza, cantidad)
        SELECT t.cod_tipo_documento_eval, t.numero_documento_eval, t.numero_poliza, COUNT(*) AS cantidad
        FROM interseguror.plaft_transaccional t
        WHERE t.tipo_cliente = 'BENEFICIARIO'
        GROUP BY t.cod_tipo_documento_eval, t.numero_documento_eval, t.numero_poliza
        HAVING COUNT(*) > 1;
    """
    execute_query_no_results(insert_beneficiarios_repetidos, 'pg')

    #Update plaft transaccional
    update_plaft_transaccional_3 = """
        WITH CTE AS (
            SELECT MIN(t.id_rep_general) AS id_rep_general,
                t.cod_tipo_documento_eval,
                t.numero_documento_eval
            FROM interseguror.plaft_transaccional t
            INNER JOIN interseguror.BENEFICIARIO_REPETIDOS cr ON t.cod_tipo_documento_eval = cr.cod_tipo_documento_eval
                                                            AND t.numero_documento_eval = cr.numero_documento_eval
                                                            AND t.numero_poliza = cr.numero_poliza
            WHERE t.tipo_cliente = 'BENEFICIARIO'
            GROUP BY t.cod_tipo_documento_eval, t.numero_documento_eval
        )
        UPDATE interseguror.plaft_transaccional t
        SET 
            activo = 0,
            reglas = t.reglas || '-' || 'R039'
        FROM CTE
        WHERE t.id_rep_general = CTE.id_rep_general;
    """
    execute_query_no_results(update_plaft_transaccional_3, 'pg')

    logger.info('excluir_polizas - fin')

def calcular_prima():
    logger.info('calcular_prima - inicio')
    sql1=f""
    sql3=f"UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T SET MONTO_PRIMA_TOTAL_SOLES = T.MONTO_PRIMA_TOTAL WHERE T.COD_MONEDA = 'PEN';"
    sqlTipoCambio=f"WITH FechaCorte AS( SELECT TP.FECHA_CORTE FROM ( SELECT * FROM INTERSEGUROR.PLAFT_LOG_PROCESO ORDER BY 1 DESC) TP LIMIT 1 ), TipoCambio AS ( SELECT COUNT(DISTINCT VTC.RATE) AS CANT_TIPO_CAMBIO, CASE WHEN COUNT(DISTINCT VTC.RATE) > 0 THEN ( SELECT DISTINCT VTC.RATE FROM INTERSEGURO.VIEW_TIPO_CAMBIO_DIARIO VTC INNER JOIN SAMP.EQUIVALENCIAS E1 ON VTC.MONEDAINIVAL = CAST(NULLIF(E1.ACSELE, '') AS DECIMAL(19,0)) AND E1.TIPO = 'MONEDA' WHERE E1.CANONICA = 'USD' AND TO_CHAR(VTC.FECHA, 'YYYYMMDD') = TO_CHAR((SELECT FECHA_CORTE FROM FechaCorte), 'YYYYMMDD') ) ELSE ( SELECT DISTINCT VTC.RATE FROM INTERSEGURO.VIEW_TIPO_CAMBIO_DIARIO VTC INNER JOIN SAMP.EQUIVALENCIAS E1 ON VTC.MONEDAINIVAL = CAST(NULLIF(E1.ACSELE, '') AS DECIMAL(19,0)) AND E1.TIPO = 'MONEDA' WHERE E1.CANONICA = 'USD' AND TO_CHAR(VTC.FECHA, 'YYYYMMDD') = ( SELECT TO_CHAR(MAX(VTC.FECHA), 'YYYYMMDD') FROM INTERSEGURO.VIEW_TIPO_CAMBIO_DIARIO VTC INNER JOIN SAMP.EQUIVALENCIAS E1 ON VTC.MONEDAINIVAL = CAST(E1.ACSELE AS DOUBLE PRECISION) AND E1.TIPO = 'MONEDA' WHERE E1.CANONICA = 'USD' ) ) END AS TIPO_CAMBIO FROM INTERSEGURO.VIEW_TIPO_CAMBIO_DIARIO VTC INNER JOIN SAMP.EQUIVALENCIAS E1 ON VTC.MONEDAINIVAL = CAST(NULLIF(E1.ACSELE, '') AS DECIMAL(19,0)) AND E1.TIPO = 'MONEDA' WHERE E1.CANONICA = 'USD' AND TO_CHAR(VTC.FECHA, 'YYYYMMDD') = TO_CHAR((SELECT FECHA_CORTE FROM FechaCorte), 'YYYYMMDD') ) SELECT TIPO_CAMBIO FROM TipoCambio; "
    ga=execute_query_to_df(sqlTipoCambio,'pg')
    tipo_cambio=ga.iloc[0, 0]
    sql4=f"UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL T SET MONTO_PRIMA_TOTAL_SOLES = T.MONTO_PRIMA_TOTAL * {tipo_cambio} WHERE T.COD_MONEDA = 'USD';"
    res=execute_query_with_results(sql1,'pg')
    res=execute_query_with_results(sql3,'pg')
    res=execute_query_with_results(sql4,'pg')
    logger.info('calcular_prima - fin')
    return res

def evaluar_montos_dobles():
    logger.info('evaluar_montos_dobles - inicio')
    sql1=f"INSERT INTO INTERSEGUROR.TMP_MONTOS_DOBLES SELECT T .NUMERO_POLIZA, T.NUMERO_DOCUMENTO_EVAL, T.COD_TIPO_DOCUMENTO_EVAL, COUNT( DISTINCT T.MONTO_PRIMA_TOTAL) AS CANTIDAD FROM PLAFT_TRANSACCIONAL T WHERE T.ACTIVO = 1 GROUP BY T.NUMERO_POLIZA, T.NUMERO_DOCUMENTO_EVAL, T.COD_TIPO_DOCUMENTO_EVAL HAVING COUNT ( DISTINCT T.MONTO_PRIMA_TOTAL ) > 1;"
    sql2=f"INSERT INTO INTERSEGUROR.TMP_MONTOS_DOBLES_TRANS SELECT MD.COD_TIPO_DOCUMENTO_EVAL, MD.NUMERO_DOCUMENTO_EVAL, MD.NUMERO_POLIZA, TT.MONTO_PRIMA_TOTAL, TT.MONTO_PRIMA_TOTAL_SOLES FROM TMP_MONTOS_DOBLES MD INNER JOIN PLAFT_TRANSACCIONAL TT ON( MD.NUMERO_POLIZA = TT.NUMERO_POLIZA AND MD.NUMERO_DOCUMENTO_EVAL = TT.NUMERO_DOCUMENTO_EVAL AND MD.COD_TIPO_DOCUMENTO_EVAL = TT.COD_TIPO_DOCUMENTO_EVAL) WHERE TT.TIPO_CLIENTE = 'ASEGURADO' AND TT.ACTIVO = 1;"
    sql3=f"UPDATE INTERSEGUROR.PLAFT_TRANSACCIONAL SET MONTO_PRIMA_TOTAL = MDP.MONTO_PRIMA_TOTAL, MONTO_PRIMA_TOTAL_SOLES = MDP.MONTO_PRIMA_TOTAL_SOLES, OBSERVACION = CONCAT( OBSERVACION, '-R50') FROM ( SELECT * FROM INTERSEGUROR.TMP_MONTOS_DOBLES_TRANS ) MDP WHERE PLAFT_TRANSACCIONAL.NUMERO_POLIZA = MDP.NUMERO_POLIZA AND PLAFT_TRANSACCIONAL.NUMERO_DOCUMENTO_EVAL = MDP.NUMERO_DOCUMENTO_EVAL AND PLAFT_TRANSACCIONAL.COD_TIPO_DOCUMENTO_EVAL = MDP.COD_TIPO_DOCUMENTO_EVAL AND PLAFT_TRANSACCIONAL.ACTIVO = 1 AND PLAFT_TRANSACCIONAL.TIPO_CLIENTE = 'CONTRATANTE';"
    res=execute_query_with_results(sql1,'pg')
    res=execute_query_with_results(sql2,'pg')
    res=execute_query_with_results(sql3,'pg')
    logger.info('evaluar_montos_dobles - fin')
    return res