from ..repository.reportes_plaft_usp_retro_repository import (
    obtener_polizas_alloy,
    obtener_polizas_sme,
    insertar_polizas_temporal,
    limpiar_temporal,
    update_impmas_desde_temp,
    registrar_log_interno,
    update_actividad_economica_transaccional,
    usp_retro_det_activo,
    usp_retro_det_add_aseg_dit,
    usp_retro_det_add_aseg_soat,
    usp_retro_det_add_contra_pbi,
    usp_retro_det_poliza_matriz,
    usp_retro_det_calc_acti_econo,
    usp_retro_det_ini_valores,
    usp_retro_desactivar_aseg_previ,
    actualizar_riesgo_sbs,
    evaluar_montos_dobles,
    calcular_prima,
    excluir_polizas    
)
import pandas as pd
from ..utils.logger import logger

def reportes_plaft_usp_retro_acsele_service():
    logger.info("reportes_plaft_usp_retro_transaccional - inicio")

    registrar_log_interno("USP_RETRO_TRANSACCIONAL - INICIO")

    update_actividad_economica_transaccional()

    registrar_log_interno("1. SETEAR VALOR ACTIVO - INICIO")

    usp_retro_det_activo()

    registrar_log_interno("1. SETEAR VALOR ACTIVO - FIN")

    registrar_log_interno("2. ADICIONA ASEGURADOS A PRODUCTO DTI - INICIO")

    usp_retro_det_add_aseg_dit()

    registrar_log_interno("2. ADICIONA ASEGURADOS A PRODUCTO DTI - FIN")

    registrar_log_interno("3. ADICIONA ASEGURDADOS A PRODUCTO SOAT - INICIO")

    usp_retro_det_add_aseg_soat()

    registrar_log_interno("3. ADICIONA ASEGURDADOS A PRODUCTO SOAT - FIN")

    registrar_log_interno("4. ADICIONA ASEGURDADOS A PRODUCTO PBI - INICIO")

    usp_retro_det_add_contra_pbi()

    registrar_log_interno("4. ADICIONA ASEGURDADOS A PRODUCTO PBI - FIN")

    registrar_log_interno("5. POLIZA MATRIZ - INICIO")

    usp_retro_det_poliza_matriz()

    registrar_log_interno("5. POLIZA MATRIZ - FIN")

    registrar_log_interno("6. EVALUAR ACTIVDAD ECONOMICA - INICIO")

    usp_retro_det_calc_acti_econo()

    registrar_log_interno("6. EVALUAR ACTIVDAD ECONOMICA - FIN")

    registrar_log_interno("7. SETEAR VALORES INICIALES - INICIO")

    usp_retro_det_ini_valores()

    registrar_log_interno("7. SETEAR VALORES INICIALES - FIN")

    registrar_log_interno("8. DESACTIVAR ASEGURDADOS PREVICIONALES - INICIO")

    usp_retro_desactivar_aseg_previ()

    registrar_log_interno("8. DESACTIVAR ASEGURDADOS PREVICIONALES - FIN")



    registrar_log_interno("17. EXCLUIR POLIZAS - INICIO")
    excluir_polizas()
    registrar_log_interno("17. EXCLUIR POLIZAS - FIN")
    
    registrar_log_interno("19. CALCULAR PRIMA - INICIO")
    calcular_prima()
    registrar_log_interno("19. CALCULAR PRIMA - FIN")
    
    registrar_log_interno("20. EVALUAR MONTOS DOBLES - INICIO")
    evaluar_montos_dobles()
    registrar_log_interno("20. EVALUAR MONTOS DOBLES - FIN")
    
    registrar_log_interno("21. ACTUALIZAR RIESGO SBS DESG.INDIV - INICIO")
    actualizar_riesgo_sbs()
    registrar_log_interno("21. ACTUALIZAR RIESGO SBS DESG.INDIV - FIN")
    
    registrar_log_interno("USP_RETRO_TRANSACCIONAL - FIN")