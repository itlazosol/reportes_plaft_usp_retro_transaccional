from ..repository.reportes_plaft_usp_retro_repository import (
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
    usp_retro_det_val_tipo_y_num_doc,
    usp_retro_det_val_nacionalidad,
    usp_retro_det_val_departamento,
    usp_retro_det_val_prod_riesgo,
    usp_retro_det_val_tipo_persona,
    usp_retro_det_val_regimen
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

    registrar_log_interno("9. EVALUAR TIPO Y NUMERO DE DOCUMENTO - INICIO")

    usp_retro_det_val_tipo_y_num_doc()

    registrar_log_interno("9. EVALUAR TIPO Y NUMERO DE DOCUMENTO - FIN")

    registrar_log_interno("10. EVALUAR NACIONALIDAD - INICIO")

    usp_retro_det_val_nacionalidad()

    registrar_log_interno("10. EVALUAR NACIONALIDAD - FIN")

    registrar_log_interno("11. EVALUAR DEPARTAMENTO - INICIO")

    usp_retro_det_val_departamento()

    registrar_log_interno("11. EVALUAR DEPARTAMENTO - FIN")

    registrar_log_interno("12. EVALUAR PRODUCTO - INICIO")

    usp_retro_det_val_prod_riesgo()

    registrar_log_interno("12. EVALUAR PRODUCTO - FIN")

    registrar_log_interno("13. EVALUAR PERSONA NATURAL - INICIO")

    usp_retro_det_val_tipo_persona()

    registrar_log_interno("13. EVALUAR PERSONA NATURAL - FIN")

    registrar_log_interno("14. EVALUAR REGIMEN - INICIO")

    usp_retro_det_val_regimen()

    registrar_log_interno("14. EVALUAR REGIMEN - FIN")
