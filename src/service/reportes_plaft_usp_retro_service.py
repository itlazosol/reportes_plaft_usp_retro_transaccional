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
    usp_retro_desactivar_aseg_previ
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


def actualizacion_envio_acsele_service():
    logger.info("actualizacion_envio_acsele - inicio")

    logger.info("actualizacion_envio_acsele - Consultando polizas Acsele")
    dfAlloy = obtener_polizas_alloy()

    logger.info("actualizacion_envio_acsele - Consultando polizas SME")
    dfSme = obtener_polizas_sme()

    logger.info("actualizacion_envio_acsele - Cruzando polizas Acsele x SME")
    merged_df = pd.merge(
        dfAlloy,
        dfSme[["idproducto", "idpoliza", "idoperacion", "evento", "idenviosme"]],
        on=["idproducto", "idpoliza", "idoperacion", "evento"],
        how="inner",
    )
    merged_df = merged_df.rename(columns={"idenviosme_y": "idenviosme"})
    merged_df = merged_df.drop("idenviosme_x", axis=1)
    merged_df = merged_df.drop_duplicates(
        subset=["idproducto", "evento", "idpoliza", "idoperacion"]
    )

    logger.info("actualizacion_envio_acsele - Limpiando tablas temporales")
    limpiar_temporal("interseguror.impmas_temp_envio")

    logger.info("actualizacion_envio_acsele - Insertando polizas en temporal")
    insertar_polizas_temporal(merged_df)

    logger.info(
        "actualizacion_envio_acsele - Actualizando estado sme de las polizas Acsele"
    )
    updates = update_impmas_desde_temp()

    response = {
        "polizas alloy": {"count": len(dfAlloy.to_dict(orient="records"))},
        "polizas sme": {"count": len(dfSme.to_dict(orient="records"))},
        "polizas mergeadas": {"count": len(merged_df.to_dict(orient="records"))},
        "polizas actualizadas": {"count": updates},
    }
    logger.info("actualizacion_envio_acsele - fin")
    return response
