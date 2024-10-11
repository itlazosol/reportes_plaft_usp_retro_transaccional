import pandas as pd
from io import StringIO
from .database import engines
from .logger import logger
from sqlalchemy.exc import SQLAlchemyError


def execute_query_to_df(query, bind):
    engine = engines[bind]
    try:
        logger.info(f'Ejecutando query => {query}')
        df = pd.read_sql(query, engine)
        return df
    except SQLAlchemyError as e:
        logger.error(f"Error al ejecutar la consulta: {str(e)}")
        raise 

def execute_query_no_results(query, bind):
    logger.info(f'Ejecutando query => {query}')
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        conn.commit()
    except Exception as error:
        logger.error(f"Error ejecutando la consulta: {error}")
        conn.rollback()
    finally:
        cursor.close()
        conn.close()
        
def execute_query_with_results(query, bind):
    logger.info(f'Ejecutando query => {query}')
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query)
        # Intenta obtener los resultados si es una consulta SELECT
        try:
            results = cursor.fetchall()
        except Exception as e:
            # Si fetchall() falla (por ejemplo, en un UPDATE), retorna el conteo de filas afectadas
            results = cursor.rowcount
            logger.info(f'Filas afectadas => {results}')
        conn.commit()
        return results
    except Exception as error:
        logger.error(f"Error ejecutando la consulta: {error}")
        conn.rollback()
        return None
    finally:
        cursor.close()
        conn.close()

def bulk_insert_from_df(df, table, schema, columns,bind):
    logger.info(f'Insertando en tabla  => {schema}.{table}')
    df = df[list(columns)]
    buffer = StringIO()
    df.to_csv(buffer, index=False, header=False)
    buffer.seek(0)
    engine = engines[bind]
    conn = engine.raw_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"SET search_path TO {schema}")
        cursor.copy_from(buffer, table, sep=",", columns=columns)
        conn.commit()
    except Exception as error:
        logger.error(f"Error durante bulk insert: {error}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()
    return True
