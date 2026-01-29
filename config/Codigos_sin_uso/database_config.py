""" Codigo Obsoleto, se prefierio usar un diccionario para evitar el uso de la libreria dotenv.
    En caso de necesitar traspasar este codigo a otros entorno u usar otros lenguajes,
    usar este archivo para los llamados con prefijos, se usa un sistema igual a lso diccionarios.
"""

import os
from dotenv import load_dotenv

load_dotenv()


#Definicion de la 
def get_db_config(prefix):
    return {
        'host': os.getenv(f'{prefix}_HOST'),
        'user': os.getenv(f'{prefix}_USER'),
        'password': os.getenv(f'{prefix}_PASS'),
        'database': os.getenv(f'{prefix}_NAME'),
        'port': int(os.getenv(f'{prefix}_PORT', 3306))
    }

DB_CONFIGS = {
    'equipment': get_db_config('PLC_DB'),
    'Codigo': get_db_config('CODIGO_DB'),
    'Combinacion': get_db_config('COMBINACION_DB')
}



