#logging_config.py
import logging
import logging.handlers
import os

class ColoredFormatter(logging.Formatter):
    """Personalized formatter that adds colors to console logs"""
    
    # Códigos de color ANSI
    COLORS = {
        'DEBUG': '\033[0;36m',  # Cyan
        'INFO': '\033[0;32m',   # Verde
        'WARNING': '\033[1;33m', # Amarillo
        'ERROR': '\033[1;31m',   # Rojo
        'CRITICAL': '\033[1;41m', # Fondo rojo
        'RESET': '\033[0m'       # Reset
    }

    def format(self, record):
        # Aplicar color según el nivel
        log_message = super().format(record)
        color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
        return f"{color}{log_message}{self.COLORS['RESET']}"


def _clear_existing_handlers():
    """Clean all existing handlers from all loggers)"""
    for logger_name in logging.root.manager.loggerDict:
        logger = logging.getLogger(logger_name)
        logger.handlers.clear()
    
    # Limpiar handlers del logger raíz
    logging.root.handlers.clear()

def configure_logging():
    """Configure logging for production"""
    # Limpiar handlers existentes
    _clear_existing_handlers()

    # Crear directorio de logs si no existe
    log_dir = './logs'
    os.makedirs(log_dir, exist_ok=True)
    
    # Configurar el logger raíz para capturar todos los logs
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Formato de los logs (para archivo)
    file_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler para archivo (todos los niveles desde INFO)
    file_handler = logging.handlers.RotatingFileHandler(
        os.path.join(log_dir, 'System_process.log'),
        maxBytes=10485760,  # 10MB
        backupCount=5,
        encoding='utf-8'
    )
    file_handler.setFormatter(file_formatter)
    file_handler.setLevel(logging.INFO)  # Captura INFO, WARNING, ERROR, CRITICAL
    
    # Formateador con colores para consola
    console_formatter = ColoredFormatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    
    # Handler para consola (solo WARNING y superiores)
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.WARNING)  # Solo warnings y errores en consola
    
    # Limpiar handlers existentes y agregar los nuevos
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    # Configurar logging para módulos específicos - EVITAR duplicación
    # No agregar handlers adicionales a loggers hijos
    for module_name in ['data_processor', 'equipment_data_handler', 'database.query_executor']:
        module_logger = logging.getLogger(module_name)
        module_logger.propagate = True  # Permite que los logs se propaguen al logger raíz
        module_logger.setLevel(logging.INFO)
    
    return logger