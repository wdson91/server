import os
import logging

logger = logging.getLogger(__name__)

def remove_file_safely(file_path: str, file_type: str = "arquivo"):
    """Remove arquivo de forma segura"""
    try:
        if os.path.exists(file_path):
            os.remove(file_path)
            logger.info(f"🗑️ {file_type} removido: {os.path.basename(file_path)}")
            return True
    except Exception as e:
        logger.error(f"Erro ao remover {file_path}: {str(e)}")
    return False

def file_existis(xml_path):
    if not os.path.exists(xml_path):
        return {"status": "error", "file": xml_path, "message": "Arquivo não encontrado"}

def strip_nif_prefix(filename):
    """Remove o prefixo NIF do nome do ficheiro se existir (ex: '514151900_FR...' -> 'FR...')"""
    import re
    # Padrão: NIF (números ou B+números) seguido de underscore
    match = re.match(r'^[A-Z]?\d+_(.+)$', filename)
    if match:
        return match.group(1)
    return filename

def invoice_fr_or_nc(filename):
    """Detecta se o ficheiro é FR ou NC, mesmo com prefixo NIF"""
    clean_name = strip_nif_prefix(filename)
    return clean_name[0:2]
