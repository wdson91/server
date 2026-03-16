import os
import sys
import logging
from pathlib import Path
sys.path.append(os.path.join(os.path.dirname(__file__), 'celery'))
from celery_config import celery_app
from sftp_connection import download_files_from_sftp, delete_file_from_sftp, connect_sftp, download_opengcs_files_from_sftp, delete_opengcs_file_from_sftp
from sftp_upload import upload_xml_to_sftp
from celery import chain

# Importar as novas referências
from utils.xml_parser import parse_xml_to_json, parse_opengcs_xml_to_json
from utils.file_utils import remove_file_safely, file_existis, invoice_fr_or_nc
from services.db_ops import process_and_insert_invoice_batch, process_nc_file, insert_opengcs_to_supabase

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de lote (lidas do env)
MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", "50"))
CLEANUP_AFTER_PROCESSING = os.getenv("CLEANUP_AFTER_PROCESSING", "true").lower() == "true"

def cleanup_processed_files():
    """Limpa arquivos processados das pastas"""
    if not CLEANUP_AFTER_PROCESSING:
        return
        
    try:
        # Limpar pasta downloads (arquivos XML)
        downloads_dir = './downloads'
        if os.path.exists(downloads_dir):
            xml_files = [f for f in os.listdir(downloads_dir) if f.endswith('.xml')]
            for xml_file in xml_files:
                file_path = os.path.join(downloads_dir, xml_file)
                #remove_file_safely(file_path, "Arquivo XML")
        
        # Limpar pasta dados_processados (arquivos JSON)
        dados_dir = './dados_processados'
        if os.path.exists(dados_dir):
            json_files = [f for f in os.listdir(dados_dir) if f.endswith('.json')]
            for json_file in json_files:
                file_path = os.path.join(dados_dir, json_file)
                #remove_file_safely(file_path, "Arquivo JSON")
                    
        logger.info("🧹 Limpeza automática concluída")
        
    except Exception as e:
        logger.error(f"Erro na limpeza automática: {str(e)}")

def process_single_opengcs_file(xml_file_path: str):
    """Tarefa Celery para processar um arquivo OpenGCs individual"""
    logger.info(f"🔄 Iniciando processamento do arquivo OpenGCs: {xml_file_path}")
    

    
    try:
        # Verificar se arquivo existe
        if not os.path.exists(xml_file_path):
            logger.error(f"❌ Arquivo não encontrado: {xml_file_path}")
            return {"status": "error", "file": xml_file_path, "message": "Arquivo não encontrado"}
        
        # Converter XML para JSON
        json_data = parse_opengcs_xml_to_json(xml_file_path)
        
        if json_data:
            # Inserir no Supabase
            insertion_success = insert_opengcs_to_supabase(json_data, xml_file_path)
            
            if insertion_success:
                # Excluir arquivo do SFTP apenas se a inserção foi bem-sucedida
                # logger.info(f"🗑️ Excluindo arquivo OpenGCs do SFTP após processamento bem-sucedido: {xml_file_path}")
                # sftp_deleted = delete_opengcs_file_from_sftp(xml_file_path)
                
                # if sftp_deleted:
                #     logger.info(f"✅ Arquivo OpenGCs excluído do SFTP com sucesso: {os.path.basename(xml_file_path)}")
                # else:
                #     logger.warning(f"⚠️ Falha ao excluir arquivo OpenGCs do SFTP: {os.path.basename(xml_file_path)}")
                
                # Remover arquivo local após processamento bem-sucedido
               # remove_file_safely(xml_file_path, "Arquivo OpenGCs XML")
                
                logger.info(f"✅ Arquivo OpenGCs processado com sucesso: {xml_file_path}")
                return {
                    "status": "success", 
                    "file": xml_file_path, 
                    "type": "OpenGCs",
                    "opengcs_count": json_data.get("opengcs_count", 0),
                    "opengcs_total": json_data.get("opengcs_total", 0)
                }
            else:
                # Se a inserção falhou, não excluir arquivo do SFTP
                logger.error(f"❌ Falha na inserção OpenGCs, arquivo não será excluído do SFTP: {xml_file_path}")
                return {
                    "status": "error", 
                    "file": xml_file_path, 
                    "type": "OpenGCs",
                    "message": "Falha na inserção no banco de dados"
                }
        else:
            logger.error(f"❌ Falha ao processar OpenGCs: {xml_file_path}")
            return {"status": "error", "file": xml_file_path, "type": "OpenGCs", "message": "Falha na conversão XML"}
            
    except Exception as e:
        logger.error(f"Erro ao processar OpenGCs {xml_file_path}: {str(e)}")
        return {"status": "error", "file": xml_file_path, "message": str(e)}

@celery_app.task
def download_and_queue_opengcs_files():
    """Tarefa Celery para baixar arquivos OpenGCs SFTP e criar tarefas individuais"""


    
    logger.info("🔄 Iniciando download de arquivos OpenGCs SFTP...")
    
    try:
        # Baixar arquivos OpenGCs do SFTP
        downloaded_files = download_opengcs_files_from_sftp()
        
        if not downloaded_files:
            logger.info("Nenhum arquivo OpenGCs encontrado no SFTP")
            return {"status": "success", "message": "Nenhum arquivo OpenGCs para processar", "queued_tasks": 0}
        
        # Limitar número de arquivos processados por vez
        files_to_process = downloaded_files[:MAX_FILES_PER_BATCH]
        remaining_files = len(downloaded_files) - len(files_to_process)
        
        logger.info(f"📊 Total de arquivos OpenGCs baixados: {len(downloaded_files)}")
        logger.info(f"📊 Arquivos OpenGCs a processar neste lote: {len(files_to_process)}")
        if remaining_files > 0:
            logger.info(f"📊 Arquivos OpenGCs restantes para próximo lote: {remaining_files}")
        
        # Criar tarefa individual para cada arquivo (limitado)
        queued_tasks = []
        for xml_file in files_to_process:
            # Criar tarefa individual no Celery
            result = process_single_opengcs_file(xml_file)
            queued_tasks.append({
                "file": xml_file,
                "task_id": result.get("task_id", None),
                "status": result.get("status", "unknown")
            })
        
        logger.info(f"✅ {len(queued_tasks)} tarefas OpenGCs criadas para processamento")
        
        return {
            "status": "success", 
            "message": f"{len(queued_tasks)} arquivos OpenGCs baixados e tarefas criadas (limite: {MAX_FILES_PER_BATCH})",
            "queued_tasks": len(queued_tasks),
            "total_files": len(downloaded_files),
            "processed_files": len(files_to_process),
            "remaining_files": remaining_files,
            "tasks": queued_tasks
        }
        
    except Exception as e:
        logger.error(f"Erro geral no download SFTP OpenGCs: {str(e)}")
        return {"status": "error", "message": str(e)}




@celery_app.task
def process_single_xml_file(xml_file_path: str):

    try:
        file_existis(xml_file_path)
        
        # Verificar se é um arquivo NC (Nota de Crédito)
        filename = os.path.basename(xml_file_path)
        file_type = invoice_fr_or_nc(filename)
        
        if file_type=='NC':
            # Processar arquivo NC (extrair referências e deletar faturas referenciadas)
            nc_result = process_nc_file(xml_file_path)
            
            # Agora também salvar a invoice NC no banco (mesmo processo das FRs)
            logger.info(f"🔄 Processando e salvando invoice NC no banco: {filename}")
            
            # Converter XML para JSON
            json_data = parse_xml_to_json(xml_file_path)
            
            if json_data:
                # Processar e inserir no Supabase usando dicionário de memória
                insertion_success = process_and_insert_invoice_batch(json_data)
                
                if insertion_success and nc_result["status"] in ["success", "warning"]:
                    # Excluir arquivo do SFTP apenas se a inserção foi bem-sucedida E o processamento de referências foi OK
                    logger.info(f"🗑️ Excluindo arquivo NC do SFTP após processamento bem-sucedido: {xml_file_path}")
                
                    sftp_deleted = delete_file_from_sftp(xml_file_path)
                    
                    if sftp_deleted:
                        logger.info(f"✅ Arquivo NC excluído do SFTP com sucesso: {filename}")
                    else:
                        logger.warning(f"⚠️ Falha ao excluir arquivo NC do SFTP: {filename}")
                    
                    # Remover arquivos locais após processamento bem-sucedido
                    remove_file_safely(xml_file_path, "Arquivo NC XML")
                    
                    logger.info(f"✅ Arquivo NC processado e salvo com sucesso: {xml_file_path}")
                    return {
                        "status": "success", 
                        "file": xml_file_path, 
                        "type": "NC",
                        "total_faturas": json_data.get("total_faturas", 0),
                        "deactivated_invoices": nc_result.get("deactivated_invoices", []),
                        "failed_deactivations": nc_result.get("failed_deactivations", []),
                        "total_references": nc_result.get("total_references", 0),
                        "message": f"NC salva no banco. {nc_result['message']}"
                    }
                elif not insertion_success:
                    # Se a inserção falhou, não excluir arquivo do SFTP
                    logger.error(f"❌ Falha ao salvar invoice NC no banco: {xml_file_path}")
                    return {
                        "status": "error", 
                        "file": xml_file_path, 
                        "type": "NC",
                        "message": "Falha na inserção da invoice NC no banco de dados",
                        "deactivated_invoices": nc_result.get("deactivated_invoices", []),
                        "failed_deactivations": nc_result.get("failed_deactivations", []),
                        "total_references": nc_result.get("total_references", 0)
                    }
                else:
                    # Se o processamento de referências falhou, mas a inserção foi OK
                    logger.warning(f"⚠️ Invoice NC salva, mas processamento de referências teve problemas: {xml_file_path}")
                    return {
                        "status": "warning", 
                        "file": xml_file_path, 
                        "type": "NC",
                        "total_faturas": json_data.get("total_faturas", 0),
                        "deactivated_invoices": nc_result.get("deactivated_invoices", []),
                        "failed_deactivations": nc_result.get("failed_deactivations", []),
                        "total_references": nc_result.get("total_references", 0),
                        "message": f"NC salva no banco, mas {nc_result.get('message', 'problemas no processamento de referências')}"
                    }
            else:
                # Se o parsing falhou, mas o processamento de referências foi OK
                if nc_result["status"] in ["success", "warning"]:
                    logger.warning(f"⚠️ Falha ao parsear invoice NC, mas referências foram processadas: {xml_file_path}")
                    return {
                        "status": "warning", 
                        "file": xml_file_path, 
                        "type": "NC",
                        "message": "Falha na conversão XML da invoice NC, mas referências foram processadas",
                        "deactivated_invoices": nc_result.get("deactivated_invoices", []),
                        "failed_deactivations": nc_result.get("failed_deactivations", []),
                        "total_references": nc_result.get("total_references", 0)
                    }
                else:
                    # Se ambos falharam
                    logger.error(f"❌ Falha ao processar invoice NC: {xml_file_path}")
                    return {
                        "status": "error", 
                        "file": xml_file_path, 
                        "type": "NC",
                        "message": nc_result.get("message", "Falha no processamento da invoice NC")
                    }
        
        # Processar arquivo FR (Fatura Regular)
        elif file_type == "FR":   
            # Converter XML para JSON
            json_data = parse_xml_to_json(xml_file_path)
            
            if json_data:
                # Processar e inserir no Supabase usando inserção em lote
                insertion_success = process_and_insert_invoice_batch(json_data)
                
                if insertion_success:
                    # Excluir arquivo do SFTP apenas se a inserção foi bem-sucedida
                    logger.info(f"🗑️ Excluindo arquivo do SFTP após processamento bem-sucedido: {xml_file_path}")
                
                    sftp_deleted = delete_file_from_sftp(xml_file_path)
                    
                    if sftp_deleted:
                         logger.info(f"✅ Arquivo excluído do SFTP com sucesso: {os.path.basename(xml_file_path)}")
                    else:
                         logger.warning(f"⚠️ Falha ao excluir arquivo do SFTP: {os.path.basename(xml_file_path)}")
                    
                    # Remover arquivos locais após processamento bem-sucedido
                    remove_file_safely(xml_file_path, "Arquivo XML")
                    
                    #logger.info(f"✅ Arquivo processado com sucesso: {xml_file_path}")
                    return {
                        "status": "success", 
                        "file": xml_file_path, 
                        "type": "FR",
                        "total_faturas": json_data.get("total_faturas", 0)
                    }
                else:
                    # Se a inserção falhou, não excluir arquivo do SFTP
                    
                    return {
                        "status": "error", 
                        "file": xml_file_path, 
                        "type": "FR",
                        "message": "Falha na inserção no banco de dados"
                    }
            else:
                #logger.error(f"❌ Falha ao processar: {xml_file_path}")
                return {"status": "error", "file": xml_file_path, "type": "FR", "message": "Falha na conversão XML"}
        else:
             
             return {"status": "error", "file": xml_file_path, "type": "FR", "message": "Falha na conversão XML"}  
    except Exception as e:
        #logger.error(f"Erro ao processar {xml_file_path}: {str(e)}")
        return {"status": "error", "file": xml_file_path, "message": str(e)}

@celery_app.task
def download_and_queue_sftp_files():
    """Tarefa Celery para baixar arquivos SFTP e criar tarefas individuais
    IMPORTANTE: Processa FRs primeiro, depois NCs para evitar inconsistências"""
    logger.info("🔄 Iniciando download de arquivos SFTP...")

    download_and_queue_opengcs_files_sync()

    try:
        # Baixar arquivos do SFTP
        downloaded_files = download_files_from_sftp()
        
        if not downloaded_files:
            logger.info("Nenhum arquivo XML encontrado no SFTP")
            return {"status": "success", "message": "Nenhum arquivo para processar", "queued_tasks": 0}
        
        # Separar arquivos por tipo (FR primeiro, depois NC)
        fr_files = []
        nc_files = []
        
        for xml_file in downloaded_files:
            filename = os.path.basename(xml_file)
            file_type = invoice_fr_or_nc(filename)
            
            if file_type == "FR":
                fr_files.append(xml_file)
            elif file_type == "NC":
                nc_files.append(xml_file)
            else:
                logger.warning(f"⚠️ Tipo de arquivo desconhecido: {filename}, será processado como FR")
                fr_files.append(xml_file)
        
        logger.info(f"📊 Arquivos separados: {len(fr_files)} FRs, {len(nc_files)} NCs")
        
        # Limitar número de arquivos processados por vez (aplicar limite separadamente)
        fr_to_process = fr_files[:MAX_FILES_PER_BATCH]
        nc_to_process = nc_files[:MAX_FILES_PER_BATCH]
        
        remaining_fr = len(fr_files) - len(fr_to_process)
        remaining_nc = len(nc_files) - len(nc_to_process)
        
        queued_tasks = []
        
        # PROCESSAR PRIMEIRO TODAS AS FRs (Faturas Regulares) - SEQUENCIALMENTE
        # Isso garante que todas as FRs sejam processadas antes das NCs
        logger.info(f"📄 Processando {len(fr_to_process)} arquivos FR primeiro (sequencialmente)...")
        fr_results = []
        for i, xml_file in enumerate(fr_to_process, 1):
            logger.info(f"🔄 Processando FR {i}/{len(fr_to_process)}: {os.path.basename(xml_file)}")
            try:
                # Processar de forma síncrona para garantir ordem
                result = process_single_xml_file(xml_file)
                fr_results.append({
                    "file": xml_file,
                    "status": result.get("status", "unknown"),
                    "type": "FR"
                })
                logger.info(f"✅ FR {i}/{len(fr_to_process)} concluída: {result.get('status', 'unknown')}")
            except Exception as e:
                logger.error(f"❌ Erro ao processar FR {i}/{len(fr_to_process)}: {str(e)}")
                fr_results.append({
                    "file": xml_file,
                    "status": "error",
                    "error": str(e),
                    "type": "FR"
                })
        
        logger.info(f"✅ Todas as {len(fr_to_process)} tarefas FR foram concluídas")
        
        # AGORA PROCESSAR AS NCs (Notas de Crédito) - PODE SER PARALELO
        logger.info(f"📝 Processando {len(nc_to_process)} arquivos NC após conclusão das FRs...")
        nc_tasks = []
        for xml_file in nc_to_process:
            # Criar tarefa individual no Celery (pode processar em paralelo)
            task = process_single_xml_file.delay(xml_file)
            nc_tasks.append(task)
            queued_tasks.append({
                "file": xml_file,
                "task_id": task.id,
                "type": "NC"
            })
            logger.info(f"📋 Tarefa NC criada para: {os.path.basename(xml_file)} (ID: {task.id})")
        
        # Adicionar resultados das FRs processadas
        queued_tasks.extend(fr_results)
        
        logger.info(f"✅ {len(queued_tasks)} tarefas criadas para processamento ({len(fr_to_process)} FRs + {len(nc_to_process)} NCs)")
        
        return {
            "status": "success", 
            "message": f"{len(fr_to_process)} FRs processadas primeiro, depois {len(nc_to_process)} NCs (limite: {MAX_FILES_PER_BATCH})",
            "queued_tasks": len(queued_tasks),
            "fr_files": len(fr_to_process),
            "nc_files": len(nc_to_process),
            "remaining_fr": remaining_fr,
            "remaining_nc": remaining_nc,
            "total_files": len(downloaded_files),
            "tasks": queued_tasks
        }
        
    except Exception as e:
        logger.error(f"Erro geral no download SFTP: {str(e)}")
        return {"status": "error", "message": str(e)}


@celery_app.task
def cleanup_files_task():
    """Tarefa Celery para limpeza programada de arquivos"""
    logger.info("🧹 Iniciando limpeza programada de arquivos...")
    
    try:
        cleanup_processed_files()
        
        return {
            "status": "success",
            "message": "Limpeza programada concluída"
        }
        
    except Exception as e:
        logger.error(f"Erro na limpeza programada: {str(e)}")
        return {
            "status": "error",
            "message": str(e)
        } 
    

@celery_app.task
def download_all():
    chain(
        download_and_queue_opengcs_files.s(),
        download_and_queue_sftp_files.s()
    ).apply_async()

def download_and_queue_opengcs_files_sync():
    """Mesma lógica do download_and_queue_opengcs_files, mas rodando síncrono"""
    logger.info("🔄 Baixando arquivos OpenGCs de forma síncrona...")

    downloaded_files = download_opengcs_files_from_sftp()
    if not downloaded_files:
        logger.info("Nenhum arquivo OpenGCs encontrado")
        return

    files_to_process = downloaded_files[:MAX_FILES_PER_BATCH]

    for xml_file in files_to_process:
        # Chama process_single_opengcs_file de forma síncrona
        process_single_opengcs_file(xml_file)
    
    logger.info(f"✅ {len(files_to_process)} arquivos OpenGCs processados")


@celery_app.task(bind=True, max_retries=3)
def async_upload_xml_to_sftp(self, xml_string: str, nif: str, filename: str):
    """
    Task Celery que empurra o processo lento do SFTP para um processo em background.
    """
    try:
        # Reconverter a string que viajou pelo broker (ex: Redis) em bytes para a sua função aceitar
        xml_bytes = xml_string.encode('utf-8')
        
        # Chama a sua função original que já funciona bem!
        result = upload_xml_to_sftp(xml_content=xml_bytes, nif=nif, filename=filename)
        return result
        
    except Exception as exc:
        # Se o SFTP der um pico de erro de ligação, tentamos outra vez (máximo 3 vezes)
        logger.error(f"Erro no upload SFTP: {exc}. A tentar de novo em 10s...")
        raise self.retry(exc=exc, countdown=10)
