import os
import json
import xmltodict
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional
from celery import Celery
from dotenv import load_dotenv
from supabase import create_client, Client

# Importar configurações
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'celery'))
from celery_config import celery_app
from sftp_connection import download_files_from_sftp, delete_file_from_sftp

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configurações de lote
BATCH_SIZE_COMPANIES = int(os.getenv("BATCH_SIZE_COMPANIES", "1000"))
BATCH_SIZE_INVOICES = int(os.getenv("BATCH_SIZE_INVOICES", "500"))
BATCH_SIZE_LINES = int(os.getenv("BATCH_SIZE_LINES", "2000"))
BATCH_SIZE_LINKS = int(os.getenv("BATCH_SIZE_LINKS", "500"))

# Limite de arquivos processados por vez
MAX_FILES_PER_BATCH = int(os.getenv("MAX_FILES_PER_BATCH", "50"))

# Configuração de limpeza automática
CLEANUP_AFTER_PROCESSING = os.getenv("CLEANUP_AFTER_PROCESSING", "true").lower() == "true"

load_dotenv()

# Configuração Supabase
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def extract_filial_from_invoice_no(invoice_no: str) -> str:
    """Extrai a filial do número da fatura (valor entre FR e Y)"""
    try:
        if not invoice_no:
            return ""
        
        # Procurar por padrão FR + espaço + números + Y
        import re
        pattern = r'FR\s+(\d+)Y'
        match = re.search(pattern, invoice_no)
        
        if match:
            filial = match.group(1)
            logger.info(f"✅ Filial extraída: {filial} do número: {invoice_no}")
            return filial
        else:
            logger.warning(f"⚠️ Padrão de filial não encontrado em: {invoice_no}")
            return ""
            
    except Exception as e:
        logger.error(f"❌ Erro ao extrair filial de {invoice_no}: {str(e)}")
        return ""

def parse_xml_to_json(xml_file_path: str) -> Optional[dict]:
    """Converte arquivo XML para JSON"""
    try:
        logger.info(f"🔄 Processando XML: {xml_file_path}")
        
        with open(xml_file_path, 'r', encoding='utf-8') as file:
            xml_content = file.read()
        
        # Converter XML para dict
        xml_dict = xmltodict.parse(xml_content)
        logger.info(f"✅ XML convertido para dict com sucesso")
        
        # Extrair dados relevantes do SAFT
        saft_data = {
            "arquivo_origem": os.path.basename(xml_file_path),
            "data_processamento": datetime.now().isoformat(),
            "total_faturas": 0,
            "faturas": []
        }
        
        # Processar dados do SAFT (estrutura básica)
        if 'AuditFile' in xml_dict:
            audit_file = xml_dict['AuditFile']
            logger.info(f"✅ AuditFile encontrado no XML")
        else:
            logger.warning(f"⚠️ AuditFile não encontrado no XML")
            return None
            
        # Extrair informações da empresa do Header
        company_data = {
            "CompanyID": "",
            "CompanyName": "",
            "AddressDetail": "",
            "City": "",
            "PostalCode": "",
            "Country": ""
        }
        
        if 'Header' in audit_file:
            header = audit_file['Header']
            company_data = {
                "CompanyID": header.get('CompanyID', ''),
                "CompanyName": header.get('CompanyName', ''),
                "AddressDetail": header.get('CompanyAddress', {}).get('AddressDetail', ''),
                "City": header.get('CompanyAddress', {}).get('City', ''),
                "PostalCode": header.get('CompanyAddress', {}).get('PostalCode', ''),
                "Country": header.get('CompanyAddress', {}).get('Country', '')
            }
            logger.info(f"✅ Dados da empresa extraídos do Header")
        
        # Extrair faturas
        if 'SourceDocuments' in audit_file:
            source_docs = audit_file['SourceDocuments']
            logger.info(f"✅ SourceDocuments encontrado")
            
            if 'SalesInvoices' in source_docs:
                sales_invoices = source_docs['SalesInvoices']
                logger.info(f"✅ SalesInvoices encontrado")
                
                if 'Invoice' in sales_invoices:
                    invoices = sales_invoices['Invoice'] if isinstance(sales_invoices['Invoice'], list) else [sales_invoices['Invoice']]
                    logger.info(f"✅ {len(invoices)} faturas encontradas")
                else:
                    logger.warning(f"⚠️ Nenhuma fatura encontrada no XML")
                    return saft_data
            else:
                logger.warning(f"⚠️ SalesInvoices não encontrado no XML")
                return saft_data
        else:
            logger.warning(f"⚠️ SourceDocuments não encontrado no XML")
            return saft_data
        
        for invoice in invoices:
            # Extrair dados do DocumentStatus
            document_status = invoice.get('DocumentStatus', {})
            invoice_status_date = document_status.get('InvoiceStatusDate', '')
            
            # Extrair dados do DocumentTotals
            document_totals = invoice.get('DocumentTotals', {})
            payment = document_totals.get('Payment', {})
            
            # Extrair dados do Line (pode ser lista ou dict)
            line_data = invoice.get('Line', {})
            if isinstance(line_data, list):
                line_data = line_data[0] if line_data else {}
            tax_data = line_data.get('Tax', {})
            
            # Extrair filial do número da fatura
            invoice_no = invoice.get('InvoiceNo', '')
            filial = extract_filial_from_invoice_no(invoice_no)
            
            fatura = {
                "CompanyID": company_data["CompanyID"],
                "CompanyName": company_data["CompanyName"],
                "AddressDetail": company_data["AddressDetail"],
                "City": company_data["City"],
                "PostalCode": company_data["PostalCode"],
                "Country": company_data["Country"],
                "InvoiceNo": invoice_no,
                "Filial": filial,  # Novo campo filial
                "ATCUD": invoice.get('ATCUD', ''),
                "CustomerID": invoice.get('CustomerID', ''),
                "InvoiceDate": invoice.get('InvoiceDate', ''),
                "InvoiceStatusDate_Date": invoice_status_date.split('T')[0] if invoice_status_date and 'T' in invoice_status_date else '',
                "InvoiceStatusDate_Time": invoice_status_date.split('T')[1] if invoice_status_date and 'T' in invoice_status_date else '',
                "HashExtract": invoice.get('Hash', '')[0] + invoice.get('Hash', '')[10] + invoice.get('Hash', '')[20] + invoice.get('Hash', '')[30] if len(invoice.get('Hash', '')) > 30 else invoice.get('Hash', ''),
                "EndDate": invoice.get('EndDate', ''),
                "TaxPayable": float(document_totals.get('TaxPayable', 0)),
                "NetTotal": float(document_totals.get('NetTotal', 0)),
                "GrossTotal": float(document_totals.get('GrossTotal', 0)),
                "PaymentAmount": float(payment.get('PaymentAmount', 0)),
                "TaxType": tax_data.get('TaxType', ''),
                "Lines": []
            }
            
            # Processar linhas da fatura
            if 'Line' in invoice:
                lines = invoice['Line'] if isinstance(invoice['Line'], list) else [invoice['Line']]
                for line in lines:
                    line_tax = line.get('Tax', {})
                    fatura["Lines"].append({
                        "LineNumber": int(line.get('LineNumber', 0)),
                        "ProductCode": line.get('ProductCode', ''),
                        "Description": line.get('Description', ''),
                        "Quantity": float(line.get('Quantity', 0)),
                        "UnitPrice": float(line.get('UnitPrice', 0)),
                        "CreditAmount": float(line.get('CreditAmount', 0)),
                        "TaxPercentage": float(line_tax.get('TaxPercentage', 0)),
                        "PriceWithIva": float(line.get('CreditAmount', 0))  # Usar CreditAmount como PriceWithIva
                    })
            
            saft_data["faturas"].append(fatura)
        
        saft_data["total_faturas"] = len(saft_data["faturas"])
        logger.info(f"✅ Processamento concluído: {saft_data['total_faturas']} faturas extraídas")
        
        return saft_data
        
    except Exception as e:
        logger.error(f"Erro ao processar XML {xml_file_path}: {str(e)}")
        return None

def insert_companies_batch(companies_data):
    """Insere empresas em lote"""
    try:
        if not companies_data:
            logger.warning("⚠️ Nenhuma empresa para inserir")
            return None
        
        logger.info(f"🏢 Tentando inserir {len(companies_data)} empresas...")
        
        # Usar upsert para evitar duplicatas
        response = supabase.table("companies").upsert(
            companies_data,
            on_conflict="company_id"
        ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} empresas inseridas/atualizadas")
        else:
            logger.warning("⚠️ Nenhuma empresa foi inserida")
            
        return response
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir empresas em lote: {str(e)}")
        return None

def insert_invoices_batch(invoices_data):
    """Insere faturas em lote"""
    try:
        if not invoices_data:
            logger.warning("⚠️ Nenhuma fatura para inserir")
            return None
        
        logger.info(f"📄 Tentando inserir {len(invoices_data)} faturas...")
        
        # Usar upsert para evitar duplicatas
        response = supabase.table(
        "invoices").upsert(
            invoices_data,
            on_conflict="invoice_no"
        ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} faturas inseridas/atualizadas")
        else:
            logger.warning("⚠️ Nenhuma fatura foi inserida")
            
        return response
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir faturas em lote: {str(e)}")
        return None

def insert_invoice_lines_batch(lines_data):
    """Insere linhas de faturas em lote"""
    try:
        if not lines_data:
            return
        
        # Inserir linhas em lote
        response = supabase.table("invoice_lines").insert(lines_data).execute()
        
        logger.info(f"✅ {len(lines_data)} linhas de faturas processadas em lote")
        return response
        
    except Exception as e:
        logger.error(f"Erro ao inserir linhas em lote: {str(e)}")
        return None

def insert_file_links_batch(links_data):
    """Insere links de arquivos em lote"""
    try:
        if not links_data:
            return
        
        # Inserir links em lote
        response = supabase.table("invoice_file_links").insert(links_data).execute()
        
        logger.info(f"✅ {len(links_data)} links de arquivos processados em lote")
        return response
        
    except Exception as e:
        logger.error(f"Erro ao inserir links em lote: {str(e)}")
        return None

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
                remove_file_safely(file_path, "Arquivo XML")
        
        # Limpar pasta dados_processados (arquivos JSON)
        dados_dir = './dados_processados'
        if os.path.exists(dados_dir):
            json_files = [f for f in os.listdir(dados_dir) if f.endswith('.json')]
            for json_file in json_files:
                file_path = os.path.join(dados_dir, json_file)
                remove_file_safely(file_path, "Arquivo JSON")
                    
        logger.info("🧹 Limpeza automática concluída")
        
    except Exception as e:
        logger.error(f"Erro na limpeza automática: {str(e)}")

def process_and_insert_invoice_batch(file_path: Path):
    """Processa e insere fatura no Supabase usando inserção em lote"""
    try:
        logger.info(f"🔄 Iniciando inserção em lote: {file_path}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"📊 Dados carregados: {data['total_faturas']} faturas")

        # Preparar dados para inserção em lote
        companies_batch = []
        invoices_batch = []
        links_batch = []

        # Armazenar linhas por fatura para inserção posterior
        lines_by_invoice = {}

        for fatura in data["faturas"]:
            # Preparar empresa para lote
            companies_batch.append({
                "company_id": fatura["CompanyID"],
                "company_name": fatura["CompanyName"],
                "address_detail": fatura["AddressDetail"],
                "city": fatura["City"],
                "postal_code": fatura["PostalCode"],
                "country": fatura["Country"]
            })

            # Preparar fatura para lote
            invoices_batch.append({
                "invoice_no": fatura["InvoiceNo"],
                "filial": fatura["Filial"],  # Novo campo filial
                "atcud": fatura["ATCUD"],
                "company_id": fatura["CompanyID"],
                "customer_id": fatura["CustomerID"],
                "invoice_date": fatura["InvoiceDate"] if fatura["InvoiceDate"] else None,
                "invoice_status_date": fatura["InvoiceStatusDate_Date"] if fatura["InvoiceStatusDate_Date"] else None,
                "invoice_status_time": fatura["InvoiceStatusDate_Time"] if fatura["InvoiceStatusDate_Time"] else None,
                "hash_extract": fatura["HashExtract"],
                "end_date": fatura["EndDate"] if fatura["EndDate"] else None,
                "tax_payable": fatura["TaxPayable"],
                "net_total": fatura["NetTotal"],
                "gross_total": fatura["GrossTotal"],
                "payment_amount": float(str(fatura["PaymentAmount"]).replace(",", ".")),
                "tax_type": fatura["TaxType"]
            })

            # Armazenar linhas por fatura (não inserir ainda)
            lines_by_invoice[fatura["InvoiceNo"]] = []
            for linha in fatura["Lines"]:
                lines_by_invoice[fatura["InvoiceNo"]].append({
                    "line_number": linha["LineNumber"],
                    "product_code": linha["ProductCode"],
                    "description": linha["Description"],
                    "quantity": linha["Quantity"],
                    "unit_price": linha["UnitPrice"],
                    "credit_amount": linha["CreditAmount"],
                    "tax_percentage": linha["TaxPercentage"],
                    "price_with_iva": float(str(linha["PriceWithIva"]).replace(",", "."))
                })

        # Inserir empresas em lote
        logger.info(f"🏢 Inserindo {len(companies_batch)} empresas...")
        insert_companies_batch(companies_batch)

        # Inserir faturas em lote
        logger.info(f"📄 Inserindo {len(invoices_batch)} faturas...")
        invoices_response = insert_invoices_batch(invoices_batch)
        
        if invoices_response and invoices_response.data:
            # Verificar se o arquivo já existe
            existing_file = supabase.table("invoice_files").select("id").eq("filename", data["arquivo_origem"]).execute()
            
            if existing_file.data:
                # Arquivo já existe, usar o ID existente
                file_id = existing_file.data[0]["id"]
                logger.info(f"ℹ️ Arquivo já existe com ID: {file_id}, reutilizando")
            else:
                # Só inserir o arquivo se pelo menos uma fatura foi inserida com sucesso
                logger.info("📝 Inserindo arquivo no banco (faturas foram inseridas)...")
                file_insert = supabase.table("invoice_files").insert({
                    "filename": data["arquivo_origem"],
                    "data_processamento": data["data_processamento"],
                    "total_faturas": data["total_faturas"]
                }).execute()

                if not file_insert.data:
                    logger.error("❌ Erro: Resposta vazia ao inserir arquivo")
                    return False

                file_id = file_insert.data[0]["id"]
                logger.info(f"✅ Arquivo inserido com ID: {file_id}")

            # Mapear invoice_no para invoice_id para as linhas
            invoice_mapping = {}
            for invoice in invoices_response.data:
                invoice_mapping[invoice["invoice_no"]] = invoice["id"]

            # Preparar linhas apenas para faturas inseridas com sucesso
            lines_batch = []
            
            for fatura in data["faturas"]:
                invoice_id = invoice_mapping.get(fatura["InvoiceNo"])
                if invoice_id:
                    # Verificar se já existem linhas para esta fatura
                    existing_lines = supabase.table("invoice_lines").select("line_number").eq("invoice_id", invoice_id).execute()
                    existing_line_numbers = {line["line_number"] for line in existing_lines.data} if existing_lines.data else set()
                    
                    # Adicionar linhas apenas se a fatura foi inserida com sucesso E se não existem linhas duplicadas
                    if fatura["InvoiceNo"] in lines_by_invoice:
                        for linha in lines_by_invoice[fatura["InvoiceNo"]]:
                            # Verificar se esta linha já existe
                            if linha["line_number"] not in existing_line_numbers:
                                linha_with_invoice_id = linha.copy()
                                linha_with_invoice_id["invoice_id"] = invoice_id
                                lines_batch.append(linha_with_invoice_id)
                            else:
                                logger.info(f"ℹ️ Linha {linha['line_number']} já existe para fatura {fatura['InvoiceNo']}, ignorando")

                    # Verificar se já existe link para esta fatura
                    existing_link = supabase.table("invoice_file_links").select("id").eq("invoice_id", invoice_id).eq("invoice_file_id", file_id).execute()
                    if not existing_link.data:
                        # Adicionar link do arquivo apenas se não existir
                        links_batch.append({
                            "invoice_file_id": file_id,
                            "invoice_id": invoice_id
                        })
                    else:
                        logger.info(f"ℹ️ Link já existe para fatura {fatura['InvoiceNo']}, ignorando")
                else:
                    logger.warning(f"⚠️ Fatura {fatura['InvoiceNo']} não foi inserida, linhas serão ignoradas")

            # Inserir linhas em lote (apenas para faturas inseridas)
            if lines_batch:
                logger.info(f"📋 Inserindo {len(lines_batch)} linhas de faturas...")
                insert_invoice_lines_batch(lines_batch)
            else:
                logger.warning("⚠️ Nenhuma linha para inserir (faturas não foram inseridas)")

            # Inserir links em lote
            if links_batch:
                logger.info(f"🔗 Inserindo {len(links_batch)} links de arquivos...")
                insert_file_links_batch(links_batch)
            else:
                logger.warning("⚠️ Nenhum link para inserir (faturas não foram inseridas)")
        else:
            logger.error("❌ Falha ao inserir faturas, arquivo e linhas não serão inseridas")
            return False

        logger.info(f"✅ Arquivo processado com sucesso: {file_path}")
        return True
                
    except Exception as e:
        logger.error(f"Erro ao processar arquivo {file_path}: {str(e)}")
        return False

@celery_app.task
def process_single_xml_file(xml_file_path: str):
    """Tarefa Celery para processar um arquivo XML individual"""
    logger.info(f"🔄 Iniciando processamento do arquivo: {xml_file_path}")
    
    try:
        # Verificar se arquivo existe
        if not os.path.exists(xml_file_path):
            logger.error(f"❌ Arquivo não encontrado: {xml_file_path}")
            return {"status": "error", "file": xml_file_path, "message": "Arquivo não encontrado"}
        
        # Converter XML para JSON
        json_data = parse_xml_to_json(xml_file_path)
        
        if json_data:
            # Salvar JSON processado
            pasta_dados_processados = './dados_processados'
            os.makedirs(pasta_dados_processados, exist_ok=True)
            
            json_filename = Path(xml_file_path).stem + '.json'
            json_path = os.path.join(pasta_dados_processados, json_filename)
            
            with open(json_path, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            
            # Processar e inserir no Supabase usando inserção em lote
            insertion_success = process_and_insert_invoice_batch(Path(json_path))
            
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
                remove_file_safely(json_path, "Arquivo JSON")
                
                logger.info(f"✅ Arquivo processado com sucesso: {xml_file_path}")
                return {
                    "status": "success", 
                    "file": xml_file_path, 
                    "total_faturas": json_data.get("total_faturas", 0)
                }
            else:
                # Se a inserção falhou, não excluir arquivo do SFTP
                logger.error(f"❌ Falha na inserção, arquivo não será excluído do SFTP: {xml_file_path}")
                return {
                    "status": "error", 
                    "file": xml_file_path, 
                    "message": "Falha na inserção no banco de dados"
                }
        else:
            logger.error(f"❌ Falha ao processar: {xml_file_path}")
            return {"status": "error", "file": xml_file_path, "message": "Falha na conversão XML"}
            
    except Exception as e:
        logger.error(f"Erro ao processar {xml_file_path}: {str(e)}")
        return {"status": "error", "file": xml_file_path, "message": str(e)}

@celery_app.task
def download_and_queue_sftp_files():
    """Tarefa Celery para baixar arquivos SFTP e criar tarefas individuais"""
    logger.info("🔄 Iniciando download de arquivos SFTP...")
    
    try:
        # Baixar arquivos do SFTP
        downloaded_files = download_files_from_sftp()
        
        if not downloaded_files:
            logger.info("Nenhum arquivo XML encontrado no SFTP")
            return {"status": "success", "message": "Nenhum arquivo para processar", "queued_tasks": 0}
        
        # Limitar número de arquivos processados por vez
        files_to_process = downloaded_files[:MAX_FILES_PER_BATCH]
        remaining_files = len(downloaded_files) - len(files_to_process)
        
        logger.info(f"📊 Total de arquivos baixados: {len(downloaded_files)}")
        logger.info(f"📊 Arquivos a processar neste lote: {len(files_to_process)}")
        if remaining_files > 0:
            logger.info(f"📊 Arquivos restantes para próximo lote: {remaining_files}")
        
        # Criar tarefa individual para cada arquivo (limitado)
        queued_tasks = []
        for xml_file in files_to_process:
            # Criar tarefa individual no Celery
            task = process_single_xml_file.delay(xml_file)
            queued_tasks.append({
                "file": xml_file,
                "task_id": task.id
            })
            logger.info(f"📋 Tarefa criada para: {xml_file} (ID: {task.id})")
        
        logger.info(f"✅ {len(queued_tasks)} tarefas criadas para processamento")
        
        return {
            "status": "success", 
            "message": f"{len(queued_tasks)} arquivos baixados e tarefas criadas (limite: {MAX_FILES_PER_BATCH})",
            "queued_tasks": len(queued_tasks),
            "total_files": len(downloaded_files),
            "processed_files": len(files_to_process),
            "remaining_files": remaining_files,
            "tasks": queued_tasks
        }
        
    except Exception as e:
        logger.error(f"Erro geral no download SFTP: {str(e)}")
        return {"status": "error", "message": str(e)}

@celery_app.task
def process_sftp_files():
    """Tarefa Celery para processar arquivos SFTP (mantida para compatibilidade)"""
    logger.info("🔄 Iniciando processamento de arquivos SFTP...")
    
    try:
        # Baixar arquivos do SFTP
        downloaded_files = download_files_from_sftp()
        
        if not downloaded_files:
            logger.info("Nenhum arquivo XML encontrado no SFTP")
            return {"status": "success", "message": "Nenhum arquivo para processar"}
        
        # Limitar número de arquivos processados por vez
        files_to_process = downloaded_files[:MAX_FILES_PER_BATCH]
        remaining_files = len(downloaded_files) - len(files_to_process)
        
        logger.info(f"📊 Total de arquivos baixados: {len(downloaded_files)}")
        logger.info(f"📊 Arquivos a processar neste lote: {len(files_to_process)}")
        if remaining_files > 0:
            logger.info(f"📊 Arquivos restantes para próximo lote: {remaining_files}")
        
        # Pasta para dados processados
        pasta_dados_processados = './dados_processados'
        os.makedirs(pasta_dados_processados, exist_ok=True)
        
        processed_count = 0
        
        for xml_file in files_to_process:
            try:
                # Converter XML para JSON
                json_data = parse_xml_to_json(xml_file)
                
                if json_data:
                    # Salvar JSON processado
                    json_filename = Path(xml_file).stem + '.json'
                    json_path = os.path.join(pasta_dados_processados, json_filename)
                    
                    with open(json_path, 'w', encoding='utf-8') as f:
                        json.dump(json_data, f, ensure_ascii=False, indent=2)
                    
                    # Processar e inserir no Supabase usando inserção em lote
                    process_and_insert_invoice_batch(Path(json_path))
                    
                    # Remover arquivos após processamento
                    #remove_file_safely(xml_file, "Arquivo XML")
                    #remove_file_safely(json_path, "Arquivo JSON")
                    processed_count += 1
                    
                    logger.info(f"✅ Processado: {xml_file}")
                else:
                    logger.error(f"❌ Falha ao processar: {xml_file}")
                    
            except Exception as e:
                logger.error(f"Erro ao processar {xml_file}: {str(e)}")
        
        logger.info(f"✅ Processamento concluído. {processed_count} arquivos processados.")
        
        return {"status": "success", "processed_count": processed_count}
        
    except Exception as e:
        logger.error(f"Erro geral no processamento SFTP: {str(e)}")
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