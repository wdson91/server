import json
import xmltodict
import logging
import re
from pathlib import Path
from datetime import datetime, timezone
import pytz
from typing import Optional
from dotenv import load_dotenv
from supabase import create_client, Client

# Importar configurações
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'celery'))
from celery_config import celery_app
from sftp_connection import download_files_from_sftp, delete_file_from_sftp, connect_sftp

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

def read_xml_file_with_encoding(xml_file_path: str, file_type: str = "XML") -> Optional[str]:
    """Lê arquivo XML tentando diferentes codificações"""
    encodings = [ 'latin-1', 'utf-8','iso-8859-1', 'cp1252']
    
    for encoding in encodings:
        try:
            with open(xml_file_path, 'r', encoding=encoding) as file:
                xml_content = file.read()
            logger.info(f"✅ {file_type} lido com sucesso usando encoding: {encoding}")
            return xml_content
        except UnicodeDecodeError as e:
            logger.warning(f"⚠️ Falha ao ler {file_type} com encoding {encoding}: {str(e)}")
            continue
    
    logger.error(f"❌ Não foi possível ler o arquivo {file_type} com nenhuma codificação: {encodings}")
    return None

def extract_filial_from_filename(filename: str) -> str:
    """Extrai a filial do nome do arquivo (ex: FR202Y2025_7-Gramido -> Gramido ou NC202Y2025_7-Gramido -> Gramido)"""
    try:
        if not filename:
            return ""
        
        # Procurar por padrão FR ou NC + números + Y + números + _ + números + - + nome_filial
        import re
        pattern = r'(FR|NC)\d+Y\d+_\d+-(.+)'
        match = re.search(pattern, filename)
        
        if match:
            filial = match.group(2)
            # Remover extensão .xml se existir
            filial = filial.replace('.xml', '')
            logger.info(f"✅ Filial extraída: {filial} do arquivo: {filename}")
            return filial
        else:
            logger.warning(f"⚠️ Padrão de filial não encontrado em: {filename}")
            return ""
            
    except Exception as e:
        logger.error(f"❌ Erro ao extrair filial de {filename}: {str(e)}")
        return ""

def parse_xml_to_json(xml_file_path: str) -> Optional[dict]:
    """Converte arquivo XML para JSON"""
    try:
        logger.info(f"🔄 Processando XML: {xml_file_path}")
        
        # Ler arquivo com codificação adequada
        xml_content = read_xml_file_with_encoding(xml_file_path, "XML")
        if xml_content is None:
            return None
        
        # Converter XML para dict
        xml_dict = xmltodict.parse(xml_content)
        logger.info(f"✅ XML convertido para dict com sucesso")
        
        # Extrair dados relevantes do SAFT
        saft_data = {
            "arquivo_origem": os.path.basename(xml_file_path),
            "data_processamento": datetime.now(tz=pytz.timezone('Europe/Lisbon')).isoformat(),
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
            "Country": "",
            "SoftwareCertificateNumber": ""
        }
        
        if 'Header' in audit_file:
            header = audit_file['Header']
            company_data = {
                "CompanyID": header.get('CompanyID', ''),
                "CompanyName": header.get('CompanyName', ''),
                "AddressDetail": header.get('CompanyAddress', {}).get('AddressDetail', ''),
                "City": header.get('CompanyAddress', {}).get('City', ''),
                "PostalCode": header.get('CompanyAddress', {}).get('PostalCode', ''),
                "Country": header.get('CompanyAddress', {}).get('Country', ''),
                "SoftwareCertificateNumber": header.get('SoftwareCertificateNumber', '')
               
            }

        customer_data = {
                    "CustomerID": "Desconhecido",
                    "AccountID": "",
                    "CustomerTaxID": "",
                    "CompanyName": "",
                    "PostalCode": "",
                    "AddressDetail": "",
                    "City": ""
                }
        if "MasterFiles" in audit_file:
            master_files  = audit_file["MasterFiles"]["Customer"]
        
            if master_files.get('CustomerID') != 999999990:
                customer_data = {
                    "CustomerID": master_files.get('CustomerID','Desconhecido'),
                    "AccountID": master_files.get('AccountID','Desconhecido'),
                    "CustomerTaxID": master_files.get('CustomerTaxID','Desconhecido'),
                    "CompanyName": master_files.get('CompanyName','Desconhecido'),
                    "PostalCode": master_files.get('BillingAddress',{}).get('PostalCode','Desconhecido'),
                    "AddressDetail": master_files.get('BillingAddress',{}).get('AddressDetail','Desconhecido'),
                    "City": master_files.get('BillingAddress',{}).get('City','Desconhecido'),
                }
                
        # Extrair faturas
        if 'SourceDocuments' in audit_file:
            source_docs = audit_file['SourceDocuments']
            
            if 'SalesInvoices' in source_docs:
                sales_invoices = source_docs['SalesInvoices']
                
                if 'Invoice' in sales_invoices:
                    invoices = sales_invoices['Invoice'] if isinstance(sales_invoices['Invoice'], list) else [sales_invoices['Invoice']]
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
            payments = document_totals.get('Payment', {})
            if isinstance(payments,list):
              payment = [ x for x in payments]
            
            # Extrair dados do Line (pode ser lista ou dict)
            line_data = invoice.get('Line', {})
            if isinstance(line_data, list):
                line_data = line_data[0] if line_data else {}
            tax_data = line_data.get('Tax', {})
            
            # Extrair filial do nome do arquivo
            filename = os.path.basename(xml_file_path)
            filial = extract_filial_from_filename(filename)
            
            # Inicializar variáveis para armazenar primeira referência e motivo (para notas de crédito)
            nc_reason_data = None  # Será um dict: {"fatura_ref": "...", "reason": "..."}
            
            fatura = {
                "CompanyID": company_data["CompanyID"],
                "CompanyName": company_data["CompanyName"],
                "AddressDetail": company_data["AddressDetail"],
                "City": company_data["City"],
                "PostalCode": company_data["PostalCode"],
                "Country": company_data["Country"],
                "CertificateNumber": company_data["SoftwareCertificateNumber"],
                "InvoiceNo": invoice.get('InvoiceNo', ''),
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
                "PaymentMethod": payments,
                "PaymentAmount": float(document_totals.get('GrossTotal', 0)),
                "TaxType": tax_data.get('TaxType', ''),
                "CustomerData":customer_data,
                "NCReason": nc_reason_data,  # JSON com primeira referência e motivo: {"fatura_ref": "...", "reason": "..."}
                "Lines": []
            }
            
            # Processar linhas da fatura
            if 'Line' in invoice:
                lines = invoice['Line'] if isinstance(invoice['Line'], list) else [invoice['Line']]
                for line in lines:
                    line_tax = line.get('Tax', {})
                    
                    # Extrair References e Reason das linhas (para notas de crédito)
                    # Apenas a primeira referência e motivo encontrados serão salvos
                    if 'References' in line and nc_reason_data is None:
                        references_data = line.get('References', {})
                        reason_value = ""
                        first_reference = ""
                        
                        # References pode ser dict ou lista
                        if isinstance(references_data, dict):
                            # Extrair Reason
                            if 'Reason' in references_data:
                                reason_value = references_data.get('Reason', '')
                                # Reason pode ser string direta ou dict com #text
                                if isinstance(reason_value, dict) and '#text' in reason_value:
                                    reason_value = reason_value['#text']
                                if reason_value and isinstance(reason_value, str):
                                    reason_value = reason_value.strip()
                            
                            # Extrair primeira Reference
                            if 'Reference' in references_data:
                                ref = references_data.get('Reference', '')
                                # Reference pode ser lista, string ou dict
                                if isinstance(ref, list) and len(ref) > 0:
                                    # Pegar apenas o primeiro
                                    r = ref[0]
                                    if isinstance(r, str):
                                        first_reference = r.strip()
                                    elif isinstance(r, dict) and '#text' in r:
                                        first_reference = r['#text'].strip()
                                elif isinstance(ref, str):
                                    first_reference = ref.strip()
                                elif isinstance(ref, dict) and '#text' in ref:
                                    first_reference = ref['#text'].strip()
                        
                        elif isinstance(references_data, list) and len(references_data) > 0:
                            # Se References é uma lista, pegar apenas o primeiro item
                            ref_item = references_data[0]
                            if isinstance(ref_item, dict):
                                # Extrair Reason
                                if 'Reason' in ref_item:
                                    reason_value = ref_item.get('Reason', '')
                                    if isinstance(reason_value, dict) and '#text' in reason_value:
                                        reason_value = reason_value['#text']
                                    if reason_value and isinstance(reason_value, str):
                                        reason_value = reason_value.strip()
                                
                                # Extrair Reference
                                if 'Reference' in ref_item:
                                    ref = ref_item.get('Reference', '')
                                    if isinstance(ref, str):
                                        first_reference = ref.strip()
                                    elif isinstance(ref, dict) and '#text' in ref:
                                        first_reference = ref['#text'].strip()
                        
                        # Salvar apenas a primeira referência encontrada
                        if first_reference:
                            nc_reason_data = {
                                "fatura_ref": first_reference,
                                "reason": reason_value if reason_value else ""
                            }
                            logger.info(f"📝 Primeira referência encontrada na linha {line.get('LineNumber', 'N/A')}: {first_reference} - Motivo: {reason_value if reason_value else 'N/A'}")
                    
                    # Nas notas de crédito, o valor pode estar em DebitAmount ao invés de CreditAmount
                    # Verificar ambos os campos e usar o que existir e não for zero
                    credit_amount = line.get('CreditAmount', 0) or 0
                    debit_amount = line.get('DebitAmount', 0) or 0
                    
                    # Converter ambos para float para comparação
                    try:
                        credit_float = float(str(credit_amount).replace(",", ".")) if credit_amount else 0.0
                    except (ValueError, TypeError):
                        credit_float = 0.0
                    
                    try:
                        debit_float = float(str(debit_amount).replace(",", ".")) if debit_amount else 0.0
                    except (ValueError, TypeError):
                        debit_float = 0.0
                    
                    # Usar CreditAmount se existir e não for zero, senão usar DebitAmount
                    if credit_float != 0:
                        amount_float = credit_float
                    elif debit_float != 0:
                        amount_float = debit_float
                        logger.debug(f"💡 Usando DebitAmount ({debit_float}) ao invés de CreditAmount na linha {line.get('LineNumber', 'N/A')}")
                    else:
                        amount_float = 0.0
                        logger.warning(f"⚠️ Nenhum valor encontrado (CreditAmount ou DebitAmount) na linha {line.get('LineNumber', 'N/A')}")
                    
                    # Extrair UnitPrice
                    unit_price = line.get('UnitPrice', 0)
                    try:
                        unit_price_float = float(str(unit_price).replace(",", ".")) if unit_price else 0.0
                    except (ValueError, TypeError):
                        unit_price_float = 0.0
                    
                    # Extrair Quantity
                    quantity = line.get('Quantity', 0)
                    try:
                        quantity_float = float(str(quantity).replace(",", ".")) if quantity else 0.0
                    except (ValueError, TypeError):
                        quantity_float = 0.0
                    
                    # Extrair TaxPercentage
                    tax_percentage = line_tax.get('TaxPercentage', 0)
                    try:
                        tax_percentage_float = float(str(tax_percentage).replace(",", ".")) if tax_percentage else 0.0
                    except (ValueError, TypeError):
                        tax_percentage_float = 0.0
                    
                    fatura["Lines"].append({
                        "LineNumber": int(line.get('LineNumber', 0)),
                        "ProductCode": line.get('ProductCode', ''),
                        "Description": line.get('Description', ''),
                        "Quantity": quantity_float,
                        "UnitPrice": unit_price_float,
                        "CreditAmount": amount_float,  # Pode ser DebitAmount convertido
                        "TaxPercentage": tax_percentage_float,
                        # Calcular PriceWithIva com base no amount + TaxPercentage %
                        "PriceWithIva": (amount_float * (1 + tax_percentage_float / 100)),
                        "Iva": (amount_float * (tax_percentage_float / 100))
                    })
                
                # Atualizar NCReason na fatura após processar todas as linhas
                # Se não houver referências, deixar como None
                fatura["NCReason"] = nc_reason_data
            
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

def insert_filiais_batch(filiais_data):
    """Insere filiais em lote"""
    try:
        if not filiais_data:
            logger.warning("⚠️ Nenhuma filial para inserir")
            return None
        
        logger.info(f"🏪 Tentando inserir {len(filiais_data)} filiais...")
        
        # Usar upsert para evitar duplicatas baseado no filial_number que é único
        response = supabase.table("filiais").upsert(
            filiais_data,
            on_conflict="filial_number"
        ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} filiais inseridas/atualizadas")
        else:
            logger.warning("⚠️ Nenhuma filial foi inserida")
            
        return response
        
    except Exception as e:
        logger.error(f"❌ Erro ao inserir filiais em lote: {str(e)}")
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

def download_opengcs_files_from_sftp():
    """Baixa arquivos OpenGCs do SFTP percorrendo pastas por NIF"""
    sftp, transport = connect_sftp()
    
    if sftp is None:
        logger.error("Não foi possível estabelecer conexão SFTP")
        return []
    
    try:
        # Pasta remota onde estão as pastas por NIF
        pasta_remota = '/home/mydreami/myDream'
        
        # Pasta local onde os arquivos serão salvos
        pasta_local = './downloads'
        os.makedirs(pasta_local, exist_ok=True)

        downloaded_files = []
        file_mappings = []  # Armazenar mapeamento arquivo local -> remoto
        
        # Listar todas as pastas (NIFs das empresas)
        try:
            pastas_nif = sftp.listdir(pasta_remota)
            logger.info(f"Pastas NIF encontradas: {pastas_nif}")
        except Exception as e:
            logger.error(f"Erro ao listar pastas NIF: {str(e)}")
            return []
        
        # Percorrer cada pasta NIF
        for pasta_nif in pastas_nif:
            try:
                caminho_pasta_nif = f'{pasta_remota}/{pasta_nif}'
                logger.info(f"🔄 Verificando pasta NIF: {pasta_nif}")
                
                # Listar arquivos na pasta NIF
                arquivos_pasta = sftp.listdir(caminho_pasta_nif)
                logger.info(f"📁 Arquivos na pasta {pasta_nif}: {arquivos_pasta}")
                
                # Baixar arquivos que começam com opengcs-{nif}
                for arquivo in arquivos_pasta:
                    if arquivo.startswith(f'opengcs-{pasta_nif}'):
                        caminho_remoto = f'{caminho_pasta_nif}/{arquivo}'
                        caminho_local = os.path.join(pasta_local, arquivo)
                        
                        logger.info(f'📥 Baixando {arquivo} da pasta {pasta_nif}...')
                        sftp.get(caminho_remoto, caminho_local)
                        downloaded_files.append(caminho_local)
                        
                        # Armazenar mapeamento para exclusão posterior
                        file_mappings.append({
                            'local_path': caminho_local,
                            'remote_path': caminho_remoto,
                            'filename': arquivo,
                            'nif_folder': pasta_nif
                        })
                        
            except Exception as e:
                logger.error(f"Erro ao processar pasta {pasta_nif}: {str(e)}")
                continue

        logger.info(f"✅ Download OpenGCs concluído! {len(downloaded_files)} arquivos baixados")
        
        # Salvar mapeamento em arquivo temporário para uso posterior
        import json
        mappings_file = os.path.join(pasta_local, 'opengcs_file_mappings.json')
        with open(mappings_file, 'w') as f:
            json.dump(file_mappings, f)
        
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Erro durante download OpenGCs: {str(e)}")
        return []
    finally:
        # Fechar conexão
        if sftp:
            sftp.close()
        if transport:
            transport.close()

def delete_opengcs_file_from_sftp(local_file_path):
    """Exclui arquivo OpenGCs do SFTP após processamento bem-sucedido"""
    try:
        # Carregar mapeamento de arquivos
        mappings_file = os.path.join('./downloads', 'opengcs_file_mappings.json')
        if not os.path.exists(mappings_file):
            logger.warning(f"⚠️ Arquivo de mapeamento OpenGCs não encontrado: {mappings_file}")
            return False
        
        with open(mappings_file, 'r') as f:
            file_mappings = json.load(f)
        
        # Encontrar mapeamento para o arquivo local
        file_mapping = None
        for mapping in file_mappings:
            if mapping['local_path'] == local_file_path:
                file_mapping = mapping
                break
        
        if not file_mapping:
            logger.warning(f"⚠️ Mapeamento não encontrado para: {local_file_path}")
            return False
        
        # Conectar ao SFTP
        sftp, transport = connect_sftp()
        if sftp is None:
            logger.error("❌ Não foi possível conectar ao SFTP para exclusão")
            return False
        
        try:
            # Excluir arquivo remoto
            remote_path = file_mapping['remote_path']
            logger.info(f"🗑️ Excluindo arquivo OpenGCs do SFTP: {file_mapping['filename']} da pasta {file_mapping['nif_folder']}")
            
            sftp.remove(remote_path)
            logger.info(f"✅ Arquivo OpenGCs excluído com sucesso do SFTP: {file_mapping['filename']}")
            
            # Remover mapeamento da lista
            file_mappings.remove(file_mapping)
            
            # Atualizar arquivo de mapeamento
            with open(mappings_file, 'w') as f:
                json.dump(file_mappings, f)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao excluir arquivo OpenGCs do SFTP: {str(e)}")
            return False
        finally:
            # Fechar conexão
            if sftp:
                sftp.close()
            if transport:
                transport.close()
                
    except Exception as e:
        logger.error(f"❌ Erro geral na exclusão SFTP OpenGCs: {str(e)}")
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

def process_and_insert_invoice_batch(file_path: Path):
    """Processa e insere fatura no Supabase usando inserção em lote"""
    try:
        logger.info(f"🔄 Iniciando inserção em lote: {file_path}")
        
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        logger.info(f"📊 Dados carregados: {data['total_faturas']} faturas")

        # Preparar dados para inserção em lote
        companies_batch = []
        filiais_batch = []
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

            # Preparar filial para lote (se filial existir)
            if fatura["Filial"]:
                filiais_batch.append({
                    "filial_id": fatura["Filial"],
                    "filial_number": fatura["Filial"],  # Campo único
                    "company_id": fatura["CompanyID"],
                    "nome": f"{fatura['CompanyName']}",
                    "endereco": fatura["AddressDetail"],
                    "cidade": fatura["City"],
                    "codigo_postal": fatura["PostalCode"],
                    "pais": fatura["Country"]
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
                "certificate_number": fatura["CertificateNumber"],
                "net_total": fatura["NetTotal"],
                "gross_total": fatura["GrossTotal"],
                "payment_methods":fatura["PaymentMethod"],
                "payment_amount": float(str(fatura["PaymentAmount"]).replace(",", ".")),
                "tax_type": fatura["TaxType"],
                "customer_data": fatura["CustomerData"],
                "nc_reason": fatura.get("NCReason"),  # JSON com primeira referência e motivo: {"fatura_ref": "...", "reason": "..."}
                "active": True  # Novas invoices são ativas por padrão
            })
            
            
            # Armazenar linhas por fatura (não inserir ainda)
            lines_by_invoice[fatura["InvoiceNo"]] = []
            for linha in fatura["Lines"]:
                
                lines_by_invoice[fatura["InvoiceNo"]].append({
                    "line_number": linha["LineNumber"],
                    "product_code": linha["ProductCode"],
                    "description": linha["Description"],
                    "quantity": linha["Quantity"],
                    "unit_price": round(linha["UnitPrice"],4),
                    "credit_amount": linha["CreditAmount"],
                    "tax_percentage": linha["TaxPercentage"],
                    "price_with_iva": float(str(linha["PriceWithIva"]).replace(",", ".")),
                    "iva":round(linha["Iva"],4)
                })
                
         
        # Inserir empresas em lote
        logger.info(f"🏢 Inserindo {len(companies_batch)} empresas...")
        insert_companies_batch(companies_batch)

        # Inserir filiais em lote
        if filiais_batch:
            logger.info(f"🏪 Inserindo {len(filiais_batch)} filiais...")
            insert_filiais_batch(filiais_batch)

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

def extract_references_from_nc_xml(xml_file_path: str) -> list:
    """Extrai referências de faturas de um arquivo NC (Nota de Crédito)"""
    try:
        logger.info(f"🔍 Extraindo referências do arquivo NC: {xml_file_path}")
        
        # Ler arquivo com codificação adequada
        xml_content = read_xml_file_with_encoding(xml_file_path, "NC XML")
        if xml_content is None:
            return []
        
        # Converter XML para dict
        xml_dict = xmltodict.parse(xml_content)
        
        references = []
        
        # Procurar por References no XML
        if 'AuditFile' in xml_dict:
            audit_file = xml_dict['AuditFile']
            
            if 'SourceDocuments' in audit_file:
                source_docs = audit_file['SourceDocuments']
                
                if 'SalesInvoices' in source_docs:
                    sales_invoices = source_docs['SalesInvoices']
                    
                    if 'Invoice' in sales_invoices:
                        invoices = sales_invoices['Invoice'] if isinstance(sales_invoices['Invoice'], list) else [sales_invoices['Invoice']]
                        
                        for invoice in invoices:
                            # Procurar por Lines em cada invoice
                            if 'Line' in invoice:
                                lines = invoice['Line'] if isinstance(invoice['Line'], list) else [invoice['Line']]
                                
                                for line in lines:
                                    # Procurar por References em cada Line
                                    if 'References' in line:
                                        refs = line['References']
                                        
                                        # References pode ser uma lista ou um dict
                                        if isinstance(refs, dict) and 'Reference' in refs:
                                            ref_list = refs['Reference'] if isinstance(refs['Reference'], list) else [refs['Reference']]
                                            for ref in ref_list:
                                                if isinstance(ref, str):
                                                    references.append(ref.strip())
                                                elif isinstance(ref, dict) and '#text' in ref:
                                                    references.append(ref['#text'].strip())
                                        elif isinstance(refs, list):
                                            for ref_item in refs:
                                                if isinstance(ref_item, str):
                                                    references.append(ref_item.strip())
                                                elif isinstance(ref_item, dict) and 'Reference' in ref_item:
                                                    ref = ref_item['Reference']
                                                    if isinstance(ref, str):
                                                        references.append(ref.strip())
                                                    elif isinstance(ref, dict) and '#text' in ref:
                                                        references.append(ref['#text'].strip())
                            
                            # Também procurar por References diretamente no invoice (para compatibilidade)
                            elif 'References' in invoice:
                                refs = invoice['References']
                                
                                # References pode ser uma lista ou um dict
                                if isinstance(refs, dict) and 'Reference' in refs:
                                    ref_list = refs['Reference'] if isinstance(refs['Reference'], list) else [refs['Reference']]
                                    for ref in ref_list:
                                        if isinstance(ref, str):
                                            references.append(ref.strip())
                                        elif isinstance(ref, dict) and '#text' in ref:
                                            references.append(ref['#text'].strip())
                                elif isinstance(refs, list):
                                    for ref_item in refs:
                                        if isinstance(ref_item, str):
                                            references.append(ref_item.strip())
                                        elif isinstance(ref_item, dict) and 'Reference' in ref_item:
                                            ref = ref_item['Reference']
                                            if isinstance(ref, str):
                                                references.append(ref.strip())
                                            elif isinstance(ref, dict) and '#text' in ref:
                                                references.append(ref['#text'].strip())
        
        logger.info(f"✅ {len(references)} referências encontradas: {references}")
        return references
        
    except Exception as e:
        logger.error(f"❌ Erro ao extrair referências do arquivo NC {xml_file_path}: {str(e)}")
        return []

def deactivate_invoice(invoice_no: str) -> bool:
    """Desativa uma fatura (marca active = false) ao invés de deletar"""
    try:
        logger.info(f"🔄 Iniciando desativação da fatura: {invoice_no}")
        
        # 1. Buscar a fatura pelo número
        invoice_response = supabase.table("invoices").select("id, active").eq("invoice_no", invoice_no).execute()
        
        if not invoice_response.data:
            logger.warning(f"⚠️ Fatura não encontrada: {invoice_no}")
            return False
        
        invoice_id = invoice_response.data[0]["id"]
        current_active = invoice_response.data[0].get("active", True)
        
        # Verificar se já está desativada
        if current_active is False:
            logger.info(f"ℹ️ Fatura {invoice_no} já está desativada")
            return True
        
        logger.info(f"📋 Fatura encontrada com ID: {invoice_id}, status atual: active={current_active}")
        
        # 2. Atualizar a fatura para active = false
        invoice_update = supabase.table("invoices").update({
            "active": False
        }).eq("id", invoice_id).execute()
        
        if invoice_update.data:
            logger.info(f"✅ Fatura {invoice_no} desativada com sucesso (active = false)")
            return True
        else:
            logger.error(f"❌ Falha ao desativar fatura {invoice_no}")
            return False
        
    except Exception as e:
        logger.error(f"❌ Erro ao desativar fatura {invoice_no}: {str(e)}")
        return False

def process_nc_file(xml_file_path: str) -> dict:
    """Processa arquivo NC (Nota de Crédito) e desativa faturas referenciadas (active = false)"""
    try:        
        # Extrair referências do arquivo NC
        references = extract_references_from_nc_xml(xml_file_path)
        
        if not references:
            logger.warning(f"⚠️ Nenhuma referência encontrada no arquivo NC: {xml_file_path}")
            return {
                "status": "warning",
                "message": "Nenhuma referência encontrada",
                "deactivated_invoices": [],
                "failed_deactivations": []
            }
        
        deactivated_invoices = []
        failed_deactivations = []
        
        # Processar cada referência
        for reference in references:
            logger.info(f"🔍 Processando referência: {reference}")
            
            # Extrair número da fatura da referência (ex: "FR 201803Y2025/239")
            # Padrão esperado: FR + espaços + números + Y + ano/número
            invoice_pattern = r'FR\s+\d+Y\d{4}/\d+'
            match = re.search(invoice_pattern, reference)
            
            if match:
                invoice_no = match.group(0)
                
                # Tentar desativar a fatura (marcar active = false)
                if deactivate_invoice(invoice_no):
                    deactivated_invoices.append(invoice_no)
                else:
                    failed_deactivations.append(invoice_no)
            else:
                logger.warning(f"⚠️ Padrão de fatura não reconhecido na referência: {reference}")
                failed_deactivations.append(reference)
        
        
        return {
            "status": "success",
            "message": f"NC processado: {len(deactivated_invoices)} faturas desativadas, {len(failed_deactivations)} falhas",
            "deactivated_invoices": deactivated_invoices,
            "failed_deactivations": failed_deactivations,
            "total_references": len(references)
        }
        
    except Exception as e:
        logger.error(f"❌ Erro ao processar arquivo NC {xml_file_path}: {str(e)}")
        return {
            "status": "error",
            "message": str(e),
            "deactivated_invoices": [],
            "failed_deactivations": []
        }

def parse_opengcs_xml_to_json(xml_file_path: str) -> Optional[dict]:
    """Converte arquivo XML OpenGCs para JSON"""
    try:
        logger.info(f"🔄 Processando XML OpenGCs: {xml_file_path}")
        
        # Ler arquivo com codificação adequada
        xml_content = read_xml_file_with_encoding(xml_file_path, "OpenGCs XML")
        if xml_content is None:
            return None
        
        # Converter XML para dict
        xml_dict = xmltodict.parse(xml_content)
        logger.info(f"✅ XML OpenGCs convertido para dict com sucesso")
        
        # Extrair dados do OpenGCs
        opengcs_data = {
            "arquivo_origem": os.path.basename(xml_file_path),
            "data_processamento": datetime.now(tz=pytz.timezone('Europe/Lisbon')).isoformat(),
            "opengcs_total": 0.0,
            "opengcs_count": 0,
            "gcs": []
        }
        
        # Processar dados do OpenGCs
        if 'OpenGCs' in xml_dict:
            opengcs_root = xml_dict['OpenGCs']
            logger.info(f"✅ OpenGCs encontrado no XML")
            
            # Extrair total e contagem
            opengcs_data["opengcs_total"] = float(opengcs_root.get('OpenGCsTotal', 0))
            opengcs_data["opengcs_count"] = int(opengcs_root.get('OpenGCs', 0) )-1
            
            # Extrair GCs (pode ser lista ou dict)
            if 'GC' in opengcs_root:
                gcs = opengcs_root['GC'] if isinstance(opengcs_root['GC'], list) else [opengcs_root['GC']]
                logger.info(f"✅ {len(gcs)} GCs encontrados")
                
                for gc in gcs:
                    gc_data = {
                        "number": int(gc.get('number', 0)),
                        "open_time": gc.get('OpenTime', ''),
                        "last_time": gc.get('LastTime', ''),
                        "guests": int(gc.get('guests', 0)),
                        "operator_no": int(gc.get('operatorNo', 0)),
                        "operator_name": gc.get('operatorName', ''),
                        "start_operator_no": int(gc.get('StartOperatorNo', 0)),
                        "start_operator_name": gc.get('StartOperatorName', ''),
                        "total": float(gc.get('total', 0))
                    }
                    opengcs_data["gcs"].append(gc_data)
            else:
                logger.warning(f"⚠️ Nenhum GC encontrado no XML")
        else:
            logger.warning(f"⚠️ OpenGCs não encontrado no XML")
            return None
        
        logger.info(f"✅ Processamento OpenGCs concluído: {opengcs_data['opengcs_count']} GCs extraídos")
        
        return opengcs_data
        
    except Exception as e:
        logger.error(f"Erro ao processar XML OpenGCs {xml_file_path}: {str(e)}")
        return None

def extract_nif_from_filename(filename: str) -> str:
    """Extrai o NIF do nome do arquivo opengcs-{nif}-{filial}"""
    try:
        # Padrão: opengcs-{nif}-{filial}
        if filename.startswith('opengcs-') and filename.endswith('.xml'):
            # 1. Remove a extensão .xml (ou qualquer outra extensão)
            nome_sem_extensao = filename.rsplit('.', 1)[0]
            
            # 2. Faz o split pelo "-" e pega a última parte (a filial sempre está depois do último "-")
            filial = nome_sem_extensao.split("-")[  1]
           
            return filial
        else:
            logger.warning(f"⚠️ Padrão de arquivo OpenGCs não reconhecido: {filename}")
            return ""
    except Exception as e:
        logger.error(f"❌ Erro ao extrair NIF de {filename}: {str(e)}")
        return ""

def extract_opengcs_filial_from_filename(filename: str) -> str:
    """Extrai a filial do nome do arquivo opengcs-{nif}-{filial}"""
    print(filename)
    
    try:
        # Padrão: opengcs-{nif}-{filial}
        if filename.startswith('opengcs-') and filename.endswith('.xml'):
            nome_sem_extensao = filename.rsplit('.', 1)[0]
            
       
            return  nome_sem_extensao[18:]
            
        else:
            logger.warning(f"⚠️ Padrão de arquivo OpenGCs não reconhecido: {filename}")
            return ""
    except Exception as e:
        logger.error(f"❌ Erro ao extrair filial OpenGCs de {filename}: {str(e)}")
        return ""

def insert_opengcs_to_supabase(opengcs_data: dict, xml_file_path: str) -> bool:
    """Insere dados OpenGCs no Supabase"""
    try:
        if not opengcs_data:
            logger.warning("⚠️ Nenhum dado OpenGCs para inserir")
            return False
        
        # Extrair NIF e filial do nome do arquivo
        filename = os.path.basename(xml_file_path)
        nif = extract_nif_from_filename(filename)
        filial = extract_opengcs_filial_from_filename(filename)
        

        if not nif:
            logger.error(f"❌ Não foi possível extrair NIF do arquivo: {filename}")
            return False
        
        logger.info(f"🏪 Inserindo dados OpenGCs para NIF: {nif}, filial: {filial}")
        
        # Verificar se já existe um registro com este NIF e filial
        existing_record = supabase.table("open_gcs_json").select("loja_id").eq("nif", nif).eq("filial", filial).execute()
        
        if existing_record.data:
            # Atualizar registro existente
            loja_id = existing_record.data[0]["loja_id"]
            logger.info(f"🔄 Atualizando registro existente com loja_id: {loja_id}")
            
            response = supabase.table("open_gcs_json").update({
                "data": opengcs_data,
                "updated_at": datetime.now(tz=pytz.timezone('Europe/Lisbon')).isoformat()
            }).eq("loja_id", loja_id).execute()
            
            if response.data:
                logger.info(f"✅ Dados OpenGCs atualizados para NIF: {nif}, filial: {filial}")
                return True
            else:
                logger.warning("⚠️ Falha ao atualizar dados OpenGCs")
                return False
        else:
            # Inserir novo registro
            logger.info(f"🆕 Inserindo novo registro para NIF: {nif}, filial: {filial}")
            
            # Gerar loja_id único (usar NIF + filial como identificador)
            loja_id = f"{nif}_{filial}" if filial else nif
            
            data_to_insert = {
                "loja_id": loja_id,
                "nif": nif,
                "filial": filial,
                "data": opengcs_data,
                "updated_at": datetime.now(tz=pytz.timezone('Europe/Lisbon')).isoformat()
            }
            
            response = supabase.table("open_gcs_json").insert(data_to_insert).execute()
            
            if response.data:
                logger.info(f"✅ Dados OpenGCs inseridos para NIF: {nif}, filial: {filial}")
                return True
            else:
                logger.warning("⚠️ Nenhum dado OpenGCs foi inserido")
                return False
            
    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados OpenGCs: {str(e)}")
        return False
########
def file_existis(xml_path):

    if not os.path.exists(xml_path):
        return {"status": "error", "file": xml_path, "message": "Arquivo não encontrado"}

def invoice_fr_or_nc(filename):
    
    return filename[0:2]
########

#@celery_app.task
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
                # Salvar JSON processado
                pasta_dados_processados = './dados_processados'
                os.makedirs(pasta_dados_processados, exist_ok=True)
                
                json_filename = Path(xml_file_path).stem + '.json'
                json_path = os.path.join(pasta_dados_processados, json_filename)
                
                with open(json_path, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                # Processar e inserir no Supabase usando inserção em lote
                insertion_success = process_and_insert_invoice_batch(Path(json_path))
                
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
                    remove_file_safely(json_path, "Arquivo JSON")
                    
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
    

from celery import chain
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
