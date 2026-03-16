import os
import json
import logging
import re
from pathlib import Path
from datetime import datetime
import pytz
from dotenv import load_dotenv
from supabase import create_client, Client

from utils.xml_parser import extract_references_from_nc_xml

logger = logging.getLogger(__name__)
load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

if not SUPABASE_URL or not SUPABASE_KEY:
    raise ValueError("SUPABASE_URL e SUPABASE_KEY devem estar definidos no .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def clean_data_for_insertion(data_dict):
    """Limpa dados para inserção no banco, removendo None e convertendo tipos"""
    cleaned = {}
    for key, value in data_dict.items():
        if value is None:
            # Manter None para campos opcionais (será convertido para NULL no banco)
            cleaned[key] = None
        elif isinstance(value, (dict, list)):
            # Serializar dict/list para JSON string se necessário
            try:
                # Tentar serializar para verificar se é válido
                json.dumps(value)
                cleaned[key] = value
            except (TypeError, ValueError):
                # Se não puder serializar, converter para string
                logger.warning(f"⚠️ Valor não serializável em {key}, convertendo para string")
                cleaned[key] = str(value)
        elif isinstance(value, float):
            # Garantir que não é NaN ou Inf
            if not (value != value or value == float('inf') or value == float('-inf')):
                cleaned[key] = value
            else:
                logger.warning(f"⚠️ Valor inválido (NaN/Inf) em {key}, usando 0")
                cleaned[key] = 0.0
        else:
            cleaned[key] = value
    return cleaned
def insert_companies_batch(companies_data):
    """Insere empresas em lote"""
    try:
        if not companies_data:
            logger.warning("⚠️ Nenhuma empresa para inserir")
            return None
        
        logger.info(f"🏢 Tentando inserir {len(companies_data)} empresas...")
        
        # Limpar dados antes de inserir
        cleaned_companies = []
        for company in companies_data:
            cleaned = clean_data_for_insertion(company)
            # Garantir que todos os campos são strings ou None
            for key, value in cleaned.items():
                if value is not None and not isinstance(value, str):
                    cleaned[key] = str(value)
            cleaned_companies.append(cleaned)
        
        # Log dos dados limpos para debug (apenas primeiro item)
        if cleaned_companies:
            try:
                logger.info(f"📋 Exemplo de dados de empresa limpos: {json.dumps(cleaned_companies[0], default=str, ensure_ascii=False)}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao logar dados: {str(e)}")
        
        # Tentar inserir um por um primeiro para identificar problemas
        if len(cleaned_companies) == 1:
            # Se for apenas um, tentar inserir diretamente
            try:
                response = supabase.table("companies").upsert(
                    cleaned_companies[0],
                    on_conflict="company_id"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir empresa individual: {str(e)}")
                logger.error(f"📋 Dados que causaram erro: {json.dumps(cleaned_companies[0], default=str, ensure_ascii=False)}")
                raise
        else:
            # Usar upsert para evitar duplicatas
            response = supabase.table("companies").upsert(
                cleaned_companies,
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
        
        # Limpar dados antes de inserir
        cleaned_filiais = []
        for filial in filiais_data:
            cleaned = clean_data_for_insertion(filial)
            # Garantir que nome não seja None (campo NOT NULL)
            if cleaned.get("nome") is None:
                cleaned["nome"] = ""
            # Garantir que todos os campos são strings ou None
            for key, value in cleaned.items():
                if value is not None and not isinstance(value, str):
                    cleaned[key] = str(value)
            cleaned_filiais.append(cleaned)
        
        # Log dos dados limpos para debug
        if cleaned_filiais:
            try:
                logger.info(f"📋 Exemplo de dados de filial limpos: {json.dumps(cleaned_filiais[0], default=str, ensure_ascii=False)}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao logar dados: {str(e)}")
        
        # Tentar inserir um por um primeiro para identificar problemas
        if len(cleaned_filiais) == 1:
            try:
                response = supabase.table("filiais").upsert(
                    cleaned_filiais[0],
                    on_conflict="filial_number"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir filial individual: {str(e)}")
                logger.error(f"📋 Dados que causaram erro: {json.dumps(cleaned_filiais[0], default=str, ensure_ascii=False)}")
                raise
        else:
            # Usar upsert para evitar duplicatas baseado no filial_number que é único
            response = supabase.table("filiais").upsert(
                cleaned_filiais,
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
        
        # Limpar dados antes de inserir
        cleaned_invoices = []
        for invoice in invoices_data:
            # Log dos dados originais para debug (apenas primeiro item)
            if len(cleaned_invoices) == 0:
                logger.debug(f"📋 Exemplo de dados de fatura originais: {json.dumps(invoice, default=str, ensure_ascii=False)[:500]}")
            cleaned = clean_data_for_insertion(invoice)
            
            # Tratamento especial para payment_methods - pode ser dict, list ou string
            if 'payment_methods' in cleaned and cleaned['payment_methods'] is not None:
                payment_methods = cleaned['payment_methods']
                if isinstance(payment_methods, (dict, list)):
                    # Tentar serializar como JSON
                    try:
                        json.dumps(payment_methods)
                        # Manter como está se puder serializar
                    except (TypeError, ValueError):
                        # Se não puder, converter para string
                        logger.warning(f"⚠️ payment_methods não serializável, convertendo para string")
                        cleaned['payment_methods'] = str(payment_methods)
                elif not isinstance(payment_methods, str):
                    # Converter outros tipos para string
                    cleaned['payment_methods'] = str(payment_methods)
            
            # Tratamento especial para customer_data - deve ser dict ou None
            if 'customer_data' in cleaned:
                if cleaned['customer_data'] is None:
                    cleaned['customer_data'] = None
                elif isinstance(cleaned['customer_data'], dict):
                    # Verificar se pode ser serializado
                    try:
                        json.dumps(cleaned['customer_data'])
                    except (TypeError, ValueError):
                        logger.warning(f"⚠️ customer_data não serializável, limpando valores problemáticos")
                        # Limpar valores problemáticos
                        cleaned['customer_data'] = {k: v for k, v in cleaned['customer_data'].items() 
                                                   if v is not None and isinstance(v, (str, int, float, bool))}
                else:
                    # Se não for dict, converter para dict vazio
                    logger.warning(f"⚠️ customer_data não é dict, usando dict vazio")
                    cleaned['customer_data'] = {}
            
            # Tratamento especial para nc_reason - deve ser dict ou None
            if 'nc_reason' in cleaned:
                if cleaned['nc_reason'] is None:
                    cleaned['nc_reason'] = None
                elif isinstance(cleaned['nc_reason'], dict):
                    try:
                        json.dumps(cleaned['nc_reason'])
                    except (TypeError, ValueError):
                        logger.warning(f"⚠️ nc_reason não serializável, limpando valores problemáticos")
                        cleaned['nc_reason'] = {k: v for k, v in cleaned['nc_reason'].items() 
                                               if v is not None and isinstance(v, (str, int, float, bool))}
                else:
                    logger.warning(f"⚠️ nc_reason não é dict, usando None")
                    cleaned['nc_reason'] = None
            
            cleaned_invoices.append(cleaned)
        
        # Log dos dados limpos para debug (apenas primeiro item)
        if cleaned_invoices:
            try:
                logger.info(f"📋 Exemplo de dados de fatura limpos: {json.dumps(cleaned_invoices[0], default=str, ensure_ascii=False)[:1000]}")
            except Exception as e:
                logger.warning(f"⚠️ Erro ao logar dados: {str(e)}")
        
        # Tentar inserir um por um primeiro para identificar problemas
        if len(cleaned_invoices) == 1:
            try:
                response = supabase.table("invoices").upsert(
                    cleaned_invoices[0],
                    on_conflict="invoice_no"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir fatura individual: {str(e)}")
                try:
                    logger.error(f"📋 Dados que causaram erro: {json.dumps(cleaned_invoices[0], default=str, ensure_ascii=False)[:2000]}")
                except:
                    logger.error(f"📋 Dados que causaram erro (não serializável)")
                raise
        else:
            # Usar upsert para evitar duplicatas
            response = supabase.table("invoices").upsert(
                cleaned_invoices,
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
            # Garantir que company_id não seja None ou vazio (campo obrigatório)
            company_id = fatura.get("CompanyID") or ""
            if not company_id:
                logger.error(f"❌ CompanyID vazio ou None na fatura {fatura.get('InvoiceNo', 'N/A')}")
                continue
                
            companies_batch.append({
                "company_id": company_id,
                "company_name": fatura.get("CompanyName") or "",
                "address_detail": fatura.get("AddressDetail") or "",
                "city": fatura.get("City") or "",
                "postal_code": fatura.get("PostalCode") or "",
                "country": fatura.get("Country") or ""
            })

            # Preparar filial para lote (se filial existir)
            filial = fatura.get("Filial") or ""
            if filial:
                filiais_batch.append({
                    "filial_id": filial,
                    "filial_number": filial,  # Campo único
                    "company_id": company_id,
                    "nome": fatura.get("CompanyName") or "",
                    "endereco": fatura.get("AddressDetail") or "",
                    "cidade": fatura.get("City") or "",
                    "codigo_postal": fatura.get("PostalCode") or "",
                    "pais": fatura.get("Country") or ""
                })
            
            # Preparar customer_data incluindo ProductCompanyTaxID se existir
            customer_data_with_tax = fatura.get("CustomerData", {}).copy() if fatura.get("CustomerData") else {}
            product_company_tax_id = fatura.get("ProductCompanyTaxID", "")
            if product_company_tax_id:
                customer_data_with_tax["ProductCompanyTaxID"] = product_company_tax_id
                logger.info(f"✅ ProductCompanyTaxID adicionado ao customer_data: {product_company_tax_id}")
            
            # Preparar fatura para lote
            # Garantir que invoice_no não seja None ou vazio (campo obrigatório)
            invoice_no = fatura.get("InvoiceNo")
            if invoice_no is None:
                invoice_no = ""
            else:
                invoice_no = str(invoice_no).strip()
            
            if not invoice_no:
                logger.error(f"❌ InvoiceNo vazio ou None na fatura")
                continue
            
            # Log do invoice_no para debug
            logger.info(f"📋 InvoiceNo: '{invoice_no}' (tipo: {type(invoice_no).__name__})")
            
            # Converter payment_methods - pode ser dict, list ou None
            payment_methods = fatura.get("PaymentMethod")
            if payment_methods is None:
                payment_methods = None
            elif isinstance(payment_methods, (dict, list)):
                # Manter como está, será validado na limpeza
                pass
            else:
                # Converter para string se não for dict/list
                payment_methods = str(payment_methods) if payment_methods else None
            
            invoices_batch.append({
                "invoice_no": invoice_no,
                "filial": filial,  # Novo campo filial
                "atcud": fatura.get("ATCUD") or None,
                "company_id": company_id,
                "customer_id": fatura.get("CustomerID") or None,
                "invoice_date": fatura.get("InvoiceDate") if fatura.get("InvoiceDate") else None,
                "invoice_status_date": fatura.get("InvoiceStatusDate_Date") if fatura.get("InvoiceStatusDate_Date") else None,
                "invoice_status_time": fatura.get("InvoiceStatusDate_Time") if fatura.get("InvoiceStatusDate_Time") else None,
                "hash_extract": fatura.get("HashExtract") or None,
                "end_date": fatura.get("EndDate") if fatura.get("EndDate") else None,
                "tax_payable": float(fatura.get("TaxPayable", 0) or 0),
                "certificate_number": fatura.get("CertificateNumber") or None,
                "net_total": float(fatura.get("NetTotal", 0) or 0),
                "gross_total": float(fatura.get("GrossTotal", 0) or 0),
                "payment_methods": payment_methods,
                "payment_amount": float(str(fatura.get("PaymentAmount", 0) or 0).replace(",", ".")),
                "tax_type": fatura.get("TaxType") or None,
                "customer_data": customer_data_with_tax if customer_data_with_tax else None,  # Inclui ProductCompanyTaxID se existir
                "nc_reason": fatura.get("NCReason") if fatura.get("NCReason") else None,  # JSON com primeira referência e motivo: {"fatura_ref": "...", "reason": "..."}
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
