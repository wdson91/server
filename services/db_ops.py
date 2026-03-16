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

def insert_companies_batch(companies_data):
    """Insere empresas em lote"""
    try:
        if not companies_data:
            logger.warning("⚠️ Nenhuma empresa para inserir")
            return None
        
        logger.info(f"🏢 Tentando inserir {len(companies_data)} empresas...")
        
        if len(companies_data) == 1:
            try:
                response = supabase.table("companies").upsert(
                    companies_data[0],
                    on_conflict="company_id"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir empresa individual: {str(e)}")
                raise
        else:
            response = supabase.table("companies").upsert(
                companies_data,
                on_conflict="company_id"
            ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} empresas inseridas/atualizadas")
            
        return response
    except Exception as e:
        logger.error(f"❌ Erro ao inserir empresas em lote: {str(e)}")
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
        
        if len(filiais_data) == 1:
            try:
                response = supabase.table("filiais").upsert(
                    filiais_data[0],
                    on_conflict="filial_number"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir filial individual: {str(e)}")
                raise
        else:
            response = supabase.table("filiais").upsert(
                filiais_data,
                on_conflict="filial_number"
            ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} filiais inseridas/atualizadas")
            
        return response
    except Exception as e:
        logger.error(f"❌ Erro ao inserir filiais em lote: {str(e)}")
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
        
        if len(invoices_data) == 1:
            try:
                response = supabase.table("invoices").upsert(
                    invoices_data[0],
                    on_conflict="invoice_no"
                ).execute()
            except Exception as e:
                logger.error(f"❌ Erro ao inserir fatura individual: {str(e)}")
                raise
        else:
            response = supabase.table("invoices").upsert(
                invoices_data,
                on_conflict="invoice_no"
            ).execute()
        
        if response.data:
            logger.info(f"✅ {len(response.data)} faturas inseridas/atualizadas")
            
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
def process_and_insert_invoice_batch(data: dict):
    """Processa e insere fatura no Supabase usando inserção em lote da memória"""
    try:
        logger.info(f"🔄 Iniciando inserção em lote na DB")
        logger.info(f"📊 Dados recebidos: {data['total_faturas']} faturas")

        companies_batch = data.get("companies_batch", [])
        filiais_batch = data.get("filiais_batch", [])
        invoices_batch = data.get("invoices_batch", [])
        lines_by_invoice = data.get("lines_by_invoice", {})
        links_batch = []
        
        if companies_batch:
            logger.info(f"🏢 Inserindo {len(companies_batch)} empresas...")
            insert_companies_batch(companies_batch)

        if filiais_batch:
            logger.info(f"🏪 Inserindo {len(filiais_batch)} filiais...")
            insert_filiais_batch(filiais_batch)

        logger.info(f"📄 Inserindo {len(invoices_batch)} faturas...")
        invoices_response = insert_invoices_batch(invoices_batch)
        
        if invoices_response and invoices_response.data:
            existing_file = supabase.table("invoice_files").select("id").eq("filename", data["arquivo_origem"]).execute()
            
            if existing_file.data:
                file_id = existing_file.data[0]["id"]
                logger.info(f"ℹ️ Arquivo já existe com ID: {file_id}, reutilizando")
            else:
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

            invoice_mapping = {}
            for invoice in invoices_response.data:
                invoice_mapping[invoice["invoice_no"]] = invoice["id"]

            lines_batch = []
            
            for fatura_obj in invoices_batch:
                inv_no = fatura_obj["invoice_no"]
                invoice_id = invoice_mapping.get(inv_no)
                
                if invoice_id:
                    existing_lines = supabase.table("invoice_lines").select("line_number").eq("invoice_id", invoice_id).execute()
                    existing_line_numbers = {line["line_number"] for line in existing_lines.data} if existing_lines.data else set()
                    
                    if inv_no in lines_by_invoice:
                        for linha in lines_by_invoice[inv_no]:
                            if linha["line_number"] not in existing_line_numbers:
                                linha_with_invoice_id = linha.copy()
                                linha_with_invoice_id["invoice_id"] = invoice_id
                                lines_batch.append(linha_with_invoice_id)

                    existing_link = supabase.table("invoice_file_links").select("id").eq("invoice_id", invoice_id).eq("invoice_file_id", file_id).execute()
                    if not existing_link.data:
                        links_batch.append({
                            "invoice_file_id": file_id,
                            "invoice_id": invoice_id
                        })
                else:
                    logger.warning(f"⚠️ Fatura {inv_no} não foi inserida, linhas ignoradas")

            if lines_batch:
                logger.info(f"📋 Inserindo {len(lines_batch)} linhas de faturas...")
                insert_invoice_lines_batch(lines_batch)

            if links_batch:
                logger.info(f"🔗 Inserindo {len(links_batch)} links de arquivos...")
                insert_file_links_batch(links_batch)
        else:
            logger.error("❌ Falha ao inserir faturas, arquivo e linhas não serão inseridas")
            return False

        logger.info(f"✅ Processamento de memória DB inserido com sucesso")
        return True
                
    except Exception as e:
        logger.error(f"Erro ao inserir dados no banco: {str(e)}")
        import traceback
        traceback.print_exc()
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
