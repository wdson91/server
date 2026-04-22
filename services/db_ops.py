import os
import json
import logging
import re
from pathlib import Path
from datetime import datetime
import pytz
from sqlalchemy import text, insert, table, column, JSON
from sqlalchemy.dialects.postgresql import insert as pg_insert
from utils.db import engine, Session

from utils.xml_parser import (
    extract_references_from_nc_xml,
    extract_nif_from_filename,
    extract_opengcs_filial_from_filename
)

logger = logging.getLogger(__name__)

def insert_companies_batch(companies_data):
    """Insere empresas em lote usando SQLAlchemy ON CONFLICT"""
    if not companies_data:
        return None
    
    try:
        session = Session()
        stmt = pg_insert(text("companies"))
        
        # O Prisma define company_id como chave primária
        tbl = table("companies", column("company_id"), column("nome"), column("nif"), column("morada"), column("cp"), column("cidade"))
        for company in companies_data:
            insert_stmt = pg_insert(tbl).values(**company)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['company_id'],
                set_={k: v for k, v in company.items() if k != 'company_id'}
            )
            session.execute(upsert_stmt)
        
        session.commit()
        logger.info(f"✅ {len(companies_data)} empresas inseridas/atualizadas")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao inserir empresas em lote: {str(e)}")
        if 'session' in locals(): session.rollback()
        return None
    finally:
        if 'session' in locals(): Session.remove()

def insert_filiais_batch(filiais_data):
    """Insere filiais em lote usando SQLAlchemy ON CONFLICT"""
    if not filiais_data:
        return None
    
    try:
        session = Session()
        for filial in filiais_data:
            # Garantir campos obrigatórios
            if not filial.get("nome"):
                filial["nome"] = "Filial sem nome"
            
            tbl = table("filiais", column("filial_number"), column("nome"), column("company_id"))
            insert_stmt = pg_insert(tbl).values(**filial)
            # O Prisma define filial_number como único
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['filial_number'],
                set_={k: v for k, v in filial.items() if k != 'filial_number'}
            )
            session.execute(upsert_stmt)
            
        session.commit()
        logger.info(f"✅ {len(filiais_data)} filiais inseridas/atualizadas")
        return True
    except Exception as e:
        logger.error(f"❌ Erro ao inserir filiais em lote: {str(e)}")
        if 'session' in locals(): session.rollback()
        return None
    finally:
        if 'session' in locals(): Session.remove()

def insert_invoices_batch(invoices_data):
    """Insere faturas em lote usando SQLAlchemy ON CONFLICT"""
    if not invoices_data:
        return None
    
    try:
        session = Session()
        results = []
        for invoice in invoices_data:
            tbl = table("invoices", 
                        column("invoice_no"), column("company_id"), column("filial_number"),
                        column("invoice_date"), column("customer_id"), column("tax_payable"),
                        column("net_total"), column("gross_total"), column("active"),
                        column("language"))
            insert_stmt = pg_insert(tbl).values(**invoice)
            # O Prisma define @@unique([invoice_no, company_id])
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=['invoice_no', 'company_id'],
                set_={k: v for k, v in invoice.items() if k not in ['invoice_no', 'company_id']}
            ).returning(text("id"), text("invoice_no"), text("company_id"))
            
            res = session.execute(upsert_stmt).fetchone()
            if res:
                results.append({"id": str(res[0]), "invoice_no": res[1], "company_id": res[2]})
        
        session.commit()
        logger.info(f"✅ {len(invoices_data)} faturas inseridas/atualizadas")
        return results # Retornamos os IDs para mapeamento de linhas
    except Exception as e:
        logger.error(f"❌ Erro ao inserir faturas em lote: {str(e)}")
        if 'session' in locals(): session.rollback()
        return None
    finally:
        if 'session' in locals(): Session.remove()

def insert_invoice_lines_batch(lines_data):
    """Insere linhas de faturas em lote"""
    if not lines_data:
        return
    
    try:
        session = Session()
        session.execute(text("INSERT INTO invoice_lines (invoice_id, line_number, product_code, description, quantity, unit_price, credit_amount, tax_percentage, price_with_iva, iva) VALUES (:invoice_id, :line_number, :product_code, :description, :quantity, :unit_price, :credit_amount, :tax_percentage, :price_with_iva, :iva)"), lines_data)
        session.commit()
        logger.info(f"✅ {len(lines_data)} linhas de faturas processadas")
    except Exception as e:
        logger.error(f"❌ Erro ao inserir linhas em lote: {str(e)}")
        if 'session' in locals(): session.rollback()
    finally:
        if 'session' in locals(): Session.remove()

def insert_file_links_batch(links_data):
    """Insere links de arquivos em lote"""
    if not links_data:
        return
    
    try:
        session = Session()
        session.execute(text("INSERT INTO invoice_file_links (invoice_file_id, invoice_id) VALUES (:invoice_file_id, :invoice_id)"), links_data)
        session.commit()
        logger.info(f"✅ {len(links_data)} links de arquivos processados")
    except Exception as e:
        logger.error(f"❌ Erro ao inserir links em lote: {str(e)}")
        if 'session' in locals(): session.rollback()
    finally:
        if 'session' in locals(): Session.remove()

def process_and_insert_invoice_batch(data: dict):
    """Processa e insere fatura no banco usando transação SQLAlchemy"""
    session = Session()
    try:
        logger.info(f"🔄 Iniciando inserção em lote na DB (PostgreSQL)")
        
        companies_batch = data.get("companies_batch", [])
        filiais_batch = data.get("filiais_batch", [])
        invoices_batch = data.get("invoices_batch", [])
        lines_by_invoice = data.get("lines_by_invoice", {})
        
        # 1. Inserir Empresas
        if companies_batch:
            insert_companies_batch(companies_batch)

        # 2. Inserir Filiais
        if filiais_batch:
            insert_filiais_batch(filiais_batch)

        # 3. Inserir Faturas e obter IDs (para associar linhas e arquivos)
        invoices_results = insert_invoices_batch(invoices_batch)
        
        if invoices_results:
            # 4. Inserir ou buscar arquivo
            existing_file = session.execute(text("SELECT id FROM invoice_files WHERE filename = :filename"), {"filename": data["arquivo_origem"]}).fetchone()
            
            if existing_file:
                file_id = str(existing_file[0])
                logger.info(f"ℹ️ Arquivo já existe com ID: {file_id}")
            else:
                file_insert = session.execute(text("INSERT INTO invoice_files (filename, data_processamento, total_faturas) VALUES (:filename, :data_processamento, :total_faturas) RETURNING id"), 
                                             {"filename": data["arquivo_origem"], "data_processamento": data["data_processamento"], "total_faturas": data["total_faturas"]}).fetchone()
                file_id = str(file_insert[0])
                logger.info(f"✅ Arquivo inserido com ID: {file_id}")

            # 5. Mapear IDs das faturas para inserir linhas
            invoice_mapping = {f"{res['invoice_no']}_{res['company_id']}": res['id'] for res in invoices_results}
            
            lines_batch = []
            links_batch = []
            
            for fatura_obj in invoices_batch:
                inv_no = fatura_obj["invoice_no"]
                comp_id = fatura_obj.get("company_id", "")
                comp_key = f"{inv_no}_{comp_id}"
                invoice_id = invoice_mapping.get(comp_key)
                
                if invoice_id:
                    # Apagar linhas antigas (como no original)
                    session.execute(text("DELETE FROM invoice_lines WHERE invoice_id = :id"), {"id": invoice_id})
                    
                    if inv_no in lines_by_invoice:
                        for linha in lines_by_invoice[inv_no]:
                            linha_with_id = linha.copy()
                            linha_with_id["invoice_id"] = invoice_id
                            lines_batch.append(linha_with_id)

                    # Verificar se link já existe
                    existing_link = session.execute(text("SELECT id FROM invoice_file_links WHERE invoice_id = :inv_id AND invoice_file_id = :file_id"), 
                                                   {"inv_id": invoice_id, "file_id": file_id}).fetchone()
                    if not existing_link:
                        links_batch.append({"invoice_file_id": file_id, "invoice_id": invoice_id})

            if lines_batch:
                insert_invoice_lines_batch(lines_batch)

            if links_batch:
                insert_file_links_batch(links_batch)
            
            session.commit()
            logger.info(f"✅ Processamento DB concluído com sucesso")
            return True
        else:
            logger.error("❌ Falha ao inserir faturas")
            return False
                
    except Exception as e:
        logger.error(f"❌ Erro ao inserir dados no banco: {str(e)}")
        session.rollback()
        return False
    finally:
        Session.remove()

def deactivate_invoice(invoice_no: str) -> bool:
    """Desativa uma fatura (active = false)"""
    try:
        session = Session()
        result = session.execute(text("UPDATE invoices SET active = false WHERE invoice_no = :invoice_no"), {"invoice_no": invoice_no})
        session.commit()
        if result.rowcount > 0:
            logger.info(f"✅ Fatura {invoice_no} desativada")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Erro ao desativar fatura {invoice_no}: {str(e)}")
        return False
    finally:
        Session.remove()

def process_nc_file(xml_file_path: str) -> dict:
    """Processa arquivo NC e desativa faturas referenciadas"""
    try:        
        references = extract_references_from_nc_xml(xml_file_path)
        if not references:
            return {"status": "warning", "message": "Nenhuma referência encontrada", "deactivated_invoices": [], "failed_deactivations": []}
        
        deactivated = []
        failed = []
        for reference in references:
            match = re.search(r'FR\s+\d+Y\d{4}/\d+', reference)
            if match:
                inv_no = match.group(0)
                if deactivate_invoice(inv_no):
                    deactivated.append(inv_no)
                else:
                    failed.append(inv_no)
        
        return {"status": "success", "deactivated_invoices": deactivated, "failed_deactivations": failed}
    except Exception as e:
        logger.error(f"❌ Erro NC: {str(e)}")
        return {"status": "error", "message": str(e)}

def insert_opengcs_to_db(opengcs_data: dict, xml_file_path: str) -> bool:
    """Insere dados OpenGCs no PostgreSQL"""
    try:
        filename = os.path.basename(xml_file_path)
        nif = extract_nif_from_filename(filename)
        filial = extract_opengcs_filial_from_filename(filename)
        
        if not nif:
            return False
        
        loja_id = f"{nif}_{filial}" if filial else nif
        session = Session()
        
        tbl = table("open_gcs_json", column("loja_id"), column("nif"), column("filial"), column("data", JSON), column("updated_at"))
        insert_stmt = pg_insert(tbl).values(
            loja_id=loja_id,
            nif=nif,
            filial=filial,
            data=opengcs_data,
            updated_at=datetime.now(tz=pytz.timezone('Europe/Lisbon'))
        )
        
        upsert_stmt = insert_stmt.on_conflict_do_update(
            index_elements=['loja_id'],
            set_={'data': opengcs_data, 'updated_at': datetime.now(tz=pytz.timezone('Europe/Lisbon'))}
        )
        
        session.execute(upsert_stmt)
        session.commit()
        logger.info(f"✅ Dados OpenGCs inseridos: {loja_id}")
        return True
    except Exception as e:
        logger.error(f"❌ Erro OpenGCs: {str(e)}")
        return False
    finally:
        Session.remove()
