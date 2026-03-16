import os
import re
import logging
from datetime import datetime
import pytz
from typing import Optional
import xmltodict

logger = logging.getLogger(__name__)

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
    """Converte arquivo XML para as estruturas de lote do Supabase"""
    try:
        logger.info(f"🔄 Processando XML: {xml_file_path}")
        
        # Ler arquivo com codificação adequada
        xml_content = read_xml_file_with_encoding(xml_file_path, "XML")
        if xml_content is None:
            return None
        
        # Converter XML para dict
        xml_dict = xmltodict.parse(xml_content)
        logger.info(f"✅ XML convertido para dict com sucesso")
        
        saft_data = {
            "arquivo_origem": os.path.basename(xml_file_path),
            "data_processamento": datetime.now(tz=pytz.timezone('Europe/Lisbon')).isoformat(),
            "total_faturas": 0,
            "companies_batch": [],
            "filiais_batch": [],
            "invoices_batch": [],
            "lines_by_invoice": {}
        }
        
        if 'AuditFile' in xml_dict:
            audit_file = xml_dict['AuditFile']
            logger.info(f"✅ AuditFile encontrado no XML")
        else:
            logger.warning(f"⚠️ AuditFile não encontrado no XML")
            return None
            
        company_data = {
            "CompanyID": "",
            "CompanyName": "",
            "AddressDetail": "",
            "City": "",
            "PostalCode": "",
            "Country": "",
            "SoftwareCertificateNumber": "",
            "ProductCompanyTaxID": ""
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
                "SoftwareCertificateNumber": header.get('SoftwareCertificateNumber', ''),
                "ProductCompanyTaxID": header.get('ProductCompanyTaxID', '')
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
            master_files = audit_file["MasterFiles"].get("Customer", {})
            if isinstance(master_files, list):
                master_files = master_files[0] if master_files else {}
            
            if master_files.get('CustomerID') != 999999990 and master_files.get('CustomerID'):
                customer_data = {
                    "CustomerID": master_files.get('CustomerID', 'Desconhecido'),
                    "AccountID": master_files.get('AccountID', 'Desconhecido'),
                    "CustomerTaxID": master_files.get('CustomerTaxID', 'Desconhecido'),
                    "CompanyName": master_files.get('CompanyName', 'Desconhecido'),
                    "PostalCode": master_files.get('BillingAddress', {}).get('PostalCode', 'Desconhecido'),
                    "AddressDetail": master_files.get('BillingAddress', {}).get('AddressDetail', 'Desconhecido'),
                    "City": master_files.get('BillingAddress', {}).get('City', 'Desconhecido'),
                }
                
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
        
        companies_seen = set()
        filiais_seen = set()
        
        for invoice in invoices:
            document_status = invoice.get('DocumentStatus', {})
            invoice_status_date = document_status.get('InvoiceStatusDate', '')
            
            document_totals = invoice.get('DocumentTotals', {})
            payments = document_totals.get('Payment', {})
            if isinstance(payments, list):
                payment = [x for x in payments]
            
            line_data = invoice.get('Line', {})
            if isinstance(line_data, list):
                line_data = line_data[0] if line_data else {}
            tax_data = line_data.get('Tax', {}) if line_data else {}
            
            filename = os.path.basename(xml_file_path)
            filial = extract_filial_from_filename(filename)
            
            nc_reason_data = None
            
            hash_value = invoice.get('Hash') or ''
            if hash_value and isinstance(hash_value, str) and len(hash_value) > 30:
                hash_extract = hash_value[0] + hash_value[10] + hash_value[20] + hash_value[30]
            else:
                hash_extract = hash_value if (hash_value and isinstance(hash_value, str)) else ''
                
            company_id = str(company_data.get("CompanyID") or "")
            if company_id and company_id not in companies_seen:
                saft_data["companies_batch"].append({
                    "company_id": company_id,
                    "company_name": str(company_data.get("CompanyName") or ""),
                    "address_detail": str(company_data.get("AddressDetail") or ""),
                    "city": str(company_data.get("City") or ""),
                    "postal_code": str(company_data.get("PostalCode") or ""),
                    "country": str(company_data.get("Country") or "")
                })
                companies_seen.add(company_id)
                
            if filial and filial not in filiais_seen:
                saft_data["filiais_batch"].append({
                    "filial_id": str(filial),
                    "filial_number": str(filial),
                    "company_id": company_id,
                    "nome": str(company_data.get("CompanyName") or ""),
                    "endereco": str(company_data.get("AddressDetail") or ""),
                    "cidade": str(company_data.get("City") or ""),
                    "codigo_postal": str(company_data.get("PostalCode") or ""),
                    "pais": str(company_data.get("Country") or "")
                })
                filiais_seen.add(filial)
                
            customer_data_with_tax = customer_data.copy() if customer_data else {}
            product_company_tax_id = company_data.get("ProductCompanyTaxID", "")
            if product_company_tax_id:
                customer_data_with_tax["ProductCompanyTaxID"] = str(product_company_tax_id)
                
            invoice_no = str(invoice.get('InvoiceNo', '')).strip()
            if not invoice_no:
                logger.error(f"❌ InvoiceNo vazio ou None na fatura")
                continue
                
            payment_methods = payments
            if payment_methods is None:
                payment_methods = None
            elif not isinstance(payment_methods, (dict, list)):
                payment_methods = str(payment_methods)
            
            lines = invoice.get('Line', [])
            if not isinstance(lines, list):
                lines = [lines]
                
            current_lines = []
            
            for line in lines:
                line_tax = line.get('Tax', {})
                if 'References' in line and nc_reason_data is None:
                    references_data = line.get('References', {})
                    reason_value = ""
                    first_reference = ""
                    
                    if isinstance(references_data, dict):
                        if 'Reason' in references_data:
                            reason_value = references_data.get('Reason', '')
                            if isinstance(reason_value, dict) and '#text' in reason_value:
                                reason_value = reason_value['#text']
                            if reason_value and isinstance(reason_value, str):
                                reason_value = reason_value.strip()
                        if 'Reference' in references_data:
                            ref = references_data.get('Reference', '')
                            if isinstance(ref, list) and len(ref) > 0:
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
                        ref_item = references_data[0]
                        if isinstance(ref_item, dict):
                            if 'Reason' in ref_item:
                                reason_value = ref_item.get('Reason', '')
                                if isinstance(reason_value, dict) and '#text' in reason_value:
                                    reason_value = reason_value['#text']
                                if reason_value and isinstance(reason_value, str):
                                    reason_value = reason_value.strip()
                            if 'Reference' in ref_item:
                                ref = ref_item.get('Reference', '')
                                if isinstance(ref, str):
                                    first_reference = ref.strip()
                                elif isinstance(ref, dict) and '#text' in ref:
                                    first_reference = ref['#text'].strip()
                    if first_reference:
                        nc_reason_data = {
                            "fatura_ref": first_reference,
                            "reason": reason_value if reason_value else ""
                        }
                        
                credit_amount = line.get('CreditAmount', 0) or 0
                debit_amount = line.get('DebitAmount', 0) or 0
                try:
                    credit_float = float(str(credit_amount).replace(",", ".")) if credit_amount else 0.0
                except (ValueError, TypeError):
                    credit_float = 0.0
                try:
                    debit_float = float(str(debit_amount).replace(",", ".")) if debit_amount else 0.0
                except (ValueError, TypeError):
                    debit_float = 0.0
                
                amount_float = credit_float if credit_float != 0 else debit_float
                
                unit_price = line.get('UnitPrice', 0)
                try:
                    unit_price_float = float(str(unit_price).replace(",", ".")) if unit_price else 0.0
                except (ValueError, TypeError):
                    unit_price_float = 0.0
                
                quantity = line.get('Quantity', 0)
                try:
                    quantity_float = float(str(quantity).replace(",", ".")) if quantity else 0.0
                except (ValueError, TypeError):
                    quantity_float = 0.0
                
                tax_percentage = line_tax.get('TaxPercentage', 0)
                try:
                    tax_percentage_float = float(str(tax_percentage).replace(",", ".")) if tax_percentage else 0.0
                except (ValueError, TypeError):
                    tax_percentage_float = 0.0
                
                current_lines.append({
                    "line_number": int(line.get('LineNumber', 0)),
                    "product_code": str(line.get('ProductCode', '')),
                    "description": str(line.get('Description', '')),
                    "quantity": quantity_float,
                    "unit_price": float(round(unit_price_float, 4)),
                    "credit_amount": amount_float,
                    "tax_percentage": tax_percentage_float,
                    "price_with_iva": float(str(amount_float * (1 + tax_percentage_float / 100)).replace(",", ".")),
                    "iva": float(round(amount_float * (tax_percentage_float / 100), 4))
                })
            
            saft_data["invoices_batch"].append({
                "invoice_no": invoice_no,
                "filial": filial,
                "atcud": invoice.get('ATCUD') or None,
                "company_id": company_id,
                "customer_id": invoice.get('CustomerID') or None,
                "invoice_date": invoice.get('InvoiceDate') if invoice.get('InvoiceDate') else None,
                "invoice_status_date": invoice_status_date.split('T')[0] if invoice_status_date and 'T' in invoice_status_date else None,
                "invoice_status_time": invoice_status_date.split('T')[1] if invoice_status_date and 'T' in invoice_status_date else None,
                "hash_extract": hash_extract or None,
                "end_date": invoice.get('EndDate') if invoice.get('EndDate') else None,
                "tax_payable": float(document_totals.get('TaxPayable', 0) or 0),
                "certificate_number": company_data.get("SoftwareCertificateNumber") or None,
                "net_total": float(document_totals.get('NetTotal', 0) or 0),
                "gross_total": float(document_totals.get('GrossTotal', 0) or 0),
                "payment_methods": payment_methods,
                "payment_amount": float(str(document_totals.get('GrossTotal', 0) or 0).replace(",", ".")),
                "tax_type": tax_data.get('TaxType') or None,
                "customer_data": customer_data_with_tax if customer_data_with_tax else None,
                "nc_reason": nc_reason_data,
                "active": True
            })
            saft_data["lines_by_invoice"][invoice_no] = current_lines
            
        saft_data["total_faturas"] = len(saft_data["invoices_batch"])
        logger.info(f"✅ Processamento concluído: {saft_data['total_faturas']} faturas mapeadas para DB")
       
        return saft_data
        
    except Exception as e:
        logger.error(f"Erro ao processar XML {xml_file_path}: {str(e)}")
        import traceback
        traceback.print_exc()
        return None
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
