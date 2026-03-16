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
            if isinstance(payments, list):
                payment = [x for x in payments]
            
            # Extrair dados do Line (pode ser lista ou dict)
            line_data = invoice.get('Line', {})
            if isinstance(line_data, list):
                line_data = line_data[0] if line_data else {}
            tax_data = line_data.get('Tax', {}) if line_data else {}
            
            # Extrair filial do nome do arquivo
            filename = os.path.basename(xml_file_path)
            filial = extract_filial_from_filename(filename)
            
            # Inicializar variáveis para armazenar primeira referência e motivo (para notas de crédito)
            nc_reason_data = None  # Será um dict: {"fatura_ref": "...", "reason": "..."}
            
            # Tratar Hash que pode ser None ou string vazia
            hash_value = invoice.get('Hash') or ''
            if hash_value and isinstance(hash_value, str) and len(hash_value) > 30:
                hash_extract = hash_value[0] + hash_value[10] + hash_value[20] + hash_value[30]
            else:
                hash_extract = hash_value if (hash_value and isinstance(hash_value, str)) else ''
            
            fatura = {
                "CompanyID": company_data["CompanyID"],
                "CompanyName": company_data["CompanyName"],
                "AddressDetail": company_data["AddressDetail"],
                "City": company_data["City"],
                "PostalCode": company_data["PostalCode"],
                "Country": company_data["Country"],
                "CertificateNumber": company_data["SoftwareCertificateNumber"],
                "ProductCompanyTaxID": company_data["ProductCompanyTaxID"],  # Novo campo para DE
                "InvoiceNo": invoice.get('InvoiceNo', ''),
                "Filial": filial,  # Novo campo filial
                "ATCUD": invoice.get('ATCUD', ''),
                "CustomerID": invoice.get('CustomerID', ''),
                "InvoiceDate": invoice.get('InvoiceDate', ''),
                "InvoiceStatusDate_Date": invoice_status_date.split('T')[0] if invoice_status_date and 'T' in invoice_status_date else '',
                "InvoiceStatusDate_Time": invoice_status_date.split('T')[1] if invoice_status_date and 'T' in invoice_status_date else '',
                "HashExtract": hash_extract,
                "EndDate": invoice.get('EndDate', ''),
                "TaxPayable": float(document_totals.get('TaxPayable', 0)),
                "NetTotal": float(document_totals.get('NetTotal', 0)),
                "GrossTotal": float(document_totals.get('GrossTotal', 0)),
                "PaymentMethod": payments,
                "PaymentAmount": float(document_totals.get('GrossTotal', 0)),
                "TaxType": tax_data.get('TaxType', '') if tax_data else '',
                "CustomerData": customer_data,
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
