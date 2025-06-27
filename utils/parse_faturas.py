import re

def inserir_qrcode_apos_atcud(texto, qrcode_texto):
    # Regex que pega a linha inteira que começa com "ATCUD:" (com qualquer coisa depois)
    pattern = re.compile(r"^(ATCUD:\s*\S.*)$", flags=re.MULTILINE)

    # Função para substituir a linha do ATCUD pela mesma linha + QR code na linha seguinte
    def replacer(match):
        linha_atcud = match.group(1)
        return f"{linha_atcud}\n{qrcode_texto}"

    texto_modificado, n = pattern.subn(replacer, texto)
    if n == 0:
        # Se não encontrou ATCUD, insere no final do texto (opcional)
        texto_modificado = texto + "\n" + qrcode_texto
    return texto_modificado

def gerar_qrcode_texto(
    nif_emitente, nif_cliente, tipo_doc, autofaturado, data_doc, numero_doc,
    pais, iva_total, outros_impostos, retencao, total_impostos, estado, certificado
):
    return (
        f"A:{nif_emitente}*"
        f"B:{nif_cliente}*"
        f"C:Desconhecido*"
        f"D:{tipo_doc}*"
        f"E:{autofaturado}*"
        f"F:{data_doc}*"
        f"G:{numero_doc}*"
        f"H:1-1*"
        f"I1:{pais}*"
        f"I5:{iva_total:.2f}*"
        f"I6:{retencao:.2f}*"
        f"N:{outros_impostos:.2f}*"
        f"O:{total_impostos:.2f}*"
        f"Q:{estado}*"
        f"R:{certificado}"
    )

import re
from datetime import datetime

def parse_faturas(text):
    # Normaliza quebras de linha
    text = re.sub(r'\n+', '\n', text.strip())
    
    faturas = []

    # Regex principal corrigido
    fatura_pattern = re.compile(
        r'(?:.*?N\.I\.F\.\s*(?P<nif_emitente>\d{9}))?.*?'
        r'Fatura-Recibo nº\s+(?P<numero_fatura>[^\n]+).*?'
        r'Data:\s*(?P<data>\d{2}/\d{2}/\d{2})\s+(?P<hora>\d{2}:\d{2}).*?'
        r'(?:Mesa:\s*(?P<mesa>\d+))?.*?'
        r'\*+\s*N\.I\.F\.\s*:\s*(?P<nif_cliente>\d{9}|Consumidor final)\s*\*+.*?'
        r'(?P<itens>(?:\d+\s*x\s*[^@]+@\s*[\d,]+\s*(?:\d+%)?\s*[\d,]+\s*\n?)+).*?'
        r'Total\s+(?P<total>[\d]+(?:,[\d]{1,2})?)',
        re.DOTALL
    )

    matches = fatura_pattern.finditer(text)

    for match in matches:
        try:
            fatura = {
                "nif_emitente": match.group('nif_emitente') or "000000000",
                "numero_fatura": match.group('numero_fatura').strip(),
                "data": datetime.strptime(match.group('data'), '%d/%m/%y').strftime('%Y-%m-%d'),
                "hora": match.group('hora'),
                "mesa": match.group('mesa') or None,
                "nif_cliente": match.group('nif_cliente') or "999999990",
                "total": match.group('total').replace(',', '.'),
                "texto_original": match.group(0),
                "texto_completo": match.group(0),
                "qrcode": f"FAKE-QR-{match.group('numero_fatura').strip()}",
                "itens": []
            }

            # Extrai itens
            itens_text = match.group('itens')
            item_pattern = re.compile(
                r'(\d+)\s*x\s*([^@]+)@\s*([\d,]+)\s*(?:(\d+)%\s*)?([\d,]+)?'
            )
            for item in item_pattern.finditer(itens_text):
                qtd = int(item.group(1))
                nome = item.group(2).strip()
                preco = float(item.group(3).replace(',', '.'))
                iva = item.group(4) if item.group(4) else "0"
                total_item = item.group(5).replace(',', '.') if item.group(5) else f"{preco * qtd:.2f}"
                fatura["itens"].append({
                    "nome": nome,
                    "quantidade": qtd,
                    "preco_unitario": f"{preco:.2f}",
                    "iva": iva,
                    "total": total_item
                })

            faturas.append(fatura)
        except Exception as e:
            print(f"Erro ao processar fatura: {e}")
            continue

    return faturas
