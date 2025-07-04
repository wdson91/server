import re
from datetime import datetime
import re
import qrcode
from io import BytesIO
from PIL import Image
from fpdf import FPDF
from datetime import datetime


def limpar_excesso_caracteres(texto, caractere='*', max_repetidos=33):
    """Remove excesso de caracteres repetidos (ex: '******' -> '***')"""
    padrao = re.compile(fr'({re.escape(caractere)}){{{max_repetidos},}}')
    return padrao.sub(caractere * max_repetidos, texto)

def inserir_marcador_qrcode(texto_fatura):
    """Insere o marcador [[QR_CODE]] após o ATCUD"""
    padrao_atcud = re.compile(r"^(ATCUD:\s*[^\n]+)", re.MULTILINE | re.IGNORECASE)
    match = padrao_atcud.search(texto_fatura)
    
    if match:
        posicao = match.end()
        return texto_fatura[:posicao] + "\n[[QR_CODE]]\n" + texto_fatura[posicao:]
    
    # Fallback: insere no final se não encontrar ATCUD
    return texto_fatura + "\n[[QR_CODE]]"


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
    # Limpeza inicial
    text = limpar_excesso_caracteres(text.strip(), '*', 33)
    text = re.sub(r'\n+', '\n', text)

    faturas = []

    # Padrão para extrair todos os dados da fatura
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
            # Dados básicos
            nif_emitente = match.group('nif_emitente') or "000000000"
            nif_cliente = match.group('nif_cliente')
            if nif_cliente == "Consumidor final":
                nif_cliente = "999999990"
            data_doc = datetime.strptime(match.group('data'), '%d/%m/%y').strftime('%Y-%m-%d')
            numero_doc = match.group('numero_fatura').strip()

            # Extrair número da filial (entre 'FR' e 'Y')
            match_filial = re.search(r'FR\s*(\d+)[Yy]', numero_doc)
            filial = match_filial.group(1) if match_filial else '000000'

            # QRCode fictício (ajuste conforme sua lógica real)
            tipo_doc = "FT"
            autofaturado = "N"
            pais = "PT"
            iva_total = 0.0
            outros_impostos = 0.0
            retencao = 0.0
            total_impostos = 0.0
            estado = "N"
            certificado = "CERT123"

            qrcode = gerar_qrcode_texto(
                nif_emitente, nif_cliente, tipo_doc, autofaturado, data_doc, numero_doc,
                pais, iva_total, outros_impostos, retencao, total_impostos, estado, certificado
            )

            texto_original = match.group(0)
            texto_completo = inserir_marcador_qrcode(text)

            # Montar dicionário da fatura
            fatura = {
                "nif_emitente": nif_emitente,
                "numero_fatura": numero_doc,
                "data": data_doc,
                "hora": match.group('hora'),
                "mesa": match.group('mesa') or None,
                "nif_cliente": nif_cliente,
                "total": match.group('total').replace(',', '.'),
                "texto_original": texto_original,
                "texto_completo": texto_completo,
                "qrcode": qrcode,
                "filial": filial,
                "itens": []
            }

            # Extrair itens
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

