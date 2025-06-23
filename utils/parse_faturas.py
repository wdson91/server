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



def parse_faturas(texto_completo):
    # Divide o texto com base no início de cada fatura
    blocos = re.split(r"(?=Fatura-Recibo nº)", texto_completo)
    blocos = [b.strip() for b in blocos if b.strip()]

    lista_faturas = []

    for fatura_text in blocos:
        fatura = {
            "numero_fatura": None,
            "data": None,
            "hora": None,
            "nif_emitente": "000000000",  # padrão
            "nif_cliente": "999999990",   # padrão
            "itens": [],
            "total": 0.0,
            "texto_original": fatura_text,
            "texto_completo": None,
            "qrcode": None,
        }

        # Número da fatura
        match_num = re.search(r"Fatura-Recibo nº\s*(FR\s*\S+)", fatura_text)
        if match_num:
            fatura["numero_fatura"] = match_num.group(1).strip()

        # Data e hora
        match_data = re.search(r"Data:\s*(\d{1,2})/(\d{1,2})/(\d{2})\s+(\d{2}):(\d{2})", fatura_text)
        if match_data:
            dia = match_data.group(1).zfill(2)
            mes = match_data.group(2).zfill(2)
            ano = match_data.group(3)
            hora = match_data.group(4).zfill(2)
            minuto = match_data.group(5).zfill(2)
            fatura["data"] = f"20{ano}-{mes}-{dia}"
            fatura["hora"] = f"{hora}:{minuto}:00"

        # NIFs
        nifs_encontrados = re.findall(r"N\.I\.F\.[:\s]*([0-9]{9})", fatura_text, re.IGNORECASE)
        if len(nifs_encontrados) > 0:
            fatura["nif_emitente"] = nifs_encontrados[0]
        if len(nifs_encontrados) > 1:
            fatura["nif_cliente"] = nifs_encontrados[1]

        # Itens
        item_pattern = re.findall(
            r"(\d+)\s+x\s+(.+?)\s+@\s+([\d,\.]+)[^\n\r]*?(\d{1,2})%\s+([\d,\.]+)",
            fatura_text
        )
        for qty, name, unit_price, tax, total in item_pattern:
            preco_unit = float(unit_price.replace(",", "."))
            total_item = float(total.replace(",", "."))
            taxa_iva = float(tax) / 100
            fatura["itens"].append({
                "nome": name.strip(),
                "quantidade": int(qty),
                "preco_unitario": preco_unit,
                "taxa_iva": taxa_iva,
                "total": total_item
            })

        # Total
        match_total = re.search(r"Total\s+([\d,\.]+)", fatura_text)
        if match_total:
            fatura["total"] = float(match_total.group(1).replace(",", "."))

        # IVA
        iva_total = sum(item["total"] * item["taxa_iva"] for item in fatura["itens"])
        total_impostos = round(iva_total, 2)
       
        # Apenas gera o QR Code se a data e número da fatura foram extraídos corretamente
        if fatura["data"] and fatura["numero_fatura"]:
            qrcode_texto = gerar_qrcode_texto(
                nif_emitente=fatura["nif_emitente"],
                nif_cliente=fatura["nif_cliente"],
                tipo_doc="FR",
                autofaturado="N",
                data_doc=fatura["data"].replace("-", ""),
                numero_doc=fatura["numero_fatura"],
                pais="PT",
                iva_total=iva_total,
                outros_impostos=0.00,
                retencao=0.00,
                total_impostos=total_impostos,
                estado="F/eR",
                certificado="1530"
            )
            fatura["qrcode"] = qrcode_texto
        else:
            fatura["qrcode"] = None  # ou "" se preferir

        
        # Inserção do QR Code após o ATCUD
        atcud_match = re.search(r"(.*?ATCUD:\s*\S+.*?\n)", fatura_text, re.DOTALL)
        if atcud_match:
            pos = atcud_match.end()
            parte1 = fatura_text[:pos].rstrip()
            parte2 = fatura_text[pos:].lstrip()
            texto_com_qrcode = f"{parte1}\n\n[[QR_CODE]]\n\n{parte2}"
        else:
            texto_com_qrcode = f"{fatura_text}\n\n[[QR_CODE]]"

        fatura["texto_completo"] = texto_com_qrcode

        lista_faturas.append(fatura)

    return lista_faturas


