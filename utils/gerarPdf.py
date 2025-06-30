import qrcode
from io import BytesIO
from PIL import Image
from fpdf import FPDF
import tempfile
import os
import textwrap

def gerar_pdf(texto_completo, qrcode_text):
    largura_papel = 90  # mm
    altura_papel = 297  # mm
    margem = 0  # mm de margem nas laterais
    largura_util = largura_papel - 2 * margem
    altura_util = altura_papel - 2 * margem
    
    pdf = FPDF(unit='mm', format=(largura_papel, altura_papel))
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)  # vamos controlar a quebra manualmente
    pdf.set_font("Courier", size=9)
    
    # Gera o QR code a partir dos dados fornecidos
    qr = qrcode.make(qrcode_text)
    if not isinstance(qr, Image.Image):
        qr = qr.convert("RGB")
    
    qr_width, qr_height = qr.size
    largura_qrcode = 40  # mm
    altura_qrcode = largura_qrcode * (qr_height / qr_width)
    
    # Salva QR code em um arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
        qr.save(tmpfile.name)
        temp_file_path = tmpfile.name
    
    # Substitui o marcador [[QR_CODE]] pelo texto para cálculo preliminar
    texto_para_dividir = texto_completo.replace('[[QR_CODE]]', '[QR]')
    
    # Quebrar o texto em linhas que caibam na largura (aproximado)
    # Usamos textwrap para ajudar, pois Courier tamanho 9 ~ 2.5mm por caractere, aproximado
    max_chars_por_linha = int(largura_util / 2.5)  # ajuste fino
    
    linhas = []
    for linha in texto_para_dividir.splitlines():
        if linha.strip() == '[QR]':
            linhas.append('[QR]')  # marcador do QR
        else:
            linhas.extend(textwrap.wrap(linha, width=max_chars_por_linha))
    
    # Calcular espaço para texto e QR code
    # Sabemos a altura do QR code em mm, reservar espaço para ele
    # Vamos distribuir o resto do espaço para as linhas de texto com espaçamento ajustado
    
    # Altura total para texto = altura_util - altura_qrcode - espaço extra (10mm)
    espaco_extra = 10
    altura_texto = altura_util - altura_qrcode - espaco_extra
    num_linhas = len([l for l in linhas if l != '[QR]'])
    
    # Espaçamento vertical entre linhas
    if num_linhas > 0:
        espacamento_linha = altura_texto / num_linhas
    else:
        espacamento_linha = 5  # default
    
    # Começa a escrever o texto na posição y inicial
    y_atual = margem
    
    for linha in linhas:
        if linha == '[QR]':
            # espaço antes do QR code
            y_atual += 2
            pos_x = (largura_papel - largura_qrcode) / 2
            pdf.image(temp_file_path, x=pos_x, y=y_atual, w=largura_qrcode, h=altura_qrcode)
            y_atual += altura_qrcode + 5
        else:
            pdf.set_xy(margem, y_atual)
            # Centraliza horizontalmente a linha (width do cell = largura_util)
            pdf.cell(largura_util, espacamento_linha, linha, ln=1, align='C')
            y_atual += espacamento_linha
    
    # Limpa arquivo temporário
    os.unlink(temp_file_path)
    
    # Retorna o PDF como buffer
    pdf_output = pdf.output(dest='S').encode('latin1')
    buffer = BytesIO(pdf_output)
    buffer.seek(0)
    return buffer
