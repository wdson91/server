import qrcode
from io import BytesIO
from PIL import Image
from fpdf import FPDF
import tempfile
import os
import textwrap
def gerar_pdf(texto_completo, qrcode_text):
    # Configurações do papel
    largura_papel = 97 # mm
    altura_papel = 297  # mm
    margem = 5  # mm
    
    pdf = FPDF(unit='mm', format=(largura_papel, altura_papel))
    pdf.add_page()
    pdf.set_auto_page_break(auto=False)
    pdf.set_font("Courier", size=10)
    
    # Gerar QR Code
    qr = qrcode.make(qrcode_text)
    with tempfile.NamedTemporaryFile(delete=False, suffix='.png') as tmpfile:
        qr.save(tmpfile.name)
        temp_file_path = tmpfile.name
    
    # Processar texto e substituir [[QR_CODE]] pela imagem
    y_pos = margem
    linhas = texto_completo.split('\n')
    qr_size = 25  # tamanho do QR code em mm
    
    for linha in linhas:
        linha = linha.strip()
        
        if not linha:
            continue
            
        # Substituir marcador QR_CODE pela imagem
        if linha == '[[QR_CODE]]':
            # Centralizar QR code
            qr_x = (largura_papel - qr_size) / 2
            pdf.image(temp_file_path, x=qr_x, y=y_pos, w=qr_size, h=qr_size)
            y_pos += qr_size + 5
            continue
            
        # Formatação condicional
        if "Fatura-Recibo" in linha:
            pdf.set_font("Courier", 'B', 11)
        elif "N.I.F." in linha or "Total" in linha:
            pdf.set_font("Courier", 'B', 10)
        elif "Processado" in linha or "ATCUD" in linha:
            pdf.set_font("Courier", 'I', 8)
        else:
            pdf.set_font("Courier", size=10)
        
        # Alinhamento condicional
        if any(x in linha for x in ["Fatura-Recibo", "N.I.F.", "Processado", "ATCUD", "Obrigado"]):
            align = 'C'
        elif "@" in linha:  # Linha de item
            align = 'L'
            # Formatar itens em colunas
            partes = linha.split('@')
            descricao = partes[0].strip()
            valores = partes[1].strip().split()
            
            pdf.set_xy(margem, y_pos)
            pdf.cell(40, 5, descricao, 0, 0)
            
            if len(valores) >= 3:
                pdf.set_xy(margem + 40, y_pos)
                pdf.cell(15, 5, valores[0], 0, 0, 'R')
                pdf.set_xy(margem + 55, y_pos)
                pdf.cell(10, 5, valores[1], 0, 0, 'R')
                pdf.set_xy(margem + 65, y_pos)
                pdf.cell(10, 5, valores[2], 0, 0, 'R')
        else:
            align = 'L'
            pdf.set_xy(margem, y_pos)
            pdf.cell(0, 5, linha, 0, 1)
        
        if align != 'L':  # Centralizado ou direito
            pdf.set_xy(0, y_pos)
            pdf.cell(largura_papel, 5, linha, 0, 1, align)
        
        y_pos += 5
        
        # Adicionar linhas separadoras após certas seções
        if "N.I.F." in linha or "Total" in linha:
            y_pos += 3
            pdf.line(margem, y_pos, largura_papel-margem, y_pos)
            y_pos += 5
    
    # Limpar arquivo temporário
    os.unlink(temp_file_path)
    
    # Retornar PDF
    pdf_output = pdf.output(dest='S').encode('latin1')
    return BytesIO(pdf_output)