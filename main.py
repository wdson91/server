from flask import Flask, request, jsonify
import re
from datetime import datetime
import os
import json

app = Flask(__name__)

DATA_FILE = "faturas.json"
MAX_FILE_SIZE = 2 * 1024 * 1024  # 2MB

def load_data():
    if os.path.exists(DATA_FILE):
        with open(DATA_FILE, "r", encoding="utf-8") as file:
            try:
                return json.load(file)
            except json.JSONDecodeError:
                return []
    return []

def save_data(data):
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def parse_fatura(text):
    fatura = {
        "numero_fatura": None,
        "data": None,
        "hora": None,
        "itens": [],
        "total": 0.0
    }

    match = re.search(r"Fatura-Recibo nº\s*(FR\s*\S+)", text)
    if match:
        fatura["numero_fatura"] = match.group(1).strip()

    match = re.search(r"Data:\s*(\d{1,2})/(\d{1,2})/(\d{2})\s+(\d{2}):(\d{2})", text)
    if match:
        dia = match.group(1).zfill(2)
        mes = match.group(2).zfill(2)
        ano = match.group(3)
        hora = match.group(4).zfill(2)
        minuto = match.group(5).zfill(2)
        fatura["data"] = f"{dia}/{mes}/{ano}"
        fatura["hora"] = f"{hora}:{minuto}"

    item_pattern = re.findall(r"(\d+)\s+x\s+(.+?)\s+@\s+([\d,\.]+).*?(\d{1,2}%)[\s\u20AC]*([\d,\.]+)", text)
    for qty, name, unit_price, tax, total in item_pattern:
        fatura["itens"].append({
            "nome": name.strip(),
            "quantidade": int(qty),
            "preco_unitario": float(unit_price.replace(",", ".")),
            "total": float(total.replace(",", "."))
        })

    match = re.search(r"Total\s+([\d,\.]+)", text)
    if match:
        fatura["total"] = float(match.group(1).replace(",", "."))

    return fatura

@app.route('/upload', methods=['POST'])
def upload_fatura():
    if 'file' not in request.files:
        return jsonify({'erro': 'Nenhum arquivo enviado'}), 400

    arquivo = request.files['file']
    if not arquivo.filename.endswith('.txt'):
        return jsonify({'erro': 'Apenas arquivos .txt são aceitos'}), 400

    arquivo.seek(0, 2)
    tamanho = arquivo.tell()
    arquivo.seek(0)
    if tamanho > MAX_FILE_SIZE:
        return jsonify({'erro': 'Arquivo muito grande. Máximo permitido: 2MB'}), 400

    try:
        texto = arquivo.read().decode('utf-8')
    except UnicodeDecodeError:
        return jsonify({'erro': 'Arquivo com codificação inválida. Use UTF-8'}), 400

    try:
        dados = parse_fatura(texto)
    except Exception as e:
        return jsonify({'erro': f'Erro ao processar a fatura: {str(e)}'}), 400

    campos_obrigatorios = ['numero_fatura', 'data', 'hora', 'itens', 'total']
    for campo in campos_obrigatorios:
        if campo not in dados or dados[campo] in [None, '', []]:
            return jsonify({'erro': f'Campo obrigatório ausente ou vazio: {campo}'}), 400

    # Carrega dados atuais do arquivo
    faturas = load_data()

    # Verifica se já existe fatura com mesmo número
    if any(f['numero_fatura'] == dados['numero_fatura'] for f in faturas):
        return jsonify({'erro': f'Fatura com número {dados["numero_fatura"]} já existe'}), 409

    if not isinstance(dados['itens'], list) or len(dados['itens']) == 0:
        return jsonify({'erro': 'A fatura deve conter ao menos um item válido'}), 400

    for item in dados['itens']:
        if not all(k in item for k in ['nome', 'quantidade', 'preco_unitario', 'total']):
            return jsonify({'erro': 'Item da fatura com dados incompletos'}), 400

    # Adiciona nova fatura e salva
    faturas.append(dados)
    save_data(faturas)

    return jsonify({'mensagem': 'Fatura processada com sucesso', 'dados': dados}), 201

@app.route('/faturas', methods=['GET'])
def get_faturas():
    faturas = load_data()
    return jsonify(faturas)

@app.route("/stats", methods=["GET"])
def stats():
    filtro_data = request.args.get("data")
    faturas = load_data()

    if filtro_data:
        data = [f for f in faturas if f.get("data") == filtro_data]
    else:
        data = faturas

    total_vendas = sum(f["total"] for f in data)
    total_itens = sum(item["quantidade"] for f in data for item in f["itens"])

    produtos = {}
    for f in data:
        for item in f["itens"]:
            produtos[item["nome"]] = produtos.get(item["nome"], 0) + item["quantidade"]

    return jsonify({
        "total_vendas": round(total_vendas, 2),
        "total_itens": total_itens,
        "vendas_por_produto": produtos,
        "quantidade_faturas": len(data),
        "filtro_data": filtro_data
    })

@app.route("/")
def index():
    return app.send_static_file("index.html")

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
