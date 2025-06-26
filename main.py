from flask import Flask, request, jsonify, g
import re
from datetime import date, datetime, time
import os
import json
from flask_sqlalchemy import SQLAlchemy
import requests
from sqlalchemy import text
# carrrega as variáveis de ambiente do arquivo .env
from dotenv import load_dotenv
from decorator import require_valid_token
from utils.supabaseUtil import get_supabase
from collections import defaultdict
import pytz
from datetime import timedelta
load_dotenv()
from flask_caching import Cache


app = Flask(__name__)

cache = Cache(app, config={
    "CACHE_TYPE": "RedisCache",
    "CACHE_REDIS_URL": "redis://localhost:6379/0",  # ajuste se necessário
    "CACHE_DEFAULT_TIMEOUT": 180  # 5 minutos
})
supabase = get_supabase()


def cache_key():
    nif = request.args.get("nif", "")
    periodo = int(request.args.get("periodo", 0))
    rota = request.path  # ex: "/api/products"
    return f"{rota}/{nif}/{periodo}"


@app.route('/protegido')
@require_valid_token
def protegido():
    return jsonify({"mensagem": "Acesso permitido!", "user_id": g.user_id})

from getFaturas import get_faturas

@app.route("/api/stats", methods=["GET"])
@require_valid_token
def relatorio():
    return get_faturas()


@app.route("/api/stats/today", methods=["GET"])
@cache.cached(timeout=180, key_prefix=cache_key)
@require_valid_token
def stats():

    nif = request.args.get("nif")
    #nif = '514757876'
    if not nif or not nif.isdigit():
        return jsonify({"error": "NIF é obrigatório e deve conter apenas números"}), 400

    hoje = date.today()
    tz = pytz.timezone("Europe/Lisbon")

    
    # Consulta bruta no Supabase
    result_today = supabase.table("faturas_fatura") \
        .select("*, itens:faturas_itemfatura(*)") \
        .eq('nif', nif) \
        .eq("data", hoje.isoformat()) \
       .execute()
    
    faturas_raw = result_today.data or []
    # ✅ Reforça o filtro manual em Python
    faturas = [f for f in faturas_raw if str(f.get("nif")) == nif]
    
    # Estatísticas de hoje
    total_vendas = sum(float(f["total"]) for f in faturas)
    total_itens = sum(
        item["quantidade"]
        for f in faturas
        for item in (f.get("itens") or [])
    )

    contagem_produtos = defaultdict(int)
    for f in faturas:
        for item in (f.get("itens") or []):
            contagem_produtos[item["nome"]] += item["quantidade"]

    produtos = sorted(
        [{"produto": nome, "quantidade": qtd} for nome, qtd in contagem_produtos.items()],
        key=lambda x: x["quantidade"],
        reverse=True
    )

    vendas_por_hora_real = defaultdict(float)
    for f in faturas:
        hora_str = f.get("hora", "")
        if hora_str:
            hora_formatada = hora_str[:2] + ":00"
            vendas_por_hora_real[hora_formatada] += float(f["total"])

    horas_base = {"08:00", "12:00", "18:00"} | set(vendas_por_hora_real.keys())
    vendas_horarias = [
        {"hora": hora, "total": round(vendas_por_hora_real.get(hora, 0.0), 2)}
        for hora in sorted(horas_base)
    ]

    # Marca o momento da atualização
    agora = datetime.now(tz)
    ultima_atualizacao = agora.strftime("%H:%M")

    # Faturas últimos 7 dias até ontem
    ontem = hoje - timedelta(days=1)
    sete_dias_atras = hoje - timedelta(days=7)
    result_7 = supabase.table("faturas_fatura") \
        .select("data, total") \
        .gte("data", sete_dias_atras.isoformat()) \
        .lte("data", ontem.isoformat()) \
        .execute()
    faturas_7 = result_7.data or []

    vendas_ultimos_7 = defaultdict(float)
    for f in faturas_7:
        vendas_ultimos_7[f["data"]] += float(f["total"])

    vendas_por_dia_ultimos_7 = [
        {"data": dia, "total": round(total, 2)}
        for dia, total in sorted(vendas_ultimos_7.items())
    ]
    total_ultimos_7 = round(sum(float(f["total"]) for f in faturas_7), 2)

    resultado = {
        "dados": {
            "total_vendas": round(total_vendas, 2),
            "total_itens": total_itens,
            "vendas_por_dia": [{"data": str(hoje), "total": round(total_vendas, 2)}],
            "vendas_por_hora": vendas_horarias,
            "vendas_por_produto": produtos,
            "quantidade_faturas": len(faturas),
            "filtro_data": str(hoje),
            "ultima_atualizacao": ultima_atualizacao,
            "ultimos_7_dias": vendas_por_dia_ultimos_7,
            "total_ultimos_7_dias": total_ultimos_7,
        }
    }

    return jsonify(resultado), 200

@app.route("/api/stats/report", methods=["GET"])
@cache.cached(timeout=180, key_prefix=cache_key)
@require_valid_token
def faturas_agrupadas_view():
    nif = request.args.get("nif")
    if not nif:
        return jsonify({"error": "NIF é obrigatório"}), 400

    hoje = date.today()
    sete_dias_atras = hoje - timedelta(days=7)
    ontem = hoje - timedelta(days=1)
    tz = pytz.timezone("Europe/Lisbon")
    agora = datetime.now(tz)

    # Faturas de hoje com filtro por NIF
    result_hoje = supabase.table("faturas_fatura") \
        .select("*") \
        .eq("data", hoje.isoformat()) \
        .eq("nif", nif) \
        .execute()

    # Faturas dos últimos 7 dias (excluindo hoje)
    result_7_dias = supabase.table("faturas_fatura") \
        .select("*") \
        .gte("data", sete_dias_atras.isoformat()) \
        .lte("data", ontem.isoformat()) \
        .eq("nif", nif) \
        .execute()

    faturas_hoje = result_hoje.data or []
    faturas_7_dias = result_7_dias.data or []

    if not faturas_hoje and not faturas_7_dias:
        return jsonify({"message": "Nenhuma fatura encontrada para o NIF informado."}), 404

    def agrupar_por_hora(faturas):
        agrupado = defaultdict(lambda: {"volume": 0.0, "quantidade": 0})
        for f in faturas:
            hora_str = f.get("hora")
            if not hora_str:
                continue
            hora_formatada = datetime.strptime(hora_str, "%H:%M:%S").strftime("%H:00")
            agrupado[hora_formatada]["volume"] += float(f.get("total", 0))
            agrupado[hora_formatada]["quantidade"] += 1
        return agrupado

    totais_hoje = agrupar_por_hora(faturas_hoje)
    totais_7_dias = agrupar_por_hora(faturas_7_dias)

    horas_com_dados = set(totais_hoje.keys()) | set(totais_7_dias.keys())
    vendas_por_hora_formatado = []
    for hora in sorted(horas_com_dados):
        vendas_por_hora_formatado.append({
            "hora": hora,
            "faturas_hoje": totais_hoje.get(hora, {}).get("quantidade", 0),
            "volume_hoje": round(totais_hoje.get(hora, {}).get("volume", 0), 2),
            "faturas_7_dias": totais_7_dias.get(hora, {}).get("quantidade", 0),
            "volume_7_dias": round(totais_7_dias.get(hora, {}).get("volume", 0), 2)
        })

    total_vendas_hoje = round(sum(float(f.get("total", 0)) for f in faturas_hoje), 2)
    quantidade_faturas_hoje = len(faturas_hoje)

    # Vendas por dia - hoje
    vendas_por_dia = [{
        "data": hoje.strftime("%Y-%m-%d"),
        "total": total_vendas_hoje
    }]

    # Vendas últimos 7 dias (agrupar por data)
    vendas_7_dias_dict = defaultdict(float)
    for f in faturas_7_dias:
        data_f = f["data"]
        vendas_7_dias_dict[data_f] += float(f.get("total", 0))

    ultimos_7_dias = [
        {"data": data, "total": round(total, 2)}
        for data, total in sorted(vendas_7_dias_dict.items())
    ]
    total_ultimos_7_dias = round(sum(v["total"] for v in ultimos_7_dias), 2)

    resposta = {
        "dados": {
            "total_vendas": total_vendas_hoje,
            "quantidade_faturas": quantidade_faturas_hoje,
            "vendas_por_dia": vendas_por_dia,
            "vendas_por_hora": vendas_por_hora_formatado,
            "filtro_data": hoje.strftime("%Y-%m-%d"),
            "ultima_atualizacao": agora.strftime("%H:%M"),
            "ultimos_7_dias": ultimos_7_dias,
            "total_ultimos_7_dias": total_ultimos_7_dias,
            "quantidade_faturas_7_dias": len(faturas_7_dias),
        }
    }

    return jsonify(resposta)


@app.route("/api/products", methods=["GET"])
@cache.cached(timeout=180, key_prefix=cache_key)
@require_valid_token
def mais_vendidos():
    nif = request.args.get("nif")
    if not nif or not nif.isdigit():
        return jsonify({"error": "NIF é obrigatório e deve conter apenas números"}), 400

    hoje = date.today()

    result = supabase.table("faturas_fatura") \
        .select("*, itens:faturas_itemfatura(*)") \
        .eq("nif", nif) \
        .eq("data", hoje.isoformat()) \
        .execute()
    
    faturas = result.data or []

    contagem_produtos = defaultdict(lambda: {"quantidade": 0, "montante": 0.0})
    total_itens = 0
    total_montante = 0.0

    for f in faturas:
        for item in (f.get("itens") or []):
            nome = item.get("nome")
            qtd = item.get("quantidade", 0)
            preco_unitario = float(item.get("preco_unitario", 0.0))

            contagem_produtos[nome]["quantidade"] += qtd
            contagem_produtos[nome]["montante"] += qtd * preco_unitario

            total_itens += qtd
            total_montante += qtd * preco_unitario

    itens_formatados = sorted([
        {
            "produto": nome,
            "quantidade": dados["quantidade"],
            "montante": round(dados["montante"], 2),
            "porcentagem_montante": round((dados["montante"] / total_montante) * 100, 2) if total_montante else 0.0
        }
        for nome, dados in contagem_produtos.items()
    ], key=lambda x: x["montante"], reverse=True)

    resultado = {
        "dados": {
            "data": str(hoje),
            "total_itens": total_itens,
            "total_montante": round(total_montante, 2),
            "itens": itens_formatados
        }
    }

    return jsonify(resultado), 200




@app.route("/api/limparcache", methods=["DELETE"])
def limpar_cache():
    """
    Limpa todo o cache do Redis.
    """
    try:
        limpar_cache_por_nif(request.args.get("nif"))
        return jsonify({"message": "Cache limpo com sucesso"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500




from utils.utils import *

@app.route("/api/stats/resumo", methods=["GET"])
@require_valid_token
@cache.cached(timeout=180, key_prefix=cache_key)
def resumo_stats():
    nif = request.args.get("nif")
    periodo = int(request.args.get("periodo", 0))
    print(f"Periodo: {periodo}")
    if not is_valid_nif(nif):
        return jsonify({"error": "NIF é obrigatório e deve conter apenas números"}), 400

    try:
        data_inicio, data_fim, data_inicio_anterior, data_fim_anterior = get_periodo_datas(periodo)
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    faturas_atual = buscar_faturas_periodo(nif, data_inicio, data_fim)
    faturas_anterior = buscar_faturas_periodo(nif, data_inicio_anterior, data_fim_anterior)

    total_atual, recibos_atual, itens_atual, ticket_atual = calcular_stats(faturas_atual)
    total_ant, recibos_ant, itens_ant, ticket_ant = calcular_stats(faturas_anterior)

    vendas_atual_por_hora = agrupar_por_hora(faturas_atual)
    vendas_ant_por_hora = agrupar_por_hora(faturas_anterior)

    comparativo_por_hora = gerar_comparativo_por_hora(vendas_atual_por_hora, vendas_ant_por_hora)

    # mapping de periodos
    periodos = {
        0: "Hoje",
        1: "Ontem",
        2: "Semana",
        3: "Mês",
        4: "Trimestre",
        5: "Ano"
    }

    dados = {
        "periodo": periodos.get(periodo, "Personalizado"),
        "total_vendas": calcular_variacao_dados(total_atual, total_ant),
        "numero_recibos": calcular_variacao_dados(recibos_atual, recibos_ant),
        "itens_vendidos": calcular_variacao_dados(itens_atual, itens_ant),
        "ticket_medio": calcular_variacao_dados(ticket_atual, ticket_ant),
        "comparativo_por_hora": comparativo_por_hora
    }

    return jsonify({"dados": dados}), 200





if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8000, debug=True)
