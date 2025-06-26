from flask import request, jsonify, g
from datetime import date, timedelta, datetime
from collections import defaultdict
from utils.supabaseUtil import get_supabase


supabase = get_supabase()

def parse_date_safe(date_str):
    try:
        return datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return None

def get_faturas():
    data_inicio = request.args.get("data_inicio")
    data_fim = request.args.get("data_fim")
    user_id = getattr(g, "user_id", None)
    nif = request.args.get("nif")  # ou extraído do token, se preferir

    print(f"ID do usuário: {user_id}")

    # Verifica se tem data de início e fim definidas
    if not data_inicio or not data_fim:
        hoje = date.today()
        primeiro_dia = hoje.replace(day=1)
        if hoje.month == 12:
            proximo_mes = hoje.replace(year=hoje.year + 1, month=1, day=1)
        else:
            proximo_mes = hoje.replace(month=hoje.month + 1, day=1)
        ultimo_dia = proximo_mes - timedelta(days=1)
        data_inicio = data_inicio or primeiro_dia.isoformat()
        data_fim = data_fim or ultimo_dia.isoformat()

    data_inicio_parsed = parse_date_safe(data_inicio)
    data_fim_parsed = parse_date_safe(data_fim)

    query = supabase.table("faturas_fatura").select("*, itens:faturas_itemfatura(*)")
    if data_inicio_parsed:
        query = query.gte("data", data_inicio)
    if data_fim_parsed:
        query = query.lte("data", data_fim)
    if nif:
        query = query.eq("nif", nif)  # Novo filtro

    result = query.execute()
    faturas = result.data if result.data else []

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

    vendas_por_dia = defaultdict(float)
    for f in faturas:
        vendas_por_dia[f["data"]] += float(f["total"])

    vendas = sorted(
        [{"data": data, "total": total} for data, total in vendas_por_dia.items()],
        key=lambda x: x["data"]
    )

    vendas_por_hora = defaultdict(float)
    for f in faturas:
        hora = f.get("hora")
        if hora:
            hora_formatada = hora[:2] + ":00"  # Assume formato HH:MM:SS
            vendas_por_hora[hora_formatada] += float(f["total"])

    vendas_horarias = sorted(
        [{"hora": hora, "total": total} for hora, total in vendas_por_hora.items()],
        key=lambda x: x["hora"]
    )

    return jsonify({
        "total_vendas": round(total_vendas, 2),
        "total_itens": total_itens,
        "vendas_por_dia": vendas,
        "vendas_por_hora": vendas_horarias,
        "vendas_por_produto": produtos,
        "quantidade_faturas": len(faturas),
        "filtro_data_inicio": data_inicio,
        "filtro_data_fim": data_fim,
    })

