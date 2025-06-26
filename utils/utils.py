# 🔹 Funções auxiliares

from collections import defaultdict
from .supabaseUtil import get_supabase

supabase = get_supabase()

def is_valid_nif(nif):
    return nif and nif.isdigit()

# Funções para buscar faturas por data e NIF
def buscar_faturas_por_data(nif, data_obj):
    response = supabase.table("faturas_fatura") \
        .select("*, itens:faturas_itemfatura(*)") \
        .eq("data", data_obj.isoformat()) \
        .eq("nif", nif) \
        .execute()
    return response.data or []


def calcular_stats(faturas):
    total = sum(float(f["total"]) for f in faturas)
    recibos = len(faturas)
    itens = sum(item["quantidade"] for f in faturas for item in (f.get("itens") or []))
    ticket = round(total / recibos, 2) if recibos else 0.0
    return total, recibos, itens, ticket

def calcular_variacao_dados(atual, anterior):
    if anterior == 0:
        variacao = 100.0 if atual > 0 else 0.0
    else:
        variacao = ((atual - anterior) / anterior) * 100

    cor = "#28a745" if variacao >= 0 else "#dc3545"
    sinal = "+" if variacao >= 0 else ""
    return {
        "valor": round(atual, 2) if isinstance(atual, float) else atual,
        "variacao": f"{sinal}{round(variacao, 1)}%",
        "cor": cor,
        "ontem": round(anterior, 2) if isinstance(anterior, float) else anterior
    }

def agrupar_por_hora(faturas):
    horas = defaultdict(float)
    for f in faturas:
        hora_str = f.get("hora")
        if hora_str:
            try:
                hora = int(hora_str.split(":")[0])
                horas[hora] += float(f.get("total", 0))
            except (ValueError, IndexError):
                continue
    return horas

def gerar_comparativo_por_hora(vendas_hoje, vendas_ontem):
    comparativo = []
    for hora in range(24):
        hora_str = f"{hora:02d}:00"
        hoje_val = vendas_hoje.get(hora, 0.0)
        ontem_val = vendas_ontem.get(hora, 0.0)
        variacao = calcular_variacao_dados(hoje_val, ontem_val)

        comparativo.append({
            "hora": hora_str,
            "hora_num": float(hora),
            "hoje": round(hoje_val, 2),
            "ontem": round(ontem_val, 2),
            "variacao": variacao["variacao"],
            "cor": variacao["cor"]
        })
    return comparativo


import redis
redis_client = redis.Redis.from_url("redis://localhost:6379/0")

def limpar_cache_por_nif(nif: str):
    if not nif or not nif.isdigit():
        raise ValueError("NIF inválido")
    
    padrao = f"*/{nif}"
    cursor = 0
    chaves_removidas = 0

    while True:
        cursor, chaves = redis_client.scan(cursor=cursor, match=padrao, count=100)
        if chaves:
            redis_client.delete(*chaves)
            chaves_removidas += len(chaves)
        if cursor == 0:
            break
    
    return f"{chaves_removidas} chaves removidas para NIF {nif}"


def format_variacao(valor: float) -> dict:
    sinal = "+" if valor >= 0 else "-"
    cor = "#28a745" if valor >= 0 else "#dc3545"  # verde vs vermelho
    variacao_formatada = f"{sinal}{abs(round(valor, 1))}%"
    return {"variacao": variacao_formatada, "cor": cor}
