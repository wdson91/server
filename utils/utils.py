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

from datetime import date, timedelta
from dateutil.relativedelta import relativedelta
from collections import defaultdict


def parse_periodo(periodo):
    periodos = {
        0: "Hoje",
        1: "Ontem",
        2: "Semana",
        3: "Mês",
        4: "Trimestre",
        5: "Ano"
    }
    
    periodo = periodos.get(periodo)
    if periodo is None:
        raise ValueError("Período inválido. Use: 0 a 5")
    
    return periodo# mapping de periodos
    


def get_periodo_datas(periodo):
    hoje = date.today()

    if periodo == 0:
        return hoje, hoje, hoje - timedelta(days=1), hoje - timedelta(days=1)

    elif periodo == 1:
        ontem = hoje - timedelta(days=1)
        return ontem, ontem, hoje - timedelta(days=2), hoje - timedelta(days=2)

    elif periodo == 2:
        inicio_semana = hoje - timedelta(days=hoje.weekday())
        fim_semana = inicio_semana + timedelta(days=6)
        inicio_anterior = inicio_semana - timedelta(days=7)
        fim_anterior = inicio_semana - timedelta(days=1)
        return inicio_semana, fim_semana, inicio_anterior, fim_anterior

    elif periodo == 3:
        inicio_mes = hoje.replace(day=1)
        fim_mes = hoje
        inicio_anterior = (inicio_mes - relativedelta(months=1)).replace(day=1)
        fim_anterior = inicio_mes - timedelta(days=1)
        return inicio_mes, fim_mes, inicio_anterior, fim_anterior

    elif periodo == 4:
        mes_atual = hoje.month
        trimestre_inicio_mes = 1 + 3 * ((mes_atual - 1) // 3)
        inicio_trimestre = hoje.replace(month=trimestre_inicio_mes, day=1)
        fim_trimestre = hoje
        inicio_anterior = (inicio_trimestre - relativedelta(months=3)).replace(day=1)
        fim_anterior = inicio_trimestre - timedelta(days=1)
        return inicio_trimestre, fim_trimestre, inicio_anterior, fim_anterior

    elif periodo == 5:
        inicio_ano = hoje.replace(month=1, day=1)
        fim_ano = hoje
        inicio_anterior = (inicio_ano - relativedelta(years=1)).replace(month=1, day=1)
        fim_anterior = inicio_ano - timedelta(days=1)
        return inicio_ano, fim_ano, inicio_anterior, fim_anterior

    else:
        raise ValueError("Período inválido. Use: hoje, ontem, semana, mes, trimestre, ano.")



def buscar_faturas_periodo(nif, data_inicio, data_fim):
    response = supabase.table("faturas_fatura") \
        .select("*, itens:faturas_itemfatura(*)") \
        .gte("data", data_inicio.isoformat()) \
        .lte("data", data_fim.isoformat()) \
        .eq("nif", nif) \
        .execute()
    return response.data or []
