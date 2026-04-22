# 🔹 Funções auxiliares

from collections import defaultdict
from typing import Optional
from sqlalchemy import text
from utils.db import Session

def is_valid_nif(nif):
    return nif and nif.isdigit()

# Funções para buscar faturas por data e NIF
def buscar_faturas_por_data(nif, data_obj):
    """Busca faturas por data e NIF (agora usando a tabela invoices)"""
    session = Session()
    try:
        # Nota: Ajustamos nomes de colunas conforme o novo schema Prisma
        query = text("""
            SELECT i.*, 
                   (SELECT json_agg(l.*) FROM invoice_lines l WHERE l.invoice_id = i.id) as itens
            FROM invoices i
            WHERE i.invoice_date = :data 
              AND i.company_id = :nif
              AND i.active = true
        """)
        result = session.execute(query, {"data": data_obj, "nif": nif})
        faturas = []
        for row in result:
            # Converter Row para dict compatível com o código antigo
            d = dict(row._mapping)
            # Mapeamento de nomes de colunas para manter compatibilidade com o frontend/lógica antiga se necessário
            # Se o frontend espera 'total', mapeamos 'gross_total' para 'total'
            d['total'] = d.get('gross_total')
            d['data'] = d.get('invoice_date')
            d['numero_fatura'] = d.get('invoice_no')
            faturas.append(d)
        return faturas
    except Exception as e:
        print(f"Erro ao buscar faturas por data: {str(e)}")
        return []
    finally:
        Session.remove()


def calcular_stats(faturas):
    total = sum(float(f.get("total") or 0) for f in faturas)
    recibos = len(faturas)
    itens = sum(item.get("quantity") or 0 for f in faturas for item in (f.get("itens") or []))
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
        # No novo schema, invoice_status_time é Time(6)
        hora_val = f.get("invoice_status_time")
        if hora_val:
            try:
                if isinstance(hora_val, str):
                    hora = int(hora_val.split(":")[0])
                else: # datetime.time object
                    hora = hora_val.hour
                horas[hora] += float(f.get("total", 0))
            except (ValueError, AttributeError):
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
    
    try:
        # Usar cache do Flask
        from main import cache
        # Limpar cache específico para o NIF
        cache.delete(f"dados_resumo_ia:{nif}:todas:0")
        cache.delete(f"dados_resumo_ia:{nif}:todas:1")
        cache.delete(f"dados_resumo_ia:{nif}:todas:2")
        cache.delete(f"dados_resumo_ia:{nif}:todas:3")
        cache.delete(f"dados_resumo_ia:{nif}:todas:4")
        cache.delete(f"dados_resumo_ia:{nif}:todas:5")
        return f"Cache limpo para NIF {nif}"
    except Exception as e:
        # Se houver erro, retorna mensagem de erro
        return f"Erro ao limpar cache: {str(e)}"


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



def buscar_faturas_periodo(nif, data_ini, data_fim, filial=None):
    """
    Busca faturas de um período específico usando SQLAlchemy.
    """
    session = Session()
    try:
        sql = """
            SELECT i.*, 
                   (SELECT json_agg(l.*) FROM invoice_lines l WHERE l.invoice_id = i.id) as itens
            FROM invoices i
            WHERE i.company_id = :nif
              AND i.invoice_date >= :data_ini
              AND i.invoice_date <= :data_fim
              AND i.active = true
        """
        params = {"nif": nif, "data_ini": data_ini, "data_fim": data_fim}
        
        if filial:
            sql += " AND i.filial = :filial"
            params["filial"] = filial

        result = session.execute(text(sql), params)
        faturas = []
        for row in result:
            d = dict(row._mapping)
            # Mapeamento para compatibilidade
            d['total'] = d.get('gross_total')
            d['data'] = d.get('invoice_date')
            d['numero_fatura'] = d.get('invoice_no')
            d['hora'] = d.get('invoice_status_time')
            faturas.append(d)
        return faturas
        
    except Exception as e:
        print(f"Erro ao buscar faturas: {str(e)}")
        return []
    finally:
        Session.remove()


def limpar_cache_dados_ia(nif: str, periodo: Optional[int] = None, filial: Optional[str] = None):
    """
    Limpa cache específico da função gerar_dados_resumo_ia
    """
    if not nif or not nif.isdigit():
        raise ValueError("NIF inválido")
    
    try:
        from main import cache
        
        if periodo is not None:
            # Limpar cache específico para período
            filial_key = filial or "todas"
            cache_key = f"dados_resumo_ia:{nif}:{filial_key}:{periodo}"
            cache.delete(cache_key)
            return f"Cache limpo para NIF {nif}, período {periodo}"
        else:
            # Limpar cache para todos os períodos
            filial_key = filial or "todas"
            for p in range(6):  # 0-5 períodos
                cache_key = f"dados_resumo_ia:{nif}:{filial_key}:{p}"
                cache.delete(cache_key)
            return f"Cache limpo para NIF {nif}, todos os períodos"
            
    except Exception as e:
        return f"Erro ao limpar cache: {str(e)}"


def buscar_faturas_multiplos_periodos(nif, periodos_datas, filial=None):
    """
    Busca faturas para múltiplos períodos em uma única consulta.
    """
    try:
        # Encontrar o período mais amplo
        todas_datas = []
        for data_ini, data_fim, _ in periodos_datas:
            todas_datas.extend([data_ini, data_fim])
        
        data_mais_antiga = min(todas_datas)
        data_mais_recente = max(todas_datas)
        
        # Uma única consulta ao banco
        faturas_completas = buscar_faturas_periodo(nif, data_mais_antiga, data_mais_recente, filial)
        
        # Filtrar faturas por período
        resultado = {}
        
        for data_ini, data_fim, nome_periodo in periodos_datas:
            faturas_periodo = []
            
            for fatura in faturas_completas:
                data_fatura = fatura.get('data')
                if data_fatura:
                    if isinstance(data_fatura, str):
                        from datetime import datetime
                        data_fatura = datetime.strptime(data_fatura, '%Y-%m-%d').date()
                    
                    if data_ini <= data_fatura <= data_fim:
                        faturas_periodo.append(fatura)
            
            resultado[nome_periodo] = faturas_periodo
        
        return resultado
        
    except Exception as e:
        print(f"Erro ao buscar faturas múltiplos períodos: {str(e)}")
        return {}


def gerar_dados_resumo_ia(nif: str, periodo: int, filial: Optional[str] = None) -> dict:
    """
    Gera dados estruturados e otimizados para análise de IA.
    Busca dados reais do banco de dados e calcula métricas comparativas.
    """
    from datetime import datetime
    
    # Gerar chave de cache única
    filial_key = filial or "todas"
    cache_key = f"dados_resumo_ia:{nif}:{filial_key}:{periodo}"
    
    try:
        # Tentar obter dados do cache primeiro
        try:
            from main import cache
            cached_data = cache.get(cache_key)
            if cached_data:
                return {"success": True, "data": cached_data, "from_cache": True}
        except ImportError:
            pass
        
        # Obter datas do período
        data_inicio, data_fim, data_inicio_anterior, data_fim_anterior = get_periodo_datas(periodo)

        data_mais_antiga = min(data_inicio_anterior, data_inicio)
        data_mais_recente = max(data_fim_anterior, data_fim)
        
        faturas_completas = buscar_faturas_periodo(nif, data_mais_antiga, data_mais_recente, filial=filial)
        
        # Processar todas as faturas
        dados_processados = processar_faturas_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior)
        
        total_at, rec_at, it_at, tk_at = dados_processados['stats_atual']
        total_bt, rec_bt, it_bt, tk_bt = dados_processados['stats_anterior']
        comparativo_hora = dados_processados['comparativo_por_hora']
        faturas_atual = dados_processados['faturas_atual']
        vendas_por_hora_atual = dados_processados['vendas_por_hora_atual']

        # Análise de produtos
        produtos_mais_vendidos = []
        produtos_unicos_count = 0
        if faturas_atual:
            produtos = defaultdict(lambda: {'quantidade': 0, 'faturamento': 0.0})
            for fatura in faturas_atual:
                for item in fatura.get('itens', []):
                    nome_produto = item.get('description', 'Produto Desconhecido')
                    quantidade = float(item.get('quantity', 0))
                    total_item = float(item.get('price_with_iva', 0))
                    
                    produtos[nome_produto]['quantidade'] += quantidade
                    produtos[nome_produto]['faturamento'] += total_item
            
            produtos_mais_vendidos = sorted(
                [{'produto': k, 'quantidade': v['quantidade'], 'faturamento': round(v['faturamento'], 2)} for k, v in produtos.items()],
                key=lambda x: x['faturamento'],
                reverse=True
            )[:10]
            produtos_unicos_count = len(produtos)

        # Análise por filiais
        analise_filiais = {}
        if not filial:
            filiais_agg = defaultdict(float)
            for fatura in faturas_atual:
                filiais_agg[fatura.get('filial', 'Sem Filial')] += float(fatura.get('total', 0))
            
            if filiais_agg:
                analise_filiais['volume_por_filial'] = sorted(
                    [{'filial': k, 'volume': round(v, 2), 'percentual_total': (v / total_at * 100) if total_at > 0 else 0} for k, v in filiais_agg.items()],
                    key=lambda x: x['volume'],
                    reverse=True
                )
                analise_filiais['total_filiais'] = len(filiais_agg)

        # Preparar dados estruturados
        dados_ia = {
            "metadata": {
                "nif": nif,
                "filial_solicitada": filial,
                "periodo": {
                    "codigo": periodo,
                    "nome": parse_periodo(periodo),
                    "inicio_atual": data_inicio.isoformat(),
                    "fim_atual": data_fim.isoformat(),
                    "inicio_anterior": data_inicio_anterior.isoformat(),
                    "fim_anterior": data_fim_anterior.isoformat()
                },
                "timestamp_geracao": datetime.now().isoformat()
            },
            "resumo_geral": {
                "faturas_processadas_atual": rec_at,
                "faturas_processadas_anterior": rec_bt,
            },
            "metricas_comparativas": {
                "total_vendas": calcular_variacao_dados(total_at, total_bt),
                "numero_recibos": calcular_variacao_dados(rec_at, rec_bt),
                "itens_vendidos": calcular_variacao_dados(it_at, it_bt),
                "ticket_medio": calcular_variacao_dados(tk_at, tk_bt)
            },
            "analise_temporal": {
                "comparativo_por_hora": comparativo_hora,
                "pico_vendas_atual": max(vendas_por_hora_atual.items(), key=lambda x: x[1], default=None),
            },
            "analise_produtos": {
                "top_10_mais_vendidos": produtos_mais_vendidos,
                "total_produtos_unicos": produtos_unicos_count
            },
            "analise_filiais": analise_filiais if not filial else None,
        }

        try:
            from main import cache
            timeout_cache = {0: 300, 1: 600, 2: 1800, 3: 3600, 4: 7200, 5: 14400}
            cache.set(cache_key, dados_ia, timeout=timeout_cache.get(periodo, 1800))
        except ImportError:
            pass

        return {"success": True, "data": dados_ia, "from_cache": False}

    except Exception as e:
        print(f"Erro ao gerar dados IA: {str(e)}")
        return {"success": False, "error": f"Erro ao gerar dados para IA: {str(e)}"}


def calcular_stats_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior):
    total_atual = total_anterior = 0.0
    recibos_atual = recibos_anterior = 0
    itens_atual = itens_anterior = 0
    
    for fatura in faturas_completas:
        data_fatura = fatura.get('data')
        if not data_fatura:
            continue
            
        if isinstance(data_fatura, str):
            from datetime import datetime
            data_fatura = datetime.strptime(data_fatura, '%Y-%m-%d').date()
        
        total_fatura = float(fatura.get('total', 0))
        itens_fatura = sum(item.get('quantity', 0) for item in (fatura.get('itens') or []))
        
        if data_inicio <= data_fatura <= data_fim:
            total_atual += total_fatura
            recibos_atual += 1
            itens_atual += itens_fatura
        elif data_inicio_anterior <= data_fatura <= data_fim_anterior:
            total_anterior += total_fatura
            recibos_anterior += 1
            itens_anterior += itens_fatura
    
    ticket_atual = round(total_atual / recibos_atual, 2) if recibos_atual else 0.0
    ticket_anterior = round(total_anterior / recibos_anterior, 2) if recibos_anterior else 0.0
    
    stats_atual = (total_atual, recibos_atual, itens_atual, ticket_atual)
    stats_anterior = (total_anterior, recibos_anterior, itens_anterior, ticket_anterior)
    
    return stats_atual, stats_anterior

def agrupar_por_hora_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior):
    vendas_por_hora_atual = defaultdict(float)
    vendas_por_hora_anterior = defaultdict(float)
    
    for fatura in faturas_completas:
        data_fatura = fatura.get('data')
        if not data_fatura:
            continue
            
        if isinstance(data_fatura, str):
            from datetime import datetime
            data_fatura = datetime.strptime(data_fatura, '%Y-%m-%d').date()
        
        hora_val = fatura.get('hora')
        if not hora_val:
            continue
            
        try:
            if isinstance(hora_val, str):
                hora = int(hora_val.split(":")[0])
            else: # datetime.time
                hora = hora_val.hour
                
            total_fatura = float(fatura.get('total', 0))
            
            if data_inicio <= data_fatura <= data_fim:
                vendas_por_hora_atual[hora] += total_fatura
            elif data_inicio_anterior <= data_fatura <= data_fim_anterior:
                vendas_por_hora_anterior[hora] += total_fatura
                
        except (ValueError, AttributeError):
            continue
    
    return dict(vendas_por_hora_atual), dict(vendas_por_hora_anterior)

def processar_faturas_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior):
    stats_atual, stats_anterior = calcular_stats_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior)
    vendas_por_hora_atual, vendas_por_hora_anterior = agrupar_por_hora_otimizado(faturas_completas, data_inicio, data_fim, data_inicio_anterior, data_fim_anterior)
    comparativo_por_hora = gerar_comparativo_por_hora(vendas_por_hora_atual, vendas_por_hora_anterior)
    
    faturas_atual = []
    faturas_anterior = []
    
    for fatura in faturas_completas:
        data_fatura = fatura.get('data')
        if data_fatura:
            if isinstance(data_fatura, str):
                from datetime import datetime
                data_fatura = datetime.strptime(data_fatura, '%Y-%m-%d').date()
            
            if data_inicio <= data_fatura <= data_fim:
                faturas_atual.append(fatura)
            elif data_inicio_anterior <= data_fatura <= data_fim_anterior:
                faturas_anterior.append(fatura)
    
    return {
        'stats_atual': stats_atual,
        'stats_anterior': stats_anterior,
        'vendas_por_hora_atual': vendas_por_hora_atual,
        'vendas_por_hora_anterior': vendas_por_hora_anterior,
        'comparativo_por_hora': comparativo_por_hora,
        'faturas_atual': faturas_atual,
        'faturas_anterior': faturas_anterior
    }
