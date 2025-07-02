from flask import Flask, request, jsonify,send_file
from datetime import datetime, date, timedelta
from collections import defaultdict
import os
import pytz
from flask_cors import CORS
from dotenv import load_dotenv
from flask_caching import Cache
from threading import Thread
import requests

from decorator import require_valid_token
from utils.supabaseUtil import get_supabase
from utils.parse_faturas import parse_faturas
from getFaturas import get_faturas
from utils.utils import is_valid_nif, get_periodo_datas, buscar_faturas_periodo, parse_periodo, calcular_stats, agrupar_por_hora, gerar_comparativo_por_hora, limpar_cache_por_nif , calcular_variacao_dados


# Configuração
load_dotenv()
app = Flask(__name__)
CORS(app)
app.config.from_mapping({
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_URL': os.getenv('REDIS_URL', 'redis://localhost:6379/0'),
    'CACHE_DEFAULT_TIMEOUT': 180,
})
cache = Cache(app)
supabase = get_supabase()
TZ = pytz.timezone('Europe/Lisbon')

# Helpers

def current_time_str(fmt='%H:%M'):
    return datetime.now(TZ).strftime(fmt)

def cache_key():
    nif = request.args.get('nif', '')
    periodo = request.args.get('periodo', '0')
    return f"{request.path}/{nif}/{periodo}"

def precache_essenciais(nif, token):
    base = 'http://localhost:8000/api'
    endpoints = [f"{base}/{path}?nif={nif}{'&periodo='+str(p) if 'products' in path else ''}" 
                 for path, p in [('stats', None), ('stats/resumo', 0), ('products', 0), ('products', 1)]]
    headers = {'Authorization': f'Bearer {token}'}
    for url in endpoints:
        try:
            requests.get(url, headers=headers, timeout=10)
        except: pass
    cache.set(f'ultima_atualizacao:{nif}', datetime.now(TZ).strftime('%d-%m %H:%M'))

# Endpoints

@app.route('/api/stats', methods=['GET'])
@require_valid_token
def relatorio():
    return get_faturas()

@app.route('/api/stats/today', methods=['GET'])
@require_valid_token
@cache.cached(timeout=180, key_prefix=cache_key)
def stats():
    nif = request.args.get('nif', '')
    if not nif.isdigit():
        return jsonify({'error': 'NIF é obrigatório e deve conter apenas números'}), 400

    hoje = date.today()
    # query faturas
    res = supabase.table('faturas_fatura').select('*, itens:faturas_itemfatura(*)') \
        .eq('nif', nif).eq('data', hoje.isoformat()).execute()
    faturas = [f for f in (res.data or []) if str(f.get('nif')) == nif]

    total_vendas = sum(float(f['total']) for f in faturas)
    total_itens = sum(item['quantidade'] for f in faturas for item in f.get('itens', []))
    produtos = sorted(({'produto': k, 'quantidade': v} for k, v in 
                       defaultdict(int, ((item['nome'], item['quantidade'])
                        for f in faturas for item in f.get('itens', []))).items()),
                      key=lambda x: x['quantidade'], reverse=True)

    vendas_por_hora = defaultdict(float)
    for f in faturas:
        h = f.get('hora', '')[:2] + ':00'
        vendas_por_hora[h] += float(f['total']) if f.get('hora') else 0
    base = {'08:00', '12:00', '18:00'} | set(vendas_por_hora)
    vendas_horarias = [{'hora': h, 'total': round(vendas_por_hora.get(h, 0), 2)} for h in sorted(base)]

    # últimos 7 dias
    ontem = hoje - timedelta(days=1)
    inicio = hoje - timedelta(days=7)
    res7 = supabase.table('faturas_fatura').select('data, total') \
        .gte('data', inicio.isoformat()).lte('data', ontem.isoformat()).execute()
    vendas7 = defaultdict(float)
    for f in res7.data or []:
        vendas7[f['data']] += float(f['total'])
    ult7 = [{'data': d, 'total': round(t, 2)} for d, t in sorted(vendas7.items())]

    resultado = {'dados': {
        'total_vendas': round(total_vendas, 2),
        'total_itens': total_itens,
        'vendas_por_dia': [{'data': str(hoje), 'total': round(total_vendas, 2)}],
        'vendas_por_hora': vendas_horarias,
        'vendas_por_produto': produtos,
        'quantidade_faturas': len(faturas),
        'filtro_data': str(hoje),
        'ultima_atualizacao': current_time_str(),
        'ultimos_7_dias': ult7,
        'total_ultimos_7_dias': round(sum(v['total'] for v in ult7), 2)
    }}
    return jsonify(resultado), 200

@app.route('/api/stats/report', methods=['GET'])
@require_valid_token
@cache.cached(timeout=180, key_prefix=cache_key)
def report():
    nif = request.args.get('nif', '')
    if not nif:
        return jsonify({'error': 'NIF é obrigatório'}), 400
    hoje = date.today()
    ontem = hoje - timedelta(days=1)
    inicio = hoje - timedelta(days=7)

    fetch = lambda q: (supabase.table('faturas_fatura').select('*')
                       .eq(*q).execute().data or [])
    fh = fetch(('data', hoje.isoformat())), fetch(('gte', inicio.isoformat()), ('lte', ontem.isoformat()))
    f_hoje, f_7d = fh

    def agg(fats):
        d = defaultdict(lambda: {'volume': 0.0, 'quantidade': 0})
        for f in fats:
            h = f.get('hora', '')[:2] + ':00'
            d[h]['volume'] += float(f.get('total', 0))
            d[h]['quantidade'] += 1
        return d
    t_hoje, t_7d = agg(f_hoje), agg(f_7d)
    hrs = sorted(set(t_hoje) | set(t_7d))
    vp = [{'hora': h, 'faturas_hoje': t_hoje[h]['quantidade'], 'volume_hoje': round(t_hoje[h]['volume'],2),
           'faturas_7_dias': t_7d[h]['quantidade'], 'volume_7_dias': round(t_7d[h]['volume'],2)} for h in hrs]

    vendas_dia = [{'data': hoje.isoformat(), 'total': round(sum(float(f['total']) for f in f_hoje),2)}]
    v7d = defaultdict(float)
    for f in f_7d: v7d[f['data']] += float(f.get('total',0))
    ult7 = [{'data': d, 'total': round(t,2)} for d,t in sorted(v7d.items())]

    resp = {'dados': {
        'total_vendas': round(sum(float(f['total']) for f in f_hoje),2),
        'quantidade_faturas': len(f_hoje),
        'vendas_por_dia': vendas_dia,
        'vendas_por_hora': vp,
        'filtro_data': hoje.isoformat(),
        'ultima_atualizacao': current_time_str(),
        'ultimos_7_dias': ult7,
        'total_ultimos_7_dias': round(sum(x['total'] for x in ult7),2),
        'quantidade_faturas_7_dias': len(f_7d)
    }}
    cache.set(f'ultima_atualizacao:{nif}', datetime.now(TZ).strftime('%d-%m %H:%M'))
    return jsonify(resp)

@app.route('/api/products', methods=['GET'])
@require_valid_token
@cache.cached(timeout=180, key_prefix=cache_key)
def products():
    nif = request.args.get('nif', '')
    if not is_valid_nif(nif):
        return jsonify({'error': 'NIF é obrigatório e deve conter apenas números'}), 400
    try:
        p = int(request.args.get('periodo', 0))
    except:
        return jsonify({'error': 'Período inválido.'}), 400

    try:
        di, df, dia, df_an = get_periodo_datas(p)
    except ValueError as e:
        return jsonify({'error': str(e)}), 400

    fats = buscar_faturas_periodo(nif, di, df)
    cont = defaultdict(lambda: {'quantidade': 0, 'montante': 0.0})
    ti = mi = 0
    for f in fats:
        for it in f.get('itens', []):
            cont[it['nome']]['quantidade'] += it['quantidade']
            cont[it['nome']]['montante'] += it['quantidade']*float(it['preco_unitario'])
            ti += it['quantidade']; mi += it['quantidade']*float(it['preco_unitario'])

    itens = sorted([
        {'produto': k, 'quantidade': d['quantidade'], 'montante': round(d['montante'],2),
         'porcentagem_montante': round(d['montante']/mi*100,2) if mi else 0.0}
        for k,d in cont.items()], key=lambda x:x['montante'], reverse=True)

    result = {'periodo': parse_periodo(p), 'data_inicio': str(di), 'data_fim': str(df),
              'total_itens': ti, 'total_montante': round(mi,2), 'itens': itens}
    cache.set(f'ultima_atualizacao:{nif}', datetime.now(TZ).strftime('%d-%m %H:%M'))
    return jsonify(result), 200

@app.route('/api/limparcache', methods=['DELETE'])
@require_valid_token
def limpar_cache():
    nif = request.args.get('nif', '')
    if not nif:
        return jsonify({'error': 'NIF é obrigatório'}), 400
    try:
        limpar_cache_por_nif(nif)
        token = request.headers.get('Authorization','').replace('Bearer ','')
        Thread(target=precache_essenciais, args=(nif, token)).start()
        return jsonify({'message': 'Cache limpo e atualização em background iniciada'}), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/ultima-atualizacao', methods=['GET'])
@require_valid_token
def ultima_atualizacao():
    nif = request.args.get('nif', '')
    if not nif:
        return jsonify({'error': 'NIF é obrigatório'}), 400
    val = cache.get(f'ultima_atualizacao:{nif}') or datetime.now(TZ).strftime('%d-%m %H:%M')
    return jsonify({'nif': nif, 'ultima_atualizacao': val}), 200

@app.route('/api/stats/resumo', methods=['GET'])
def resumo_stats():
    nif = request.args.get('nif','')
    try: p = int(request.args.get('periodo','0'))
    except: return jsonify({'error':'Período inválido'}),400
    if not is_valid_nif(nif): return jsonify({'error':'NIF inválido'}),400
    try:
        di, df, dia, dfan = get_periodo_datas(p)
    except ValueError as e:
        return jsonify({'error':str(e)}),400
    fa = buscar_faturas_periodo(nif,di,df)
    fb = buscar_faturas_periodo(nif,dia,dfan)
    total_at, rec_at, it_at, tk_at = calcular_stats(fa)
    total_bt, rec_bt, it_bt, tk_bt = calcular_stats(fb)
    v_at = agrupar_por_hora(fa)
    v_bt = agrupar_por_hora(fb)
    comp = gerar_comparativo_por_hora(v_at, v_bt)
    data = {'periodo': parse_periodo(p), 'total_vendas': calcular_variacao_dados(total_at,total_bt),
            'numero_recibos': calcular_variacao_dados(rec_at,rec_bt), 'itens_vendidos': calcular_variacao_dados(it_at,it_bt),
            'ticket_medio': calcular_variacao_dados(tk_at,tk_bt), 'comparativo_por_hora': comp}
    cache.set(f'ultima_atualizacao:{nif}', datetime.now(TZ).strftime('%d-%m %H:%M'))
    return jsonify({'dados':data}),200

@app.route('/api/upload-fatura', methods=['POST'])
def upload_fatura():
    if 'file' not in request.files:
        return jsonify({'erro':'Arquivo não enviado'}),400
    f = request.files['file']
    if not f.filename:
        return jsonify({'erro':'Arquivo sem nome'}),400
    try:
        text = f.read().decode('utf-8')
    except UnicodeDecodeError:
        return jsonify({'erro':'Arquivo com codificação inválida. Use UTF-8'}),400
    try:
        fats = parse_faturas(text)
        if not fats:
            return jsonify({'erro':'Nenhuma fatura válida encontrada no arquivo'}),400
    except Exception as e:
        return jsonify({'erro':str(e)}),400
    criadas, erros = [], []
    for fa in fats:
        if any(not fa.get(c) for c in ['numero_fatura','data','hora','total','nif_emitente','nif_cliente','itens']):
            erros.append({'numero_fatura':fa.get('numero_fatura','desconhecido'),'erro':'Campos obrigatórios faltando'})
            continue
        nf = fa['numero_fatura'].replace('/','_').replace(' ','')
        now = datetime.utcnow().isoformat()
        try:
            res = supabase.table('faturas_fatura').insert({
                'numero_fatura':nf,'data':fa['data'],'hora':fa['hora'],'total':float(fa['total']),
                'texto_original':fa['texto_original'],'texto_completo':fa['texto_completo'],'qrcode':fa['qrcode'],
                'nif':fa['nif_emitente'],'nif_cliente':fa['nif_cliente'],'criado_em':now,'atualizado_em':now
            }).execute()
            if not res.data:
                raise ValueError('Falha ao inserir fatura no banco de dados')
            fid = res.data[0]['id']
            for it in fa['itens']:
                supabase.table('faturas_itemfatura').insert({
                    'fatura_id':fid,'nome':it['nome'],'quantidade':it['quantidade'],
                    'preco_unitario':float(it['preco_unitario']),'total':float(it['total'])
                }).execute()
            criadas.append(res.data[0])
        except Exception as e:
            msg = str(e)
            if '23505' in msg:
                erros.append({'numero_fatura':nf,'erro':msg})
            else:
                erros.append({'numero_fatura':nf,'erro':msg})
    status = 201 if criadas else 400
    return jsonify({'mensagem':f'{len(criadas)} fatura(s) processada(s) com sucesso','faturas':criadas,'erros':erros}),status



@app.route("/api/faturas", methods=["GET"])
@require_valid_token
def buscar_faturas_periodo_route():
    nif = request.args.get("nif")
    periodo_raw = request.args.get("periodo", "0")

    if not nif or not nif.isdigit():
        return jsonify({"error": "NIF é obrigatório e deve conter apenas números"}), 400

    try:
        periodo = int(periodo_raw)
    except ValueError:
        return jsonify({"error": "Período inválido. Deve ser um número inteiro."}), 400

    try:
        data_inicio, data_fim , inicio_anterior, fim_anterior  = get_periodo_datas(periodo)
        print(f"Período selecionado: {data_inicio} a {data_fim}")
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    # Consulta no Supabase
    result = supabase.table("faturas_fatura") \
        .select("id, data, total,numero_fatura,hora ") \
        .eq("nif", nif) \
        .gte("data", data_inicio.isoformat()) \
        .lte("data", data_fim.isoformat()) \
        .execute()

    faturas = result.data or []
    
    if not faturas:
        return jsonify({"message": "Nenhuma fatura encontrada para esse período."}), 404

    return jsonify({"faturas": faturas, "periodo": {"inicio": str(data_inicio), "fim": str(data_fim)}})


from utils.gerarPdf import gerar_pdf
@app.route('/api/faturas/pdf', methods=['GET'])
def baixar_fatura_pdf():
    # Busca no Supabase pelo número da fatura

    numero_fatura = request.args.get('numero_fatura', '').strip()
    
    response = supabase.table("faturas_fatura") \
        .select("texto_completo, qrcode") \
        .eq("numero_fatura", numero_fatura) \
        .single() \
        .execute()

    
    
    fatura = response.data
    if not fatura:
        return jsonify({"error": "Fatura não encontrada"}), 404

    texto_completo = fatura.get("texto_completo")
    if not texto_completo:
        return jsonify({"error": "Texto completo da fatura não encontrado"}), 404
    qrcode = fatura.get("qrcode")
    pdf_buffer = gerar_pdf(texto_completo,qrcode)

    return send_file(
        pdf_buffer,
        as_attachment=True,
        download_name=f'fatura_{numero_fatura}.pdf',
        mimetype='application/pdf'
    )


@app.route("/api/faturas/todas", methods=["GET"])
@require_valid_token
def buscar_todas_faturas():
    nif = request.args.get("nif")  # Obtém o NIF do query param
    
    if not nif or not nif.isdigit():
        return jsonify({"error": "NIF é obrigatório e deve conter apenas números"}), 400

    # Consulta no Supabase (sem filtro de data)
    result = supabase.table("faturas_fatura") \
        .select("numero_fatura, total, hora, data") \
        .eq("nif", nif) \
        .order("data", desc=True) \
        .execute()

    faturas = result.data or []
    
    if not faturas:
        return jsonify({"message": "Nenhuma fatura encontrada para este NIF."}), 404

    return jsonify({"faturas": faturas, "total": len(faturas)})


if __name__ == '__main__':
    app.run(host='0.0.0.0',port=8000,debug=True)
