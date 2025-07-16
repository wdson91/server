from flask import Flask, request, jsonify,send_file
from datetime import datetime, date, timedelta
import os
import pytz
from flask_cors import CORS
from dotenv import load_dotenv
from flask_caching import Cache

# Importar Celery
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'celery'))
from celery_config import celery_app
from tasks import download_and_queue_sftp_files, process_single_xml_file, download_and_queue_opengcs_files, process_single_opengcs_file

from utils.supabaseUtil import get_supabase

# Configuração
load_dotenv()
app = Flask(__name__)
CORS(app)


app.config.from_mapping({
    'CACHE_TYPE': 'RedisCache',
    'CACHE_REDIS_URL': os.getenv('REDIS_URL'),
    'CACHE_DEFAULT_TIMEOUT': 180,
})
cache = Cache(app)
supabase = get_supabase()
TZ = pytz.timezone('Europe/Lisbon')

# Endpoint para baixar arquivos SFTP e criar tarefas individuais
@app.route('/api/download-sftp-queue', methods=['POST'])
def trigger_sftp_download_and_queue():
    """Endpoint para baixar arquivos SFTP e criar tarefas individuais no Celery"""
    try:
        # Executar tarefa Celery
        task = download_and_queue_sftp_files.delay()
        return jsonify({
            "status": "success",
            "message": "Download SFTP iniciado e tarefas sendo criadas",
            "task_id": task.id
        }), 202
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao iniciar download SFTP: {str(e)}"
        }), 500

# Endpoint para processar arquivos SFTP manualmente (mantido para compatibilidade)
@app.route('/api/process-sftp', methods=['POST'])
def trigger_sftp_processing():
    """Endpoint para processar arquivos SFTP manualmente"""
    try:
        # Executar tarefa Celery
        task = download_and_queue_sftp_files.delay()
        return jsonify({
            "status": "success",
            "message": "Processamento SFTP iniciado",
            "task_id": task.id
        }), 202
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao iniciar processamento: {str(e)}"
        }), 500

# Endpoint para baixar arquivos OpenGCs SFTP e criar tarefas individuais
@app.route('/api/download-opengcs-queue', methods=['POST'])
def trigger_opengcs_download_and_queue():
    """Endpoint para baixar arquivos OpenGCs SFTP e criar tarefas individuais no Celery"""
    try:
        # Executar tarefa Celery
        task = download_and_queue_opengcs_files.delay()
        return jsonify({
            "status": "success",
            "message": "Download OpenGCs SFTP iniciado e tarefas sendo criadas",
            "task_id": task.id
        }), 202
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao iniciar download OpenGCs SFTP: {str(e)}"
        }), 500

# Endpoint para processar arquivos OpenGCs manualmente
@app.route('/api/process-opengcs', methods=['POST'])
def trigger_opengcs_processing():
    """Endpoint para processar arquivos OpenGCs manualmente"""
    try:
        # Executar tarefa Celery
        task = download_and_queue_opengcs_files.delay()
        return jsonify({
            "status": "success",
            "message": "Processamento OpenGCs iniciado",
            "task_id": task.id
        }), 202
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao iniciar processamento OpenGCs: {str(e)}"
        }), 500

# Endpoint para verificar status de uma tarefa
@app.route('/api/task-status/<task_id>', methods=['GET'])
def get_task_status(task_id):
    """Endpoint para verificar status de uma tarefa Celery"""
    try:
        task = celery_app.AsyncResult(task_id)
        if task.state == 'PENDING':
            response = {
                'state': task.state,
                'current': 0,
                'total': 1,
                'status': 'Task is pending...'
            }
        elif task.state != 'FAILURE':
            response = {
                'state': task.state,
                'current': task.info.get('current', 0),
                'total': task.info.get('total', 1),
                'status': task.info.get('status', '')
            }
            if 'result' in task.info:
                response['result'] = task.info['result']
        else:
            # something went wrong in the background job
            response = {
                'state': task.state,
                'current': 1,
                'total': 1,
                'status': str(task.info),  # this is the exception raised
            }
        return jsonify(response)
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao verificar status: {str(e)}"
        }), 500

# Endpoint para processar um arquivo XML específico
@app.route('/api/process-xml-file', methods=['POST'])
def process_specific_xml_file():
    """Endpoint para processar um arquivo XML específico"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        
        if not file_path:
            return jsonify({
                "status": "error",
                "message": "file_path é obrigatório"
            }), 400
        
        if not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": "Arquivo não encontrado"
            }), 404
        
        # Executar tarefa Celery
        task = process_single_xml_file.delay(file_path)
        return jsonify({
            "status": "success",
            "message": "Processamento de arquivo XML iniciado",
            "task_id": task.id
        }), 202
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao processar arquivo: {str(e)}"
        }), 500

# Endpoint específico para processar arquivos NC (Nota de Crédito)
@app.route('/api/process-nc-file', methods=['POST'])
def process_nc_file_endpoint():
    """Endpoint para processar um arquivo NC específico"""
    try:
        data = request.get_json()
        file_path = data.get('file_path')
        
        if not file_path:
            return jsonify({
                "status": "error",
                "message": "file_path é obrigatório"
            }), 400
        
        if not os.path.exists(file_path):
            return jsonify({
                "status": "error",
                "message": "Arquivo não encontrado"
            }), 404
        
        # Verificar se é um arquivo NC
        filename = os.path.basename(file_path)
        if not filename.startswith('NC'):
            return jsonify({
                "status": "error",
                "message": "Arquivo deve começar com 'NC' para ser processado como Nota de Crédito"
            }), 400
        
        # Executar tarefa Celery
        task = process_single_xml_file.delay(file_path)
        return jsonify({
            "status": "success",
            "message": "Processamento de arquivo NC iniciado",
            "task_id": task.id
        }), 202
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao processar arquivo NC: {str(e)}"
        }), 500

# Endpoint para listar tarefas ativas
@app.route('/api/active-tasks', methods=['GET'])
def get_active_tasks():
    """Endpoint para listar tarefas ativas no Celery"""
    try:
        i = celery_app.control.inspect()
        active_tasks = i.active()
        
        if active_tasks:
            return jsonify({
                "status": "success",
                "active_tasks": active_tasks
            })
        else:
            return jsonify({
                "status": "success",
                "message": "Nenhuma tarefa ativa",
                "active_tasks": {}
            })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro ao listar tarefas: {str(e)}"
        }), 500

# Endpoint para limpeza manual das pastas
@app.route('/api/cleanup', methods=['POST'])
def manual_cleanup():
    """Endpoint para limpeza manual das pastas"""
    try:
        from tasks import cleanup_processed_files
        
        # Executar limpeza
        cleanup_processed_files()
        
        return jsonify({
            "status": "success",
            "message": "Limpeza manual executada com sucesso"
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": f"Erro na limpeza manual: {str(e)}"
        }), 500

# Endpoint para verificar saúde do sistema
@app.route('/api/health', methods=['GET'])
def health_check():
    """Endpoint para verificar saúde do sistema"""
    try:
        # Verificar conexão com Redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        import redis
        r = redis.from_url(redis_url)
        r.ping()
        
        # Verificar conexão com Supabase
        supabase = get_supabase()
        # Teste simples de conexão
        response = supabase.table("companies").select("company_id").limit(1).execute()
        
        return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": "connected",
                "supabase": "connected",
                "celery": "available"
            }
        }), 200
        
    except Exception as e:
        return jsonify({
            "status": "unhealthy",
            "timestamp": datetime.now().isoformat(),
            "error": str(e)
        }), 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8000)
