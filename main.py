import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'celery'))

from flask import Flask, request, jsonify,send_file
from datetime import datetime, date, timedelta
import os
import pytz
from flask_cors import CORS
from dotenv import load_dotenv
from flask_caching import Cache

# Importar Celery

from celery_config import celery_app
from tasks import download_and_queue_sftp_files, process_single_opengcs_file, download_and_queue_opengcs_files_sync
from sftp_upload import upload_xml_to_sftp

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


@app.route('/home', methods=['GET'])
def get():
    return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": "connected",
                "supabase": "connected",
                "celery": "available"
            }
        }), 200

from tasks import async_upload_xml_to_sftp
@app.route("/send-file", methods=["POST"])
def send_file():
    import xml.etree.ElementTree as ET
    import json as _json
    try:
        # Dados chegam como JSON: { "filename": "FR....xml", "xml": "<AuditFile>..." }
        # O Lua envia com encoding Windows-1252, por isso tentamos UTF-8 e depois latin-1
        raw = request.data
        try:
            body = raw.decode("utf-8")
        except UnicodeDecodeError:
            body = raw.decode("latin-1")
        try:
            data = _json.loads(body)
        except _json.JSONDecodeError:
            return jsonify({"status": "error", "message": "Body JSON inválido ou vazio"}), 400
        if not data:
            return jsonify({"status": "error", "message": "Body JSON inválido ou vazio"}), 400

        filename = data.get("filename", "")
        xml_string = data.get("xml", "")

        if not xml_string:
            return jsonify({"status": "error", "message": "Campo 'xml' é obrigatório"}), 400

        if not filename.lower().endswith(".xml"):
            return jsonify({"status": "error", "message": "O ficheiro deve ser XML"}), 400

        # Converter string para bytes e fazer parse do XML
        content = xml_string.encode("utf-8")
        size_mb = len(content) / (1024 * 1024)

        if size_mb > 2:
            return jsonify({
                "status": "error",
                "message": f"O ficheiro excede o limite de 2MB (tamanho: {size_mb:.2f}MB)"
            }), 413

        try:
            root = ET.fromstring(content)
        except ET.ParseError as parse_err:
            return jsonify({"status": "error", "message": f"XML inválido: {str(parse_err)}"}), 400

        # Detectar tipo de ficheiro e extrair NIF
        is_opengcs = filename.lower().startswith("opengcs")
        if is_opengcs:
            # Formato: opengcs-{NIF}-{nome}.xml  →  extrair NIF do filename
            parts = filename.split("-")
            if len(parts) < 2:
                return jsonify({"status": "error", "message": "Filename OpenGCs inválido, formato esperado: opengcs-{NIF}-{nome}.xml"}), 400
            nif = parts[1]
        else:
            # SAF-T PT: extrair NIF do CompanyID dentro do XML
            company_id_el = root.find(".//{*}CompanyID")
            if company_id_el is None:
                return jsonify({"status": "error", "message": "CompanyID (NIF) não encontrado no XML"}), 422
            nif = company_id_el.text

        # Enviar XML para o SFTP na pasta do NIF
        sftp_result = None
        sftp_error = None
        try:
            # Enviamos a string limpa para não ter problemas de serialização
            async_upload_xml_to_sftp.delay(
                xml_string=xml_string, 
                nif=nif, 
                filename=filename or f"upload_{nif}_assincrono.xml"
            )
            print("Ficheiro enviado para fila em background")   
            sftp_status_msg = "Enviado para fila em background"
        except Exception as celery_err:
            sftp_status_msg = f"Falha ao enviar para Celery: {str(celery_err)}"
        response = {
            "status": "success",
            "message": "Ficheiro recebido na API e na fila de processamento",
            "nif": nif,
            "size_mb": round(size_mb, 4),
            "sftp_info": sftp_status_msg
        }
        
        return jsonify(response), 200


    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500



@app.route("/upload-saft-chunk", methods=["POST"])
def upload_saft_chunk():
    import json as _json
    import xml.etree.ElementTree as ET
    from werkzeug.utils import secure_filename
    
    try:
        raw = request.data
        try:
            body = raw.decode("utf-8")
        except UnicodeDecodeError:
            body = raw.decode("latin-1")
            
        try:
            data = _json.loads(body)
        except _json.JSONDecodeError:
            return jsonify({"status": "error", "message": "Body JSON inválido"}), 400

        filename = data.get("filename", "")
        nif = data.get("nif", "")
        chunk_index = int(data.get("chunk_index", 1))
        total_chunks = int(data.get("total_chunks", 1))
        chunk_data = data.get("data", "")
        
        if not filename or not nif or not chunk_data:
            return jsonify({"status": "error", "message": "Campos obrigatórios: filename, nif, e data."}), 400

        safe_filename = secure_filename(filename)
        upload_dir = "/tmp/saft_uploads"
        
        if not os.path.exists(upload_dir):
            os.makedirs(upload_dir, exist_ok=True)
            
        file_path = os.path.join(upload_dir, safe_filename)
        chunk_file = f"{file_path}.part{chunk_index}"
        
        # Write the chunk to a temporary file
        content = chunk_data.encode("utf-8")
        with open(chunk_file, "wb") as f:
            f.write(content)
            
        # Check if this is the last chunk
        if chunk_index == total_chunks:
            # All chunks have been uploaded. Assemble them.
            assembled_content = b""
            for i in range(1, total_chunks + 1):
                part_file = f"{file_path}.part{i}"
                if not os.path.exists(part_file):
                    return jsonify({"status": "error", "message": f"Chunk {i} em falta."}), 400
                with open(part_file, "rb") as f:
                    assembled_content += f.read()
                    
            size_mb = len(assembled_content) / (1024 * 1024)
            
            # Verify valid XML
            try:
                ET.fromstring(assembled_content)
            except ET.ParseError as parse_err:
                return jsonify({"status": "error", "message": f"XML inválido reconstruído: {str(parse_err)}"}), 400
            
            # Clean up the parts
            for i in range(1, total_chunks + 1):
                try:
                    os.remove(f"{file_path}.part{i}")
                except Exception:
                    pass
                    
            # Upload to SFTP
            sftp_result = None
            sftp_error = None
            try:
                sftp_result = upload_xml_to_sftp(
                    xml_content=assembled_content,
                    nif=nif,
                    filename=filename
                )
            except Exception as sftp_err:
                sftp_error = str(sftp_err)

            response = {
                "status": "success",
                "message": "Ficheiro completo montado e recebido com sucesso",
                "nif": nif,
                "size_mb": round(size_mb, 4),
            }
            if sftp_result:
                response["sftp"] = sftp_result
            if sftp_error:
                response["sftp_warning"] = f"Upload SFTP falhou: {sftp_error}"

            return jsonify(response), 200

        else:
            return jsonify({
                "status": "success", 
                "message": f"Chunk {chunk_index} de {total_chunks} guardado com sucesso."
            }), 200

    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500







def ensure_redis_running():
    import redis
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    try:
        r = redis.from_url(redis_url)
        r.ping()
        print("✅ Redis conectado com sucesso!")
    except redis.ConnectionError:
        print("❌ AVISO CRÍTICO: Não foi possível conectar ao Redis.")
        print("Certifique-se de que o servidor Redis está a correr (ex: sudo service redis-server start)")
        print("Algumas funcionalidades que dependem do Celery em background poderão falhar.")
        # sys.exit(1) # Opcional: descomente se quiser impedir o Flask de iniciar sem Redis


@app.route('/home', methods=['GET'])
def get():
    return jsonify({
            "status": "healthy",
            "timestamp": datetime.now().isoformat(),
            "services": {
                "redis": "connected",
                "supabase": "connected",
                "celery": "available"
            }
        }), 200
if __name__ == "__main__":
    ensure_redis_running()
    app.run(debug=True, host="0.0.0.0", port=8000)
