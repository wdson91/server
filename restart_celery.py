#!/usr/bin/env python3
"""
Script para reiniciar o Celery com as tarefas corretas
"""
import os
import sys
import subprocess
import time
import signal
from dotenv import load_dotenv

load_dotenv()

def kill_celery_processes():
    """Mata processos Celery existentes"""
    print("🛑 Parando processos Celery existentes...")
    
    try:
        # Buscar processos Celery
        result = subprocess.run(['pgrep', '-f', 'celery'], capture_output=True, text=True)
        
        if result.stdout:
            pids = result.stdout.strip().split('\n')
            for pid in pids:
                if pid:
                    try:
                        os.kill(int(pid), signal.SIGTERM)
                        print(f"✅ Processo {pid} terminado")
                    except Exception as e:
                        print(f"⚠️  Erro ao terminar processo {pid}: {e}")
        else:
            print("ℹ️  Nenhum processo Celery encontrado")
            
    except Exception as e:
        print(f"⚠️  Erro ao buscar processos: {e}")

def start_celery_worker():
    """Inicia o Celery worker"""
    print("🚀 Iniciando Celery worker...")
    try:
        # Comando para iniciar o worker
        cmd = [
            "celery", "-A", "celery_config.celery_app", 
            "worker", "--loglevel=info", "--concurrency=2"
        ]
        
        process = subprocess.Popen(cmd)
        print(f"✅ Celery worker iniciado com PID: {process.pid}")
        return process
    except Exception as e:
        print(f"❌ Erro ao iniciar Celery worker: {e}")
        return None

def start_celery_beat():
    """Inicia o Celery beat scheduler"""
    print("⏰ Iniciando Celery beat scheduler...")
    try:
        # Comando para iniciar o beat
        cmd = [
            "celery", "-A", "celery_config.celery_app", 
            "beat", "--loglevel=info"
        ]
        
        process = subprocess.Popen(cmd)
        print(f"✅ Celery beat iniciado com PID: {process.pid}")
        return process
    except Exception as e:
        print(f"❌ Erro ao iniciar Celery beat: {e}")
        return None

def start_flower():
    """Inicia o Flower para monitoramento"""
    print("🌸 Iniciando Flower (monitoramento Celery)...")
    try:
        # Comando para iniciar o Flower
        cmd = [
            "celery", "-A", "celery_config.celery_app", 
            "flower", "--port=5555", "--broker=redis://localhost:6379/0"
        ]
        
        process = subprocess.Popen(cmd)
        print(f"✅ Flower iniciado com PID: {process.pid}")
        print("🌐 Interface web disponível em: http://localhost:5555")
        return process
    except Exception as e:
        print(f"❌ Erro ao iniciar Flower: {e}")
        return None

def test_tasks():
    """Testa se as tarefas estão registradas"""
    print("🔍 Testando registro de tarefas...")
    try:
        from celery_config import celery_app
        
        # Verificar tarefas registradas
        registered_tasks = list(celery_app.tasks.keys())
        
        required_tasks = [
            'tasks.download_and_queue_sftp_files',
            'tasks.process_single_xml_file',
            'tasks.process_sftp_files'
        ]
        
        missing_tasks = []
        for task_name in required_tasks:
            if task_name in registered_tasks:
                print(f"✅ {task_name}")
            else:
                print(f"❌ {task_name}")
                missing_tasks.append(task_name)
        
        if missing_tasks:
            print(f"\n⚠️  Tarefas faltando: {missing_tasks}")
            return False
        else:
            print("\n✅ Todas as tarefas estão registradas!")
            return True
            
    except Exception as e:
        print(f"❌ Erro ao testar tarefas: {e}")
        return False

def main():
    """Função principal"""
    print("🔧 Reiniciando Celery com tarefas corretas")
    print("=" * 50)
    
    # Testar tarefas antes
    print("📋 Testando tarefas antes do restart...")
    test_tasks()
    
    # Parar processos existentes
    kill_celery_processes()
    
    # Aguardar um pouco
    time.sleep(2)
    
    # Testar tarefas novamente
    print("\n📋 Testando tarefas após parar processos...")
    test_tasks()
    
    # Iniciar worker
    worker_process = start_celery_worker()
    if not worker_process:
        print("❌ Falha ao iniciar worker. Saindo...")
        sys.exit(1)
    
    # Aguardar worker inicializar
    time.sleep(3)
    
    # Testar tarefas após iniciar worker
    print("\n📋 Testando tarefas após iniciar worker...")
    test_tasks()
    
    # Aguardar um pouco antes de iniciar o beat
    time.sleep(2)
    
    # Iniciar beat
    beat_process = start_celery_beat()
    if not beat_process:
        print("❌ Falha ao iniciar beat. Saindo...")
        worker_process.terminate()
        sys.exit(1)
    
    # Aguardar um pouco antes de iniciar o Flower
    time.sleep(2)
    
    # Iniciar Flower
    flower_process = start_flower()
    if not flower_process:
        print("❌ Falha ao iniciar Flower. Saindo...")
        worker_process.terminate()
        beat_process.terminate()
        sys.exit(1)
    
    print("\n✅ Todos os serviços iniciados com sucesso!")
    print("📋 Serviços disponíveis:")
    print("   - Celery Worker: Processando tarefas individuais")
    print("   - Celery Beat: Agendando tarefas a cada 5 minutos")
    print("   - Flower: http://localhost:5555 (monitoramento)")
    print("📋 Para parar os serviços, pressione Ctrl+C")
    
    try:
        # Manter os processos rodando
        while True:
            time.sleep(1)
            
            # Verificar se os processos ainda estão rodando
            if worker_process.poll() is not None:
                print("❌ Celery worker parou inesperadamente")
                break
                
            if beat_process.poll() is not None:
                print("❌ Celery beat parou inesperadamente")
                break
                
            if flower_process.poll() is not None:
                print("❌ Flower parou inesperadamente")
                break
                
    except KeyboardInterrupt:
        print("\n🛑 Parando serviços...")
        
        # Terminar processos
        if worker_process:
            worker_process.terminate()
            print("✅ Celery worker parado")
            
        if beat_process:
            beat_process.terminate()
            print("✅ Celery beat parado")
            
        if flower_process:
            flower_process.terminate()
            print("✅ Flower parado")
            
        print("👋 Serviços parados com sucesso!")

if __name__ == "__main__":
    main() 