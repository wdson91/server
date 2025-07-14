#!/usr/bin/env python3
"""
Script para iniciar o Celery worker, beat scheduler e Flower
"""
import os
import sys
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

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

def main():
    """Função principal"""
    print("🔧 Iniciando serviços Celery e Flower...")
    
    # Verificar se Redis está disponível
    redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
    print(f"📡 Conectando ao Redis: {redis_url}")
    
    # Iniciar worker
    worker_process = start_celery_worker()
    if not worker_process:
        print("❌ Falha ao iniciar worker. Saindo...")
        sys.exit(1)
    
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
    
    print("✅ Todos os serviços iniciados com sucesso!")
    print("📋 Serviços disponíveis:")
    print("   - Celery Worker: Processando tarefas")
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