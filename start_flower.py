#!/usr/bin/env python3
"""
Script para iniciar apenas o Flower (monitoramento do Celery)
"""
import os
import sys
import subprocess
import time
from dotenv import load_dotenv

load_dotenv()

def start_flower():
    """Inicia o Flower para monitoramento"""
    print("🌸 Iniciando Flower (monitoramento Celery)...")
    print("📡 Conectando ao Redis...")
    
    try:
        # Verificar se Redis está disponível
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url)
        r.ping()
        print("✅ Redis conectado")
        
        # Comando para iniciar o Flower
        cmd = [
            "celery", "-A", "celery_config.celery_app", 
            "flower", 
            "--port=5555", 
            "--broker=redis://localhost:6379/0",
            "--broker_api=redis://localhost:6379/0"
        ]
        
        process = subprocess.Popen(cmd)
        print(f"✅ Flower iniciado com PID: {process.pid}")
        print("🌐 Interface web disponível em: http://localhost:5555")
        print("📋 Para parar o Flower, pressione Ctrl+C")
        
        try:
            # Manter o processo rodando
            while True:
                time.sleep(1)
                
                # Verificar se o processo ainda está rodando
                if process.poll() is not None:
                    print("❌ Flower parou inesperadamente")
                    break
                    
        except KeyboardInterrupt:
            print("\n🛑 Parando Flower...")
            process.terminate()
            print("✅ Flower parado com sucesso!")
            
    except Exception as e:
        print(f"❌ Erro ao iniciar Flower: {e}")
        print("🔧 Verifique se:")
        print("   - Redis está rodando")
        print("   - Celery worker está ativo")
        print("   - Porta 5555 está livre")
        return False

def main():
    """Função principal"""
    print("🔧 Iniciando Flower - Monitoramento do Celery")
    print("=" * 50)
    
    success = start_flower()
    
    if not success:
        sys.exit(1)

if __name__ == "__main__":
    main() 