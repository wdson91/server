#!/usr/bin/env python3
"""
Script para testar se as tarefas Celery estão registradas corretamente
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def test_celery_tasks():
    """Testa se as tarefas Celery estão registradas"""
    print("🔍 Testando registro de tarefas Celery...")
    
    try:
        # Importar configuração do Celery
        from ..celery.celery_config import celery_app
        
        # Verificar tarefas registradas
        registered_tasks = celery_app.tasks.keys()
        
        print("📋 Tarefas registradas:")
        for task_name in registered_tasks:
            print(f"  - {task_name}")
        
        # Verificar tarefas específicas
        required_tasks = [
            'tasks.download_and_queue_sftp_files',
            'tasks.process_single_xml_file',
            'tasks.process_sftp_files'
        ]
        
        missing_tasks = []
        for task_name in required_tasks:
            if task_name in registered_tasks:
                print(f"✅ {task_name} - REGISTRADA")
            else:
                print(f"❌ {task_name} - NÃO REGISTRADA")
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

def test_celery_imports():
    """Testa se os imports estão funcionando"""
    print("\n🔍 Testando imports...")
    
    try:
        # Testar import das tarefas
        from tasks import download_and_queue_sftp_files, process_single_xml_file, process_sftp_files
        print("✅ Imports das tarefas funcionando")
        
        # Testar import da configuração
        from ..celery.celery_config import celery_app
        print("✅ Import da configuração funcionando")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro nos imports: {e}")
        return False

def test_celery_worker():
    """Testa se o worker consegue carregar as tarefas"""
    print("\n🔍 Testando worker...")
    
    try:
        from ..celery.celery_config import celery_app
        
        # Verificar se o worker consegue carregar as tarefas
        i = celery_app.control.inspect()
        stats = i.stats()
        
        if stats:
            print("✅ Worker está respondendo")
            return True
        else:
            print("⚠️  Worker não está respondendo (pode não estar rodando)")
            return False
            
    except Exception as e:
        print(f"❌ Erro ao testar worker: {e}")
        return False

def main():
    """Função principal"""
    print("🔧 Teste de Tarefas Celery")
    print("=" * 50)
    
    # Testar imports
    imports_ok = test_celery_imports()
    
    # Testar registro de tarefas
    tasks_ok = test_celery_tasks()
    
    # Testar worker
    worker_ok = test_celery_worker()
    
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES")
    print("=" * 50)
    
    print(f"Imports: {'✅ OK' if imports_ok else '❌ FALHOU'}")
    print(f"Tarefas: {'✅ OK' if tasks_ok else '❌ FALHOU'}")
    print(f"Worker: {'✅ OK' if worker_ok else '⚠️  NÃO RODANDO'}")
    
    if imports_ok and tasks_ok:
        print("\n🎉 Tarefas Celery configuradas corretamente!")
        print("📋 Para iniciar o worker:")
        print("   celery -A celery_config.celery_app worker --loglevel=info")
        return True
    else:
        print("\n🔧 Problemas encontrados:")
        if not imports_ok:
            print("   - Verificar se tasks.py está correto")
        if not tasks_ok:
            print("   - Verificar se as tarefas estão definidas com @celery_app.task")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 