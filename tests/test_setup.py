#!/usr/bin/env python3
"""
Script de teste para verificar se todos os componentes estão funcionando
"""
import os
import sys
import requests
import json
from dotenv import load_dotenv

load_dotenv()

def test_redis_connection():
    """Testa conexão com Redis"""
    print("🔍 Testando conexão com Redis...")
    try:
        import redis
        redis_url = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
        r = redis.from_url(redis_url)
        r.ping()
        print("✅ Redis conectado com sucesso")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar Redis: {e}")
        return False

def test_supabase_connection():
    """Testa conexão com Supabase"""
    print("🔍 Testando conexão com Supabase...")
    try:
        from supabase import create_client
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ SUPABASE_URL ou SUPABASE_KEY não configurados")
            return False
            
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        # Teste simples de conexão
        response = supabase.table("companies").select("company_id").limit(1).execute()
        print("✅ Supabase conectado com sucesso")
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar Supabase: {e}")
        return False

def test_sftp_connection():
    """Testa conexão SFTP"""
    print("🔍 Testando conexão SFTP...")
    try:
        from sftp_connection import connect_sftp
        sftp, transport = connect_sftp()
        
        if sftp is None:
            print("❌ Não foi possível conectar ao SFTP")
            return False
            
        # Teste simples - listar arquivos
        arquivos = sftp.listdir('uploads')
        print(f"✅ SFTP conectado com sucesso. {len(arquivos)} arquivos encontrados")
        
        if sftp:
            sftp.close()
        if transport:
            transport.close()
        return True
    except Exception as e:
        print(f"❌ Erro ao conectar SFTP: {e}")
        return False

def test_flask_server():
    """Testa servidor Flask"""
    print("🔍 Testando servidor Flask...")
    try:
        # Tentar conectar ao servidor
        response = requests.get("http://localhost:8000/api/health", timeout=5)
        if response.status_code == 200:
            print("✅ Servidor Flask respondendo")
            return True
        else:
            print(f"❌ Servidor Flask retornou status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Servidor Flask não está rodando")
        return False
    except Exception as e:
        print(f"❌ Erro ao testar servidor Flask: {e}")
        return False

def test_celery_worker():
    """Testa Celery worker"""
    print("🔍 Testando Celery worker...")
    try:
        from ..celery.celery_config import celery_app
        
        # Teste simples - verificar se worker está ativo
        i = celery_app.control.inspect()
        active_tasks = i.active()
        
        if active_tasks is not None:
            print("✅ Celery worker está ativo")
            return True
        else:
            print("❌ Celery worker não está respondendo")
            return False
    except Exception as e:
        print(f"❌ Erro ao testar Celery worker: {e}")
        return False

def test_flower():
    """Testa Flower (interface de monitoramento)"""
    print("🔍 Testando Flower...")
    try:
        # Tentar conectar ao Flower
        response = requests.get("http://localhost:5555", timeout=5)
        if response.status_code == 200:
            print("✅ Flower está respondendo")
            return True
        else:
            print(f"❌ Flower retornou status {response.status_code}")
            return False
    except requests.exceptions.ConnectionError:
        print("❌ Flower não está rodando")
        return False
    except Exception as e:
        print(f"❌ Erro ao testar Flower: {e}")
        return False

def test_xml_parsing():
    """Testa parsing de XML"""
    print("🔍 Testando parsing de XML...")
    try:
        import xmltodict
        
        # Criar XML de teste
        test_xml = """<?xml version="1.0" encoding="UTF-8"?>
        <AuditFile>
            <Company>
                <CompanyID>123456789</CompanyID>
                <CompanyName>Empresa Teste</CompanyName>
            </Company>
        </AuditFile>"""
        
        # Testar parsing
        xml_dict = xmltodict.parse(test_xml)
        
        if 'AuditFile' in xml_dict:
            print("✅ Parsing de XML funcionando")
            return True
        else:
            print("❌ Falha no parsing de XML")
            return False
    except Exception as e:
        print(f"❌ Erro ao testar parsing XML: {e}")
        return False

def run_all_tests():
    """Executa todos os testes"""
    print("🚀 Iniciando testes do sistema...")
    print("=" * 50)
    
    tests = [
        ("Redis", test_redis_connection),
        ("Supabase", test_supabase_connection),
        ("SFTP", test_sftp_connection),
        ("XML Parsing", test_xml_parsing),
        ("Flask Server", test_flask_server),
        ("Celery Worker", test_celery_worker),
        ("Flower", test_flower),
    ]
    
    results = {}
    
    for test_name, test_func in tests:
        print(f"\n📋 Testando: {test_name}")
        try:
            result = test_func()
            results[test_name] = result
        except Exception as e:
            print(f"❌ Erro inesperado no teste {test_name}: {e}")
            results[test_name] = False
    
    # Resumo dos resultados
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results.items():
        status = "✅ PASSOU" if result else "❌ FALHOU"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n🎯 Resultado: {passed}/{total} testes passaram")
    
    if passed == total:
        print("🎉 Todos os testes passaram! Sistema pronto para uso.")
        return True
    else:
        print("⚠️  Alguns testes falharam. Verifique as configurações.")
        return False

def main():
    """Função principal"""
    print("🔧 Teste de Configuração do Sistema SFTP-Celery")
    print("=" * 50)
    
    # Verificar se arquivo .env existe
    if not os.path.exists('.env'):
        print("⚠️  Arquivo .env não encontrado. Criando exemplo...")
        with open('.env.example', 'w') as f:
            f.write("""# Redis
REDIS_URL=redis://localhost:6379/0

# Supabase
SUPABASE_URL=sua_url_do_supabase
SUPABASE_KEY=sua_chave_do_supabase

# Outras configurações
API_KEY_DEEP=sua_chave_api
""")
        print("📝 Arquivo .env.example criado. Configure suas variáveis de ambiente.")
        return False
    
    # Executar testes
    success = run_all_tests()
    
    if success:
        print("\n🚀 Sistema configurado corretamente!")
        print("📋 Próximos passos:")
        print("1. Inicie o Celery + Flower: python start_celery.py")
        print("2. Inicie o Flask: python main.py")
        print("3. Acesse Flask: http://localhost:8000/api/health")
        print("4. Acesse Flower: http://localhost:5555")
    else:
        print("\n🔧 Problemas encontrados:")
        print("1. Verifique se Redis está rodando")
        print("2. Configure SUPABASE_URL e SUPABASE_KEY no .env")
        print("3. Verifique conectividade de rede para SFTP")
        print("4. Instale todas as dependências: pip install -r requirements.txt")
        print("5. Para Flower, verifique se a porta 5555 está livre")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 