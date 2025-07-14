#!/usr/bin/env python3
"""
Script para testar conexão com Supabase
"""
import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

def test_supabase_connection():
    """Testa conexão com Supabase"""
    print("🔧 Teste de Conexão com Supabase")
    print("=" * 40)
    
    # Verificar variáveis de ambiente
    SUPABASE_URL = os.getenv("SUPABASE_URL")
    SUPABASE_KEY = os.getenv("SUPABASE_KEY")
    
    if not SUPABASE_URL:
        print("❌ SUPABASE_URL não configurado")
        return False
        
    if not SUPABASE_KEY:
        print("❌ SUPABASE_KEY não configurado")
        return False
    
    print(f"✅ SUPABASE_URL: {SUPABASE_URL}")
    print(f"✅ SUPABASE_KEY: {SUPABASE_KEY[:10]}...")
    
    try:
        # Criar cliente
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("✅ Cliente Supabase criado")
        
        # Testar conexão básica
        print("🔄 Testando conexão...")
        response = supabase.table("companies").select("company_id").limit(1).execute()
        print("✅ Conexão bem-sucedida!")
        
        # Testar inserção
        print("🔄 Testando inserção...")
        test_data = {
            "company_id": "TEST_CONNECTION",
            "company_name": "Teste de Conexão",
            "address_detail": "Endereço Teste",
            "city": "Lisboa",
            "postal_code": "1000-000",
            "country": "Portugal"
        }
        
        insert_response = supabase.table("companies").upsert(
            test_data,
            on_conflict="company_id"
        ).execute()
        
        if insert_response.data:
            print("✅ Inserção bem-sucedida!")
            print(f"📊 Dados inseridos: {len(insert_response.data)} registros")
        else:
            print("⚠️ Inserção retornou dados vazios")
            
        # Testar consulta
        print("🔄 Testando consulta...")
        query_response = supabase.table("companies").select("*").eq("company_id", "TEST_CONNECTION").execute()
        
        if query_response.data:
            print("✅ Consulta bem-sucedida!")
            print(f"📊 Dados encontrados: {len(query_response.data)} registros")
        else:
            print("⚠️ Consulta não retornou dados")
            
        return True
        
    except Exception as e:
        print(f"❌ Erro na conexão: {str(e)}")
        return False

def test_table_structure():
    """Testa estrutura das tabelas"""
    print("\n🔧 Teste de Estrutura das Tabelas")
    print("=" * 40)
    
    try:
        supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_KEY"))
        
        # Listar tabelas disponíveis
        tables = ["companies", "invoices", "invoice_lines", "invoice_files", "invoice_file_links"]
        
        for table in tables:
            try:
                response = supabase.table(table).select("*").limit(1).execute()
                print(f"✅ Tabela '{table}' acessível")
            except Exception as e:
                print(f"❌ Erro na tabela '{table}': {str(e)}")
                
    except Exception as e:
        print(f"❌ Erro geral: {str(e)}")

def main():
    """Função principal"""
    print("🔧 Teste de Conexão com Supabase")
    print("=" * 40)
    
    # Testar conexão
    connection_ok = test_supabase_connection()
    
    if connection_ok:
        # Testar estrutura das tabelas
        test_table_structure()
        
        print("\n✅ Todos os testes concluídos!")
    else:
        print("\n❌ Falha na conexão. Verifique as configurações.")

if __name__ == "__main__":
    main() 