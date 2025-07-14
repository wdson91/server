#!/usr/bin/env python3
"""
Script para testar especificamente a extração de invoice_lines do XML
"""
import os
import sys
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def test_invoice_lines_extraction():
    """Testa extração de invoice_lines do XML"""
    print("🔍 Teste de Extração de Invoice Lines")
    print("=" * 50)
    
    try:
        from tasks import parse_xml_to_json
        
        # Verificar se há arquivos XML na pasta downloads
        downloads_dir = './downloads'
        if not os.path.exists(downloads_dir):
            print("❌ Pasta downloads não encontrada")
            return False
            
        xml_files = [f for f in os.listdir(downloads_dir) if f.endswith('.xml')]
        
        if not xml_files:
            print("❌ Nenhum arquivo XML encontrado na pasta downloads")
            return False
            
        # Usar o primeiro arquivo XML encontrado
        test_file = os.path.join(downloads_dir, xml_files[0])
        print(f"📄 Testando arquivo: {test_file}")
        
        # Testar parse XML para JSON
        print("\n🔄 Testando parse XML para JSON...")
        json_data = parse_xml_to_json(test_file)
        
        if not json_data:
            print("❌ Falha no parse XML para JSON")
            return False
            
        print(f"✅ Parse bem-sucedido: {json_data['total_faturas']} faturas encontradas")
        
        # Analisar linhas de cada fatura
        total_lines = 0
        for i, fatura in enumerate(json_data["faturas"]):
            print(f"\n📄 Fatura {i+1}: {fatura['InvoiceNo']}")
            print(f"   - Company: {fatura['CompanyID']}")
            print(f"   - Customer: {fatura['CustomerID']}")
            print(f"   - Total: {fatura['GrossTotal']}")
            print(f"   - Linhas encontradas: {len(fatura['Lines'])}")
            
            if fatura['Lines']:
                print("   📋 Detalhes das linhas:")
                for j, linha in enumerate(fatura['Lines']):
                    print(f"      Linha {j+1}:")
                    print(f"        - Número: {linha['LineNumber']}")
                    print(f"        - Produto: {linha['ProductCode']}")
                    print(f"        - Descrição: {linha['Description'][:50]}...")
                    print(f"        - Quantidade: {linha['Quantity']}")
                    print(f"        - Preço Unitário: {linha['UnitPrice']}")
                    print(f"        - Valor: {linha['CreditAmount']}")
                    print(f"        - IVA: {linha['TaxPercentage']}%")
                    print(f"        - Preço com IVA: {linha['PriceWithIva']}")
            else:
                print("   ⚠️ NENHUMA LINHA ENCONTRADA!")
            
            total_lines += len(fatura['Lines'])
        
        print(f"\n📊 Resumo:")
        print(f"   - Total de faturas: {json_data['total_faturas']}")
        print(f"   - Total de linhas: {total_lines}")
        
        if total_lines == 0:
            print("❌ PROBLEMA: Nenhuma linha foi extraída!")
            return False
        else:
            print("✅ Linhas extraídas com sucesso!")
            
        # Salvar JSON para inspeção
        pasta_dados_processados = './dados_processados'
        os.makedirs(pasta_dados_processados, exist_ok=True)
        
        json_filename = Path(test_file).stem + '_lines_test.json'
        json_path = os.path.join(pasta_dados_processados, json_filename)
        
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON salvo em: {json_path}")
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def test_supabase_lines_insertion():
    """Testa inserção de linhas no Supabase"""
    print("\n🔍 Teste de Inserção de Linhas no Supabase")
    print("=" * 50)
    
    try:
        from supabase import create_client
        
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ SUPABASE_URL ou SUPABASE_KEY não configurados")
            return False
            
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Verificar se há faturas no banco
        print("🔄 Verificando faturas no banco...")
        invoices_response = supabase.table("invoices").select("id, invoice_no").limit(5).execute()
        
        if not invoices_response.data:
            print("❌ Nenhuma fatura encontrada no banco")
            return False
            
        print(f"✅ {len(invoices_response.data)} faturas encontradas")
        
        # Verificar linhas existentes
        print("🔄 Verificando linhas existentes...")
        lines_response = supabase.table("invoice_lines").select("id, invoice_id, line_number").limit(10).execute()
        
        print(f"✅ {len(lines_response.data)} linhas encontradas")
        
        if lines_response.data:
            print("📋 Exemplos de linhas:")
            for linha in lines_response.data[:3]:
                print(f"   - ID: {linha['id']}, Invoice ID: {linha['invoice_id']}, Linha: {linha['line_number']}")
        
        # Testar inserção de uma linha de teste
        print("\n🔄 Testando inserção de linha de teste...")
        test_invoice = invoices_response.data[0]
        
        test_line = {
            "invoice_id": test_invoice["id"],
            "line_number": 999,
            "product_code": "TEST_PRODUCT",
            "description": "Produto de teste para verificação",
            "quantity": 1.0,
            "unit_price": 100.0,
            "credit_amount": 100.0,
            "tax_percentage": 23.0,
            "price_with_iva": 123.0
        }
        
        insert_response = supabase.table("invoice_lines").insert(test_line).execute()
        
        if insert_response.data:
            print("✅ Linha de teste inserida com sucesso!")
            print(f"   - ID da linha: {insert_response.data[0]['id']}")
            
            # Remover linha de teste
            line_id = insert_response.data[0]['id']
            delete_response = supabase.table("invoice_lines").delete().eq("id", line_id).execute()
            print("✅ Linha de teste removida")
        else:
            print("❌ Falha ao inserir linha de teste")
            return False
            
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste de inserção: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    print("🚀 Iniciando testes de invoice_lines...")
    
    # Teste 1: Extração do XML
    success1 = test_invoice_lines_extraction()
    
    # Teste 2: Inserção no Supabase
    success2 = test_supabase_lines_insertion()
    
    print("\n" + "=" * 50)
    print("📊 Resultados dos Testes:")
    print(f"   - Extração XML: {'✅ SUCESSO' if success1 else '❌ FALHA'}")
    print(f"   - Inserção Supabase: {'✅ SUCESSO' if success2 else '❌ FALHA'}")
    
    if success1 and success2:
        print("\n🎉 Todos os testes passaram!")
    else:
        print("\n⚠️ Alguns testes falharam. Verifique os logs acima.") 