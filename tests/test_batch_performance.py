#!/usr/bin/env python3
"""
Script para testar performance da inserção em lote
"""
import os
import sys
import time
import json
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def test_batch_insert_performance():
    """Testa performance da inserção em lote"""
    print("🚀 Teste de Performance - Inserção em Lote")
    print("=" * 50)
    
    try:
        from tasks import insert_companies_batch, insert_invoices_batch, insert_invoice_lines_batch
        
        # Dados de teste
        test_companies = [
            {
                "company_id": f"TEST{i:03d}",
                "company_name": f"Empresa Teste {i}",
                "address_detail": f"Endereço {i}",
                "city": "Lisboa",
                "postal_code": "1000-000",
                "country": "Portugal"
            }
            for i in range(1, 101)  # 100 empresas
        ]
        
        test_invoices = [
            {
                "invoice_no": f"INV{i:06d}",
                "atcud": f"ATC{i:06d}",
                "company_id": f"TEST{(i % 100) + 1:03d}",
                "customer_id": f"CUST{i:06d}",
                "invoice_date": "2024-01-01",
                "invoice_status_date": "2024-01-01",
                "invoice_status_time": "12:00:00",
                "hash_extract": f"HASH{i:06d}",
                "end_date": "2024-01-01",
                "tax_payable": 100.0,
                "net_total": 1000.0,
                "gross_total": 1100.0,
                "payment_amount": 1100.0,
                "tax_type": "IVA"
            }
            for i in range(1, 101)  # 100 faturas
        ]
        
        test_lines = [
            {
                "invoice_id": None,  # Será atualizado
                "line_number": j,
                "product_code": f"PROD{i:06d}",
                "description": f"Produto {i} - Linha {j}",
                "quantity": 1.0,
                "unit_price": 100.0,
                "credit_amount": 100.0,
                "tax_percentage": 23.0,
                "price_with_iva": 123.0
            }
            for i in range(1, 101)  # 100 faturas
            for j in range(1, 6)  # 5 linhas por fatura = 500 linhas
        ]
        
        print("📊 Dados de teste criados:")
        print(f"   - {len(test_companies)} empresas")
        print(f"   - {len(test_invoices)} faturas")
        print(f"   - {len(test_lines)} linhas de faturas")
        
        # Teste de inserção em lote
        print("\n🔄 Testando inserção em lote...")
        
        start_time = time.time()
        
        # Inserir empresas em lote
        companies_start = time.time()
        companies_result = insert_companies_batch(test_companies)
        companies_time = time.time() - companies_start
        
        # Inserir faturas em lote
        invoices_start = time.time()
        invoices_result = insert_invoices_batch(test_invoices)
        invoices_time = time.time() - invoices_start
        
        # Simular inserção de linhas (sem invoice_id real)
        lines_start = time.time()
        lines_result = insert_invoice_lines_batch(test_lines[:10])  # Apenas 10 para teste
        lines_time = time.time() - lines_start
        
        total_time = time.time() - start_time
        
        print("\n📈 Resultados de Performance:")
        print(f"   - Empresas: {companies_time:.2f}s ({len(test_companies)} registros)")
        print(f"   - Faturas: {invoices_time:.2f}s ({len(test_invoices)} registros)")
        print(f"   - Linhas: {lines_time:.2f}s (10 registros de teste)")
        print(f"   - Total: {total_time:.2f}s")
        
        # Calcular performance
        total_records = len(test_companies) + len(test_invoices) + 10
        records_per_second = total_records / total_time if total_time > 0 else 0
        
        print(f"\n⚡ Performance:")
        print(f"   - {records_per_second:.1f} registros/segundo")
        print(f"   - {total_records} registros em {total_time:.2f}s")
        
        if companies_result and invoices_result:
            print("\n✅ Inserção em lote funcionando corretamente!")
            return True
        else:
            print("\n❌ Erro na inserção em lote")
            return False
            
    except Exception as e:
        print(f"\n❌ Erro no teste: {e}")
        return False

def test_individual_vs_batch():
    """Compara inserção individual vs lote"""
    print("\n🔍 Comparação: Individual vs Lote")
    print("=" * 50)
    
    try:
        from supabase import create_client
        
        SUPABASE_URL = os.getenv("SUPABASE_URL")
        SUPABASE_KEY = os.getenv("SUPABASE_KEY")
        
        if not SUPABASE_URL or not SUPABASE_KEY:
            print("❌ SUPABASE_URL ou SUPABASE_KEY não configurados")
            return False
            
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        
        # Dados de teste pequenos
        test_data = [
            {
                "company_id": f"PERF{i:03d}",
                "company_name": f"Empresa Performance {i}",
                "address_detail": f"Endereço {i}",
                "city": "Lisboa",
                "postal_code": "1000-000",
                "country": "Portugal"
            }
            for i in range(1, 11)  # 10 empresas
        ]
        
        print("📊 Testando com 10 registros...")
        
        # Teste inserção individual
        print("\n🔄 Inserção Individual:")
        individual_start = time.time()
        
        for item in test_data:
            supabase.table("companies").upsert(
                item,
                on_conflict="company_id"
            ).execute()
        
        individual_time = time.time() - individual_start
        print(f"   - Tempo: {individual_time:.2f}s")
        print(f"   - Chamadas: 10")
        
        # Teste inserção em lote
        print("\n🔄 Inserção em Lote:")
        batch_start = time.time()
        
        supabase.table("companies").upsert(
            test_data,
            on_conflict="company_id"
        ).execute()
        
        batch_time = time.time() - batch_start
        print(f"   - Tempo: {batch_time:.2f}s")
        print(f"   - Chamadas: 1")
        
        # Comparação
        speedup = individual_time / batch_time if batch_time > 0 else 0
        print(f"\n📈 Melhoria de Performance:")
        print(f"   - {speedup:.1f}x mais rápido")
        print(f"   - {individual_time - batch_time:.2f}s economizados")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Erro na comparação: {e}")
        return False

def main():
    """Função principal"""
    print("🔧 Teste de Performance - Inserção em Lote")
    print("=" * 50)
    
    # Teste de inserção em lote
    batch_ok = test_batch_insert_performance()
    
    # Teste de comparação
    comparison_ok = test_individual_vs_batch()
    
    print("\n" + "=" * 50)
    print("📊 RESUMO DOS TESTES")
    print("=" * 50)
    
    print(f"Inserção em Lote: {'✅ OK' if batch_ok else '❌ FALHOU'}")
    print(f"Comparação Individual vs Lote: {'✅ OK' if comparison_ok else '❌ FALHOU'}")
    
    if batch_ok and comparison_ok:
        print("\n🎉 Performance otimizada! Inserção em lote funcionando.")
        print("📋 Vantagens:")
        print("   - Menos chamadas ao banco")
        print("   - Processamento mais rápido")
        print("   - Melhor uso de recursos")
        return True
    else:
        print("\n⚠️  Problemas encontrados na performance.")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1) 