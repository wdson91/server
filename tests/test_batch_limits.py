#!/usr/bin/env python3
"""
Script para testar os limites de lote configurados
"""
import os
import sys
from dotenv import load_dotenv

load_dotenv()

def test_batch_limits():
    """Testa os limites de lote configurados"""
    print("🔧 Teste de Limites de Lote")
    print("=" * 50)
    
    # Carregar configurações
    max_files = int(os.getenv("MAX_FILES_PER_BATCH", "50"))
    batch_companies = int(os.getenv("BATCH_SIZE_COMPANIES", "1000"))
    batch_invoices = int(os.getenv("BATCH_SIZE_INVOICES", "500"))
    batch_lines = int(os.getenv("BATCH_SIZE_LINES", "2000"))
    batch_links = int(os.getenv("BATCH_SIZE_LINKS", "500"))
    
    print("📊 Limites Configurados:")
    print(f"   - Arquivos por lote: {max_files}")
    print(f"   - Empresas por lote: {batch_companies}")
    print(f"   - Faturas por lote: {batch_invoices}")
    print(f"   - Linhas por lote: {batch_lines}")
    print(f"   - Links por lote: {batch_links}")
    
    # Simular cenários
    print("\n📈 Cenários de Teste:")
    
    # Cenário 1: Poucos arquivos
    print(f"\n1️⃣ Poucos arquivos ({max_files//2} arquivos):")
    print(f"   - Serão processados: {max_files//2} arquivos")
    print(f"   - Restantes: 0")
    
    # Cenário 2: Limite exato
    print(f"\n2️⃣ Limite exato ({max_files} arquivos):")
    print(f"   - Serão processados: {max_files} arquivos")
    print(f"   - Restantes: 0")
    
    # Cenário 3: Muitos arquivos
    total_files = max_files * 2
    print(f"\n3️⃣ Muitos arquivos ({total_files} arquivos):")
    print(f"   - Serão processados: {max_files} arquivos")
    print(f"   - Restantes: {max_files} arquivos")
    print(f"   - Ciclos necessários: {total_files // max_files + (1 if total_files % max_files > 0 else 0)}")
    
    # Cenário 4: Performance estimada
    print(f"\n4️⃣ Performance Estimada:")
    estimated_companies = max_files * 10  # ~10 empresas por arquivo
    estimated_invoices = max_files * 50    # ~50 faturas por arquivo
    estimated_lines = max_files * 250      # ~250 linhas por arquivo
    
    print(f"   - Empresas por ciclo: ~{estimated_companies}")
    print(f"   - Faturas por ciclo: ~{estimated_invoices}")
    print(f"   - Linhas por ciclo: ~{estimated_lines}")
    
    # Verificar se os limites fazem sentido
    print(f"\n✅ Validação dos Limites:")
    
    if estimated_companies <= batch_companies:
        print(f"   ✅ Empresas: {estimated_companies} <= {batch_companies}")
    else:
        print(f"   ⚠️ Empresas: {estimated_companies} > {batch_companies} (pode precisar de ajuste)")
    
    if estimated_invoices <= batch_invoices:
        print(f"   ✅ Faturas: {estimated_invoices} <= {batch_invoices}")
    else:
        print(f"   ⚠️ Faturas: {estimated_invoices} > {batch_invoices} (pode precisar de ajuste)")
    
    if estimated_lines <= batch_lines:
        print(f"   ✅ Linhas: {estimated_lines} <= {batch_lines}")
    else:
        print(f"   ⚠️ Linhas: {estimated_lines} > {batch_lines} (pode precisar de ajuste)")
    
    return True

def show_configuration_help():
    """Mostra ajuda para configuração"""
    print("\n🔧 Como Configurar:")
    print("=" * 50)
    
    print("1️⃣ Adicione ao seu arquivo .env:")
    print("""
# Limite de arquivos processados por vez
MAX_FILES_PER_BATCH=50

# Limites de inserção em lote
BATCH_SIZE_COMPANIES=1000
BATCH_SIZE_INVOICES=500
BATCH_SIZE_LINES=2000
BATCH_SIZE_LINKS=500
""")
    
    print("2️⃣ Recomendações:")
    print("   - MAX_FILES_PER_BATCH: 20-100 (depende da capacidade do servidor)")
    print("   - BATCH_SIZE_*: Ajuste baseado na memória disponível")
    print("   - Para servidores menores: reduza os valores")
    print("   - Para servidores maiores: aumente os valores")
    
    print("\n3️⃣ Monitoramento:")
    print("   - Use Flower (http://localhost:5555) para monitorar")
    print("   - Verifique logs para performance")
    print("   - Ajuste valores conforme necessário")

def main():
    """Função principal"""
    print("🔧 Teste de Limites de Lote")
    print("=" * 50)
    
    # Testar limites
    test_batch_limits()
    
    # Mostrar ajuda
    show_configuration_help()
    
    print("\n✅ Teste concluído!")

if __name__ == "__main__":
    main() 