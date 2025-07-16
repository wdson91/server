#!/usr/bin/env python3
"""
Teste da integração com DeepSeek
"""

import os
import sys
from dotenv import load_dotenv

# Adicionar o diretório atual ao path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ..openai.openai_integration import OpenAIIntegration

def test_deepseek_connection():
    """Testa a conexão com o DeepSeek"""
    print("🔍 Testando conexão com DeepSeek...")
    
    try:
        # Carregar variáveis de ambiente
        load_dotenv()
        
        # Verificar se a API key existe
        api_key = os.getenv('API_KEY_DEEP')
        if not api_key:
            print("❌ API_KEY_DEEP não encontrada no arquivo .env")
            return False
        
        print(f"✅ API Key encontrada: {api_key[:10]}...")
        
        # Criar instância da integração
        integration = OpenAIIntegration()
        
        # Testar conexão
        result = integration.test_connection()
        
        if result["success"]:
            print("✅ Conexão com DeepSeek estabelecida com sucesso!")
            print(f"📊 Modelo: {result.get('model', 'N/A')}")
            return True
        else:
            print(f"❌ Erro na conexão: {result['error']}")
            return False
            
    except Exception as e:
        print(f"❌ Erro geral: {str(e)}")
        return False

def test_deepseek_analysis():
    """Testa a análise com DeepSeek"""
    print("\n🔍 Testando análise com DeepSeek...")
    
    try:
        # Criar instância da integração
        integration = OpenAIIntegration()
        
        # Dados de teste
        test_data = {
            "metadata": {
                "nif": "514757876",
                "periodo": {
                    "codigo": 0,
                    "nome": "hoje"
                }
            },
            "metricas_comparativas": {
                "total_vendas": {
                    "atual": 2500.0,
                    "anterior": 2000.0,
                    "variacao": 25.0,
                    "status": "crescimento"
                },
                "ticket_medio": {
                    "atual": 12.50,
                    "anterior": 10.00,
                    "variacao": 25.0,
                    "status": "crescimento"
                }
            },
            "analise_produtos": {
                "top_10_mais_vendidos": [
                    {"produto": "Hambúrguer Clássico", "quantidade": 45, "montante": 675.0},
                    {"produto": "Batata Frita", "quantidade": 38, "montante": 190.0}
                ]
            }
        }
        
        # Testar análise
        result = integration.analyze_with_openai(
            data=test_data,
            prompt="Analise os dados de vendas e forneça insights relevantes."
        )
        
        if result["success"]:
            print("✅ Análise com DeepSeek concluída com sucesso!")
            print("\n📋 RESULTADO:")
            print("="*50)
            print(result["analysis"])
            print("="*50)
            
            if "usage" in result:
                usage = result["usage"]
                print(f"\n📊 Estatísticas:")
                print(f"   Tokens usados: {usage.get('total_tokens', 'N/A')}")
                print(f"   Tokens de entrada: {usage.get('prompt_tokens', 'N/A')}")
                print(f"   Tokens de saída: {usage.get('completion_tokens', 'N/A')}")
            
            return True
        else:
            print(f"❌ Erro na análise: {result['error']}")
            return False
            
    except Exception as e:
        print(f"❌ Erro geral: {str(e)}")
        return False

def test_deepseek_insights():
    """Testa a geração de insights com DeepSeek"""
    print("\n🔍 Testando geração de insights com DeepSeek...")
    
    try:
        # Criar instância da integração
        integration = OpenAIIntegration()
        
        # Testar geração de insights
        result = integration.generate_insights(
            nif="514757876",
            periodo=0,
            tipo_analise="vendas"
        )
        
        if result["success"]:
            print("✅ Geração de insights com DeepSeek concluída com sucesso!")
            
            if result["analysis"]["success"]:
                print("\n📋 RESULTADO:")
                print("="*50)
                print(result["analysis"]["analysis"])
                print("="*50)
                
                if "usage" in result["analysis"]:
                    usage = result["analysis"]["usage"]
                    print(f"\n📊 Estatísticas:")
                    print(f"   Modelo: {result['analysis'].get('model', 'N/A')}")
                    print(f"   Tokens usados: {usage.get('total_tokens', 'N/A')}")
                    print(f"   Tokens de entrada: {usage.get('prompt_tokens', 'N/A')}")
                    print(f"   Tokens de saída: {usage.get('completion_tokens', 'N/A')}")
            else:
                print(f"❌ Erro na análise: {result['analysis']['error']}")
                return False
            
            return True
        else:
            print(f"❌ Erro na geração de insights: {result['error']}")
            return False
            
    except Exception as e:
        print(f"❌ Erro geral: {str(e)}")
        return False

def main():
    """Função principal de teste"""
    print("🚀 Iniciando testes da integração DeepSeek...")
    print("="*60)
    
    # Teste 1: Conexão
    connection_ok = test_deepseek_connection()
    
    if not connection_ok:
        print("\n❌ Teste de conexão falhou. Abortando outros testes.")
        return
    
    # Teste 2: Análise simples
    analysis_ok = test_deepseek_analysis()
    
    # Teste 3: Geração de insights
    insights_ok = test_deepseek_insights()
    
    # Resumo dos resultados
    print("\n" + "="*60)
    print("📊 RESUMO DOS TESTES")
    print("="*60)
    print(f"🔗 Conexão: {'✅ OK' if connection_ok else '❌ FALHOU'}")
    print(f"📋 Análise: {'✅ OK' if analysis_ok else '❌ FALHOU'}")
    print(f"💡 Insights: {'✅ OK' if insights_ok else '❌ FALHOU'}")
    
    if connection_ok and analysis_ok and insights_ok:
        print("\n🎉 Todos os testes passaram! Integração DeepSeek funcionando corretamente.")
    else:
        print("\n⚠️ Alguns testes falharam. Verifique a configuração.")

if __name__ == "__main__":
    main() 