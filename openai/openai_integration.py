import os
import json
from datetime import datetime
from typing import Dict, Any, Optional
from dotenv import load_dotenv

# Importar o SDK oficial da OpenAI
from openai import OpenAI

# Importar a função de geração de dados diretamente
from utils.utils import gerar_dados_resumo_ia

# Carregar variáveis de ambiente
load_dotenv()

class OpenAIIntegration:
    def __init__(self):
        self.api_key = os.getenv('API_KEY_DEEP')
        if not self.api_key:
            raise ValueError("API_KEY_DEEP não encontrada no arquivo .env")
        
        # Inicializar cliente DeepSeek usando o SDK OpenAI
        self.client = OpenAI(
            api_key=self.api_key,
            base_url="https://api.deepseek.com/v1"
        )
        
        # Configurações centralizadas do modelo DeepSeek
        self.default_model = "deepseek-chat"
        self.default_temperature = 0.6 # Reduzido de 1.5 para 0.7 para respostas mais focadas
        self.default_max_tokens = 4000  # Reduzido de 4000 para 2000 para respostas mais concisas
        self.default_timeout = 60
        self.default_server_url = "http://localhost:8000"
    
    def _make_openai_request(self, payload: Dict[str, Any], timeout: int = 30) -> Dict[str, Any]:
        """Método helper para fazer requisições para DeepSeek usando o SDK"""
        try:
            # Preparar mensagens para o SDK
            messages = payload.get("messages", [])
            
            # Configurar parâmetros da requisição
            request_params = {
                "model": payload.get("model", self.default_model),
                "messages": messages,
                "temperature": payload.get("temperature", self.default_temperature),
                "max_tokens": payload.get("max_tokens", self.default_max_tokens)
            }
            
            # Fazer chamada usando o SDK
            response = self.client.chat.completions.create(**request_params)
            
            # Processar resposta
            if response and response.choices:
                return {
                    "success": True,
                    "analysis": response.choices[0].message.content,
                    "usage": {
                        "total_tokens": response.usage.total_tokens,
                        "prompt_tokens": response.usage.prompt_tokens,
                        "completion_tokens": response.usage.completion_tokens
                    },
                    "model": response.model
                }
            else:
                return {"success": False, "error": "Resposta vazia do DeepSeek"}
                
        except Exception as e:
            return {"success": False, "error": f"Erro na comunicação com DeepSeek: {str(e)}"}
    
    def analyze_with_openai(self, data: Dict[str, Any], prompt: str,
                           model: Optional[str] = None, max_tokens: Optional[int] = None,
                           temperature: Optional[float] = None) -> Dict[str, Any]:
        """Envia dados para análise do DeepSeek - SEMPRE usa IA avançada"""
        model = model or self.default_model
        max_tokens = max_tokens or self.default_max_tokens
        temperature = temperature if temperature is not None else self.default_temperature
        
        data_str = json.dumps(data, indent=2, ensure_ascii=False)
        
        full_prompt = f"""{prompt}
                Dados:
                {data_str}"""
        
        payload = {
            "model": model,
            "messages": [{"role": "user", "content": full_prompt}],
            "temperature": temperature,
            "max_tokens": max_tokens
        }
        
        return self._make_openai_request(payload)
    
    def generate_insights(self, nif: str, periodo: int = 0, filial: Optional[str] = None,
                          prompt: Optional[str] = None, server_url: Optional[str] = None, 
                          token: Optional[str] = None, model: Optional[str] = None, 
                          tipo_analise: str = "vendas", max_tokens: Optional[int] = None,
                          temperature: Optional[float] = None) -> Dict[str, Any]:
        """Gera insights usando IA baseado nos dados fornecidos"""
        try:
            # Etapa 1: Obter dados reais diretamente da função de lógica de negócios.
            # Isso remove a necessidade de uma chamada HTTP para a própria API.
            resultado_dados = gerar_dados_resumo_ia(nif, periodo, filial)
            
            if not resultado_dados.get("success"):
                # Se a busca de dados falhar, propaga o erro.
                raise Exception(resultado_dados.get("error", "Erro desconhecido ao buscar dados."))
            
            data = resultado_dados["data"]
            
            # Etapa 2: Preparar e gerar análise com IA
            prompt = prompt or self.get_custom_prompt(tipo_analise)
            analysis = self.analyze_with_openai(data, prompt, model, max_tokens, temperature)
            
            return {
                "success": True,
                "nif": nif,
                "periodo": periodo,
                "filial": filial,
                "data_timestamp": datetime.now().isoformat(),
                "original_data": data,
                "analysis": analysis
            }
        except Exception as e:
            # Captura qualquer erro, incluindo falhas em obter dados reais
            return {
                "success": False,
                "error": str(e),
                "nif": nif,
                "periodo": periodo,
                "filial": filial,
                "data_timestamp": datetime.now().isoformat()
            }
    
    def get_default_prompt(self, periodo: int) -> str:
        periodos = {0: "hoje", 1: "ontem", 2: "esta semana", 3: "este mês", 4: "este trimestre", 5: "este ano"}
        periodo_nome = periodos.get(periodo, "o período selecionado")
        
        return f"""Você é um analista de dados. Analise o seguinte relatório de vendas em JSON e responda com um resumo simples contendo:

                    1. Principais variações entre o período atual e o anterior.
                    2. Produtos mais vendidos e sua importância no total vendido.
                    3. Horários e dias com maior movimento.
                    4. Qualquer insight que ajude a melhorar as vendas ou a operação.

                    Responda de forma objetiva, como se estivesse enviando um relatório para o gerente de uma loja.

                    DADOS:
                    
                    """
    

    def get_custom_prompt(self, tipo_analise: str) -> str:
        prompts = {
            "vendas": """Analise os dados de vendas e forneça:

                1. **Performance Geral** - Vendas totais, ticket médio, volume por hora
                2. **Análise de Produtos** - Top produtos, correlações, sazonalidades
                3. **Horários de Pico** - Padrões de movimento e oportunidades
                4. **Recomendações** - 2-3 ações para aumentar vendas

                Seja conciso e use dados específicos.""",
                            
                            "operacional": """Analise os dados operacionais e forneça:

                1. **Eficiência** - Processos, gargalos, produtividade
                2. **Recursos** - Utilização, capacidade, otimizações
                3. **Recomendações** - 2-3 melhorias operacionais específicas

                Foque em dados concretos e ações práticas.""",
                            
                            "financeiro": """Analise os dados financeiros e forneça:

                1. **Rentabilidade** - Margens, ROI, performance financeira
                2. **Riscos** - Identificação e estratégias de mitigação
                3. **Recomendações** - 2-3 ações para otimizar finanças

                Use dados específicos e análise objetiva.""",
                            
                            "marketing": """Analise os dados de marketing e forneça:

                1. **Segmentação** - Comportamento de clientes, oportunidades
                2. **Performance** - Conversão, retenção, ROI de campanhas
                3. **Recomendações** - 2-3 estratégias de marketing baseadas em dados

                Foque em insights acionáveis.""",
                            
                            "estratégico": """Analise os dados estratégicos e forneça:

                1. **Posicionamento** - Vantagens competitivas, tendências
                2. **Oportunidades** - Crescimento, expansão, inovação
                3. **Recomendações** - 2-3 estratégias de longo prazo

                Use vis ão estratégica e dados concretos."""
        }
        
        return prompts.get(tipo_analise, self.get_default_prompt(0))
    
    def set_model(self, model: str):
        """Altera o modelo padrão"""
        self.default_model = model
    
    def set_temperature(self, temperature: float):
        """Altera a temperatura padrão"""
        self.default_temperature = temperature
    
    def set_max_tokens(self, max_tokens: int):
        """Altera o número máximo de tokens"""
        self.default_max_tokens = max_tokens
    
    def test_connection(self) -> Dict[str, Any]:
        """Testa a conexão com o DeepSeek"""
        try:
            # Teste simples com uma mensagem curta
            response = self.client.chat.completions.create(
                model=self.default_model,
                messages=[{"role": "user", "content": "Teste de conexão"}],
                max_tokens=10,
                temperature=0
            )
            return {
                "success": True,
                "message": "Conexão com DeepSeek estabelecida com sucesso",
                "model": response.model
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Erro na conexão com DeepSeek: {str(e)}"
            }


def main():
    """Função principal para teste da integração"""
    try:
        print("🚀 Iniciando teste da integração DeepSeek...")
        
        # Testar conexão primeiro
        openai_integration = OpenAIIntegration()
        connection_test = openai_integration.test_connection()
        
        if not connection_test["success"]:
            print(f"❌ Erro na conexão: {connection_test['error']}")
            return
        
        print("✅ Conexão com DeepSeek estabelecida!")
        print(f"📊 Modelo: {connection_test.get('model', 'N/A')}")
        
        # Configurações
        nif = "514757876"  # NIF de teste
        periodo = 0  # Hoje
        server_url = "http://localhost:8000"
        token = None  # Adicione seu token se necessário
        
        print(f"\n📊 NIF: {nif}")
        print(f"📅 Período: {periodo}")
        print(f"🌐 Servidor: {server_url}")
        
        # Gerar insights
        result = openai_integration.generate_insights(
            nif=nif,
            periodo=periodo,
            server_url=server_url,
            token=token
        )
        
        if result["success"]:
            print("\n✅ Análise concluída com sucesso!")
            print("\n" + "="*50)
            print("📋 RESULTADO DA ANÁLISE")
            print("="*50)
            
            if result["analysis"]["success"]:
                print(result["analysis"]["analysis"])
                
                print(f"\n📊 Estatísticas:")
                print(f"   Modelo usado: {result['analysis']['model']}")
                if "usage" in result["analysis"]:
                    usage = result["analysis"]["usage"]
                    print(f"   Tokens usados: {usage.get('total_tokens', 'N/A')}")
                    print(f"   Tokens de entrada: {usage.get('prompt_tokens', 'N/A')}")
                    print(f"   Tokens de saída: {usage.get('completion_tokens', 'N/A')}")
            else:
                print(f"❌ Erro na análise: {result['analysis']['error']}")
        else:
            print(f"❌ Erro: {result['error']}")
            
    except Exception as e:
        print(f"❌ Erro geral: {str(e)}")


if __name__ == "__main__":
    main() 