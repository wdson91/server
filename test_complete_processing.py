#!/usr/bin/env python3
"""
Script para testar processamento completo de arquivo XML
"""
import os
import sys
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()

def test_complete_processing():
    """Testa processamento completo de um arquivo XML"""
    print("🔧 Teste de Processamento Completo")
    print("=" * 50)
    
    try:
        # Importar funções
        from tasks import parse_xml_to_json, process_and_insert_invoice_batch
        
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
        
        # Salvar JSON para teste
        pasta_dados_processados = './dados_processados'
        os.makedirs(pasta_dados_processados, exist_ok=True)
        
        json_filename = Path(test_file).stem + '_test.json'
        json_path = os.path.join(pasta_dados_processados, json_filename)
        
        import json
        with open(json_path, 'w', encoding='utf-8') as f:
            json.dump(json_data, f, ensure_ascii=False, indent=2)
        
        print(f"✅ JSON salvo em: {json_path}")
        
        # Testar inserção no banco
        print("\n🔄 Testando inserção no banco...")
        process_and_insert_invoice_batch(Path(json_path))
        
        print("✅ Processamento completo concluído!")
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Função principal"""
    print("🔧 Teste de Processamento Completo")
    print("=" * 50)
    
    success = test_complete_processing()
    
    if success:
        print("\n✅ Teste concluído com sucesso!")
    else:
        print("\n❌ Teste falhou!")

if __name__ == "__main__":
    main() 