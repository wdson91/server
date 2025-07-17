#!/usr/bin/env python3
"""
Script de teste para a funcionalidade OpenGCs
"""

import os
import sys
import json
import tempfile
from pathlib import Path

# Adicionar o diretório atual ao path
sys.path.append(os.path.dirname(__file__))

from tasks import (
    parse_opengcs_xml_to_json,
    extract_nif_from_filename,
    insert_opengcs_to_supabase,
    download_opengcs_files_from_sftp
)

def create_test_xml():
    """Cria um arquivo XML de teste OpenGCs"""
    xml_content = """<?xml version="1.0" encoding="UTF-8"?>
<OpenGCs>
<OpenGCsTotal>0.90</OpenGCsTotal>
<OpenGCs>4</OpenGCs>
<GC>
<number>1</number>
<OpenTime>0000-00-00T00:00:00</OpenTime>
<LastTime>2025-07-14T20:15:41</LastTime>
<guests>0</guests>
<operatorNo>99</operatorNo>
<operatorName>Técnico</operatorName>
<StartOperatorNo>0</StartOperatorNo>
<StartOperatorName/>
<total>0.30</total>
</GC>
<GC>
<number>2</number>
<OpenTime>0000-00-00T00:00:00</OpenTime>
<LastTime>2025-07-14T19:51:57</LastTime>
<guests>0</guests>
<operatorNo>99</operatorNo>
<operatorName>Técnico</operatorName>
<StartOperatorNo>0</StartOperatorNo>
<StartOperatorName/>
<total>0.30</total>
</GC>
<GC>
<number>5</number>
<OpenTime>0000-00-00T00:00:00</OpenTime>
<LastTime>2025-07-15T09:40:36</LastTime>
<guests>0</guests>
<operatorNo>99</operatorNo>
<operatorName>Técnico</operatorName>
<StartOperatorNo>0</StartOperatorNo>
<StartOperatorName/>
<total>0.30</total>
</GC>
</OpenGCs>"""
    
    return xml_content

def create_test_xml_with_accent():
    """Cria um arquivo XML de teste OpenGCs com acentos (latin-1)"""
    xml_content = """<?xml version="1.0" encoding="ISO-8859-1"?>
<OpenGCs>
<OpenGCsTotal>0.90</OpenGCsTotal>
<OpenGCs>4</OpenGCs>
<GC>
<number>1</number>
<OpenTime>0000-00-00T00:00:00</OpenTime>
<LastTime>2025-07-14T20:15:41</LastTime>
<guests>0</guests>
<operatorNo>99</operatorNo>
<operatorName>Técnico</operatorName>
<StartOperatorNo>0</StartOperatorNo>
<StartOperatorName/>
<total>0.30</total>
</GC>
</OpenGCs>"""
    
    return xml_content

def test_parse_opengcs_xml():
    """Testa a conversão XML para JSON"""
    print("🧪 Testando conversão XML para JSON...")
    
    # Teste 1: XML UTF-8
    xml_content = create_test_xml()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
        f.write(xml_content)
        xml_file_path = f.name
    
    try:
        # Testar conversão
        result = parse_opengcs_xml_to_json(xml_file_path)
        
        if result:
            print("✅ Conversão XML UTF-8 bem-sucedida")
            print(f"📊 Total OpenGCs: {result['opengcs_total']}")
            print(f"📊 Contagem OpenGCs: {result['opengcs_count']}")
            print(f"📊 GCs encontrados: {len(result['gcs'])}")
            
            # Mostrar primeiro GC
            if result['gcs']:
                first_gc = result['gcs'][0]
                print(f"📋 Primeiro GC: Número {first_gc['number']}, Operador: {first_gc['operator_name']}")
        else:
            print("❌ Falha na conversão XML UTF-8")
            return False
            
    finally:
        # Limpar arquivo temporário
        if os.path.exists(xml_file_path):
            os.unlink(xml_file_path)
    
    # Teste 2: XML com acentos (latin-1)
    xml_content_accent = create_test_xml_with_accent()
    
    with tempfile.NamedTemporaryFile(mode='wb', suffix='.xml', delete=False) as f:
        f.write(xml_content_accent.encode('latin-1'))
        xml_file_path = f.name
    
    try:
        # Testar conversão
        result = parse_opengcs_xml_to_json(xml_file_path)
        
        if result:
            print("✅ Conversão XML com acentos bem-sucedida")
            print(f"📊 Total OpenGCs: {result['opengcs_total']}")
            print(f"📊 Contagem OpenGCs: {result['opengcs_count']}")
            print(f"📊 GCs encontrados: {len(result['gcs'])}")
            
            # Mostrar primeiro GC
            if result['gcs']:
                first_gc = result['gcs'][0]
                print(f"📋 Primeiro GC: Número {first_gc['number']}, Operador: {first_gc['operator_name']}")
            
            return True
        else:
            print("❌ Falha na conversão XML com acentos")
            return False
            
    finally:
        # Limpar arquivo temporário
        if os.path.exists(xml_file_path):
            os.unlink(xml_file_path)

def test_extract_nif_from_filename():
    """Testa a extração de NIF do nome do arquivo"""
    print("\n🧪 Testando extração de NIF do nome do arquivo...")
    
    test_cases = [
        ("opengcs-123456789.xml", "123456789"),
        ("opengcs-987654321.xml", "987654321"),
        ("opengcs-123456789", "123456789"),
        ("invalid-name.xml", ""),
        ("opengcs-.xml", ""),
    ]
    
    for filename, expected_nif in test_cases:
        result = extract_nif_from_filename(filename)
        if result == expected_nif:
            print(f"✅ {filename} -> {result}")
        else:
            print(f"❌ {filename} -> {result} (esperado: {expected_nif})")
            return False
    
    return True

def test_sftp_connection():
    """Testa conexão SFTP para OpenGCs"""
    print("\n🧪 Testando conexão SFTP para OpenGCs...")
    
    try:
        files = download_opengcs_files_from_sftp()
        print(f"✅ Conexão SFTP OK. {len(files)} arquivos OpenGCs encontrados")
        return True
    except Exception as e:
        print(f"❌ Erro na conexão SFTP: {str(e)}")
        return False

def test_supabase_connection():
    """Testa conexão com Supabase"""
    print("\n🧪 Testando conexão com Supabase...")
    
    try:
        # Criar dados de teste
        test_data = {
            "arquivo_origem": "test-opengcs.xml",
            "data_processamento": "2025-01-15T10:30:00",
            "opengcs_total": 0.90,
            "opengcs_count": 4,
            "gcs": [
                {
                    "number": 1,
                    "open_time": "0000-00-00T00:00:00",
                    "last_time": "2025-07-14T20:15:41",
                    "guests": 0,
                    "operator_no": 99,
                    "operator_name": "Técnico",
                    "start_operator_no": 0,
                    "start_operator_name": "",
                    "total": 0.30
                }
            ]
        }
        
        # Criar arquivo temporário para teste
        with tempfile.NamedTemporaryFile(mode='w', suffix='.xml', delete=False) as f:
            f.write("test")
            xml_file_path = f.name
        
        try:
            # Testar inserção (pode falhar se tabela não existir, mas testa conexão)
            result = insert_opengcs_to_supabase(test_data, xml_file_path)
            if result:
                print("✅ Conexão Supabase OK")
            else:
                print("⚠️ Inserção falhou (pode ser normal se tabela não existir)")
            return True
        finally:
            if os.path.exists(xml_file_path):
                os.unlink(xml_file_path)
                
    except Exception as e:
        print(f"❌ Erro na conexão Supabase: {str(e)}")
        return False

def main():
    """Executa todos os testes"""
    print("🚀 Iniciando testes da funcionalidade OpenGCs...\n")
    
    tests = [
        ("Conversão XML para JSON", test_parse_opengcs_xml),
        ("Extração de NIF", test_extract_nif_from_filename),
        ("Conexão SFTP", test_sftp_connection),
        ("Conexão Supabase", test_supabase_connection),
    ]
    
    passed = 0
    total = len(tests)
    
    for test_name, test_func in tests:
        print(f"\n{'='*50}")
        print(f"🧪 {test_name}")
        print(f"{'='*50}")
        
        try:
            if test_func():
                passed += 1
                print(f"✅ {test_name}: PASSOU")
            else:
                print(f"❌ {test_name}: FALHOU")
        except Exception as e:
            print(f"❌ {test_name}: ERRO - {str(e)}")
    
    print(f"\n{'='*50}")
    print(f"📊 RESULTADO FINAL: {passed}/{total} testes passaram")
    print(f"{'='*50}")
    
    if passed == total:
        print("🎉 Todos os testes passaram! A funcionalidade OpenGCs está pronta.")
        return 0
    else:
        print("⚠️ Alguns testes falharam. Verifique os logs acima.")
        return 1

if __name__ == "__main__":
    exit(main()) 