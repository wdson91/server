#!/usr/bin/env python3
"""
Script para testar download SFTP com estrutura de pastas por NIF
"""
import os
from dotenv import load_dotenv

load_dotenv()

def test_sftp_download():
    """Testa o download SFTP"""
    print("🔧 Teste de Download SFTP")
    print("=" * 40)
    
    try:
        from sftp_connection import download_files_from_sftp
        
        print("🔄 Iniciando download...")
        downloaded_files = download_files_from_sftp()
        
        if downloaded_files:
            print(f"✅ Download bem-sucedido!")
            print(f"📊 Arquivos baixados: {len(downloaded_files)}")
            print("📁 Arquivos:")
            for file in downloaded_files:
                print(f"   - {os.path.basename(file)}")
        else:
            print("⚠️ Nenhum arquivo foi baixado")
            
        return True
        
    except Exception as e:
        print(f"❌ Erro no teste: {str(e)}")
        import traceback
        traceback.print_exc()
        return False

def main():
    """Função principal"""
    print("🔧 Teste de Download SFTP")
    print("=" * 40)
    
    success = test_sftp_download()
    
    if success:
        print("\n✅ Teste concluído com sucesso!")
    else:
        print("\n❌ Teste falhou!")

if __name__ == "__main__":
    main() 