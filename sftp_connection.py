import paramiko
import os
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Credenciais e host
#host = "dreamidserver.ddns.net"
host = "13.48.69.154"
port = 22
username = "sftpuser"
password = "fd4d41fd-8e17-3cfa-a193-34601e70baf8"

def connect_sftp():
    """Estabelece conexão SFTP e retorna o cliente"""
    try:
        # Conectar via SSH
        transport = paramiko.Transport((host, port))
        transport.connect(username=username, password=password)
        
        # Criar cliente SFTP
        sftp = paramiko.SFTPClient.from_transport(transport)
        logger.info("Conexão SFTP estabelecida com sucesso")
        return sftp, transport
    except Exception as e:
        logger.error(f"Erro ao conectar SFTP: {str(e)}")
        return None, None

def download_files_from_sftp():
    """Baixa arquivos do SFTP percorrendo pastas por NIF"""
    sftp, transport = connect_sftp()
    
    if sftp is None:
        logger.error("Não foi possível estabelecer conexão SFTP")
        return []
    
    try:
        # Pasta remota onde estão as pastas por NIF
        pasta_remota = 'uploads'
        
        # Pasta local onde os arquivos serão salvos
        pasta_local = './downloads'
        os.makedirs(pasta_local, exist_ok=True)

        downloaded_files = []
        
        # Listar todas as pastas (NIFs das empresas)
        try:
            pastas_nif = sftp.listdir(pasta_remota)
            logger.info(f"Pastas NIF encontradas: {pastas_nif}")
        except Exception as e:
            logger.error(f"Erro ao listar pastas NIF: {str(e)}")
            return []
        
        # Percorrer cada pasta NIF
        for pasta_nif in pastas_nif:
            try:
                caminho_pasta_nif = f'{pasta_remota}/{pasta_nif}'
                logger.info(f"🔄 Verificando pasta NIF: {pasta_nif}")
                
                # Listar arquivos na pasta NIF
                arquivos_pasta = sftp.listdir(caminho_pasta_nif)
                logger.info(f"📁 Arquivos na pasta {pasta_nif}: {arquivos_pasta}")
                
                # Baixar todos os arquivos XML que começam com FR
                for arquivo in arquivos_pasta:
                    if arquivo.endswith('.xml') and arquivo.startswith('FR'):
                        caminho_remoto = f'{caminho_pasta_nif}/{arquivo}'
                        caminho_local = os.path.join(pasta_local, arquivo)
                        
                        logger.info(f'📥 Baixando {arquivo} da pasta {pasta_nif}...')
                        sftp.get(caminho_remoto, caminho_local)
                        downloaded_files.append(caminho_local)
                        
            except Exception as e:
                logger.error(f"Erro ao processar pasta {pasta_nif}: {str(e)}")
                continue

        logger.info(f"✅ Download concluído! {len(downloaded_files)} arquivos baixados")
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Erro durante download: {str(e)}")
        return []
    finally:
        # Fechar conexão
        if sftp:
            sftp.close()
        if transport:
            transport.close()

if __name__ == "__main__":
    download_files_from_sftp()