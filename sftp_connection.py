import paramiko
import os
import logging
import json

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
        file_mappings = []  # Armazenar mapeamento arquivo local -> remoto
        
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
                
                # Baixar todos os arquivos XML que começam com FR ou NC
                for arquivo in arquivos_pasta:
                    if arquivo.endswith('.xml') and (arquivo.startswith('FR') or arquivo.startswith('NC')):
                        caminho_remoto = f'{caminho_pasta_nif}/{arquivo}'
                        caminho_local = os.path.join(pasta_local, arquivo)
                        
                        logger.info(f'📥 Baixando {arquivo} da pasta {pasta_nif}...')
                        sftp.get(caminho_remoto, caminho_local)
                        downloaded_files.append(caminho_local)
                        
                        # Armazenar mapeamento para exclusão posterior
                        file_mappings.append({
                            'local_path': caminho_local,
                            'remote_path': caminho_remoto,
                            'filename': arquivo,
                            'nif_folder': pasta_nif
                        })
                        
            except Exception as e:
                logger.error(f"Erro ao processar pasta {pasta_nif}: {str(e)}")
                continue

        logger.info(f"✅ Download concluído! {len(downloaded_files)} arquivos baixados")
        
        # Salvar mapeamento em arquivo temporário para uso posterior
        import json
        mappings_file = os.path.join(pasta_local, 'file_mappings.json')
        with open(mappings_file, 'w') as f:
            json.dump(file_mappings, f)
        
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

def delete_file_from_sftp(local_file_path):
    """Exclui arquivo do SFTP após processamento bem-sucedido"""
    try:
        # Carregar mapeamento de arquivos
        mappings_file = os.path.join('./downloads', 'file_mappings.json')
        if not os.path.exists(mappings_file):
            logger.warning(f"⚠️ Arquivo de mapeamento não encontrado: {mappings_file}")
            return False
        
        with open(mappings_file, 'r') as f:
            file_mappings = json.load(f)
        
        # Encontrar mapeamento para o arquivo local
        file_mapping = None
        for mapping in file_mappings:
            if mapping['local_path'] == local_file_path:
                file_mapping = mapping
                break
        
        if not file_mapping:
            logger.warning(f"⚠️ Mapeamento não encontrado para: {local_file_path}")
            return False
        
        # Conectar ao SFTP
        sftp, transport = connect_sftp()
        if sftp is None:
            logger.error("❌ Não foi possível conectar ao SFTP para exclusão")
            return False
        
        try:
            # Excluir arquivo remoto
            remote_path = file_mapping['remote_path']
            logger.info(f"🗑️ Excluindo arquivo do SFTP: {file_mapping['filename']} da pasta {file_mapping['nif_folder']}")
            
            sftp.remove(remote_path)
            logger.info(f"✅ Arquivo excluído com sucesso do SFTP: {file_mapping['filename']}")
            
            # Remover mapeamento da lista
            file_mappings.remove(file_mapping)
            
            # Atualizar arquivo de mapeamento
            with open(mappings_file, 'w') as f:
                json.dump(file_mappings, f)
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao excluir arquivo do SFTP: {str(e)}")
            return False
        finally:
            # Fechar conexão
            if sftp:
                sftp.close()
            if transport:
                transport.close()
                
    except Exception as e:
        logger.error(f"❌ Erro geral na exclusão SFTP: {str(e)}")
        return False

if __name__ == "__main__":
    download_files_from_sftp()