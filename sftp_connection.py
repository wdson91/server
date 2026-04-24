from dotenv import load_dotenv
from dotenv import load_dotenv
import paramiko
import os
import logging
import json

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()
# Credenciais e host

host = os.getenv("SFTP_HOST")
port = int(os.getenv("SFTP_PORT", 22))
username = os.getenv("SFTP_USERNAME")
password = os.getenv("SFTP_PASSWORD")

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
        pasta_remota = '/home/mydreami/myDream'
       
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
                        # Adicionar o NIF (pasta_nif) ao nome do ficheiro local para evitar sobrerposições de clientes diferentes!
                        nome_local_seguro = f"{pasta_nif}_{arquivo}"
                        caminho_local = os.path.join(pasta_local, nome_local_seguro)
                        
                        logger.info(f'📥 Baixando {arquivo} da pasta {pasta_nif} como {nome_local_seguro}...')
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

def ensure_remote_dir(sftp, remote_dir):
    """Garante que um diretório existe no SFTP"""
    try:
        sftp.stat(remote_dir)
    except IOError:
        # Se não existir, criar (pode falhar se o pai não existir, mas aqui o pai é o NIF que já existe)
        try:
            sftp.mkdir(remote_dir)
            logger.info(f"📁 Diretório criado no SFTP: {remote_dir}")
        except Exception as e:
            logger.error(f"❌ Erro ao criar diretório {remote_dir}: {str(e)}")

def move_file_to_processed_on_sftp(local_file_path):
    """Move arquivo do SFTP para a pasta /processados após processamento bem-sucedido"""
    try:
        basename = os.path.basename(local_file_path)
        
        # O nome local seguro é formato: NIF_filename.xml
        if "_" not in basename:
            logger.error(f"❌ Nome de arquivo local inválido para reconstruir caminho: {basename}")
            return False
            
        nif_folder, filename = basename.split('_', 1)
        
        # Conectar ao SFTP
        sftp, transport = connect_sftp()
        if sftp is None:
            logger.error("❌ Não foi possível conectar ao SFTP para movimentação")
            return False
        
        try:
            # Reconstruir caminhos remotos
            pasta_remota = '/home/mydreami/myDream'
            remote_base_dir = f"{pasta_remota}/{nif_folder}"
            remote_path = f"{remote_base_dir}/{filename}"
            processed_dir = f"{remote_base_dir}/processados"
            new_remote_path = f"{processed_dir}/{filename}"
            
            # Garantir que a pasta processados existe
            ensure_remote_dir(sftp, processed_dir)
            
            logger.info(f"📦 Movendo arquivo no SFTP: {filename} para {processed_dir}")
            
            # Mover arquivo remoto
            sftp.rename(remote_path, new_remote_path)
            logger.info(f"✅ Arquivo movido com sucesso no SFTP: {filename}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao mover arquivo no SFTP: {str(e)}")
            return False
        finally:
            if sftp: sftp.close()
            if transport: transport.close()
                
    except Exception as e:
        logger.error(f"❌ Erro geral na movimentação SFTP: {str(e)}")
        return False

if __name__ == "__main__":
    download_files_from_sftp()
def download_opengcs_files_from_sftp():
    """Baixa arquivos OpenGCs do SFTP percorrendo pastas por NIF"""
    sftp, transport = connect_sftp()
    
    if sftp is None:
        logger.error("Não foi possível estabelecer conexão SFTP")
        return []
    
    try:
        # Pasta remota onde estão as pastas por NIF
        pasta_remota = '/home/mydreami/myDream'
        
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
                
                # Baixar arquivos que começam com opengcs-{nif}
                for arquivo in arquivos_pasta:
                    if arquivo.startswith(f'opengcs-{pasta_nif}'):
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

        logger.info(f"✅ Download OpenGCs concluído! {len(downloaded_files)} arquivos baixados")
        
        return downloaded_files
        
    except Exception as e:
        logger.error(f"Erro durante download OpenGCs: {str(e)}")
        return []
    finally:
        # Fechar conexão
        if sftp:
            sftp.close()
        if transport:
            transport.close()

from utils.xml_parser import extract_nif_from_filename

def move_opengcs_file_to_processed_on_sftp(local_file_path):
    """Move arquivo OpenGCs do SFTP para a pasta /processados após processamento bem-sucedido"""
    try:
        basename = os.path.basename(local_file_path)
        nif_folder = extract_nif_from_filename(basename)
        
        if not nif_folder:
            logger.error(f"❌ Não foi possível extrair NIF do arquivo: {basename}")
            return False
            
        # Conectar ao SFTP
        sftp, transport = connect_sftp()
        if sftp is None:
            logger.error("❌ Não foi possível conectar ao SFTP para movimentação OpenGCs")
            return False
        
        try:
            # Reconstruir caminhos remotos
            pasta_remota = '/home/mydreami/myDream'
            remote_base_dir = f"{pasta_remota}/{nif_folder}"
            remote_path = f"{remote_base_dir}/{basename}"
            processed_dir = f"{remote_base_dir}/processados"
            new_remote_path = f"{processed_dir}/{basename}"
            
            # Garantir que a pasta processados existe
            ensure_remote_dir(sftp, processed_dir)
            
            logger.info(f"📦 Movendo arquivo OpenGCs no SFTP: {basename} para {processed_dir}")
            
            # Mover arquivo remoto
            sftp.rename(remote_path, new_remote_path)
            logger.info(f"✅ Arquivo OpenGCs movido com sucesso no SFTP: {basename}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ Erro ao mover arquivo OpenGCs no SFTP: {str(e)}")
            return False
        finally:
            if sftp: sftp.close()
            if transport: transport.close()
                
    except Exception as e:
        logger.error(f"❌ Erro geral na movimentação SFTP OpenGCs: {str(e)}")
        return False
