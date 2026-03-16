import io
import logging
import os
from datetime import datetime

from sftp_connection import connect_sftp

logger = logging.getLogger(__name__)

# Pasta raiz no SFTP onde estão as pastas por NIF
SFTP_ROOT = "/home/mydreami/myDream"


class SFTPXMLUploader:
    """Envia ficheiros XML para o servidor SFTP na pasta correspondente ao NIF."""

    def __init__(self):
        self.sftp = None
        self.transport = None

    def __enter__(self):
        self.sftp, self.transport = connect_sftp()
        if self.sftp is None:
            raise ConnectionError("Não foi possível estabelecer conexão SFTP")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.sftp:
            self.sftp.close()
        if self.transport:
            self.transport.close()

    def _ensure_remote_dir(self, remote_path: str):
        """Cria a directoria remota se não existir."""
        try:
            self.sftp.stat(remote_path)
        except FileNotFoundError:
            logger.info(f"📁 Criando pasta remota: {remote_path}")
            self.sftp.mkdir(remote_path)

    def upload_xml(
        self,
        xml_content: bytes,
        nif: str,
        filename: str | None = None,
    ) -> dict:
        """
        Faz upload de um ficheiro XML para a pasta do NIF no SFTP.

        Args:
            xml_content: conteúdo do XML em bytes.
            nif: NIF (CompanyID) extraído do XML — define a pasta de destino.
            filename: nome do ficheiro remoto. Se omitido, gera um nome com timestamp.

        Returns:
            dict com remote_path e filename do ficheiro enviado.

        Raises:
            ConnectionError: se não conseguir ligar ao SFTP.
            Exception: para outros erros de upload.
        """
        if self.sftp is None:
            raise ConnectionError("SFTP não está ligado. Use dentro de 'with SFTPXMLUploader()'.")

        # Gerar nome de ficheiro se não fornecido
        if not filename:
            timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S")
            filename = f"upload_{nif}_{timestamp}.xml"

        # Garantir extensão .xml
        if not filename.lower().endswith(".xml"):
            filename += ".xml"

        # Caminho da pasta do NIF no SFTP
        nif_folder = f"{SFTP_ROOT}/{nif}"
        remote_path = f"{nif_folder}/{filename}"

        # Criar pasta do NIF se necessário
        self._ensure_remote_dir(nif_folder)

        # Fazer upload a partir de buffer em memória (sem disco)
        logger.info(f"📤 Enviando '{filename}' para SFTP em '{nif_folder}'...")
        with io.BytesIO(xml_content) as buf:
            self.sftp.putfo(buf, remote_path)

        logger.info(f"✅ Upload concluído: {remote_path}")
        return {
            "remote_path": remote_path,
            "filename": filename,
            "nif_folder": nif_folder,
            "size_bytes": len(xml_content),
        }


def upload_xml_to_sftp(
    xml_content: bytes,
    nif: str,
    filename: str | None = None,
) -> dict:
    """
    Função de conveniência para upload de um XML sem gerir o contexto manualmente.

    Exemplo:
        result = upload_xml_to_sftp(xml_bytes, nif="514244208", filename="FR2025.xml")

    Returns:
        dict com remote_path, filename, nif_folder e size_bytes.
    """
    with SFTPXMLUploader() as uploader:
        return uploader.upload_xml(xml_content, nif, filename)
