import os
import requests

def enviar_faturas_para_api(diretorio="faturas_simuladas", url_api="http://localhost:8000/api/upload-fatura"):
    """
    Lê todos os arquivos .txt no diretório e envia um a um para a API como se fosse um upload.
    """
    print("🔍 Iniciando o envio de faturas...")

    arquivos = [f for f in os.listdir(diretorio) if f.endswith(".txt")]

    if not arquivos:
        print("Nenhum arquivo .txt encontrado no diretório especificado.")
        return

    for arquivo in arquivos:
        caminho = os.path.join(diretorio, arquivo)
        with open(caminho, "rb") as f:
            files = {"file": (arquivo, f, "text/plain")}
            try:
                #print(f"Enviando {arquivo}...")
                response = requests.post(url_api, files=files)
                
                print(response.text)
                deve_apagar = False

                if response.status_code == 201:
                    print(f"{arquivo} enviado com sucesso!\n")
                    deve_apagar = True

                elif response.status_code != 200:
                    try:
                        erro_json = response.json()
                        for erro in erro_json.get("erros", []):
                            if "23505" in str(erro.get("erro", "")):  # Duplicado
                                print(f"{arquivo} já existe na base (duplicado).\n")
                                deve_apagar = True
                    except Exception as e:
                        print(f"Erro ao interpretar JSON de erro: {e}")

                if deve_apagar:
                    os.remove(caminho)
                    print(f"{arquivo} apagado.\n")

            except Exception as e:
                print(f" Erro ao enviar {arquivo}: {e}\n")

    print(" Envio de faturas concluído.")


enviar_faturas_para_api()