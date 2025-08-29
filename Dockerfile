# --- Estágio 1: Builder ---
# Instala dependências, incluindo as que precisam de compilação
FROM python:3.11-slim AS builder

WORKDIR /app

# Instalar dependências do sistema necessárias para compilar
RUN apt-get update && apt-get install -y gcc && rm -rf /var/lib/apt/lists/*

# Copiar e instalar dependências Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt


# --- Estágio 2: Final ---
# A imagem final, que é mais leve e segura
FROM python:3.11-slim

WORKDIR /app

# Copiar dependências instaladas do estágio 'builder'
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

# Copiar o código da aplicação (respeitando o .dockerignore)
COPY . .

# Criar diretórios necessários
RUN mkdir -p downloads dados_processados

# Criar usuário não-root e dar permissões
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expor a porta
EXPOSE 8000

# Adicionar um Health Check para que o Docker saiba se a aplicação está saudável
HEALTHCHECK --interval=30s --timeout=3s \
  CMD curl --fail http://localhost:8000/api/health || exit 1
# (Você precisa de criar uma rota /health na sua API que retorne status 200 OK)

# Comando para iniciar o servidor de produção
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]