# 🚀 Deploy em Produção

Guia completo para rodar o sistema de processamento SFTP em produção.

## 📋 Pré-requisitos

### Servidor Linux (Ubuntu/Debian recomendado)
```bash
# Atualizar sistema
sudo apt update && sudo apt upgrade -y

# Instalar dependências
sudo apt install -y python3 python3-pip python3-venv redis-server supervisor nginx
```

### Configuração do Redis
```bash
# Verificar se Redis está rodando
sudo systemctl status redis

# Se não estiver, iniciar
sudo systemctl start redis
sudo systemctl enable redis
```

## 🔧 Configuração do Projeto

### 1. Clonar e Configurar
```bash
# Clonar projeto
git clone <seu-repositorio>
cd server

# Criar ambiente virtual
python3 -m venv venv
source venv/bin/activate

# Instalar dependências
pip install -r requirements.txt
```

### 2. Configurar Variáveis de Ambiente
```bash
# Criar arquivo .env
cp .env.example .env

# Editar com suas configurações
nano .env
```

**Exemplo de .env para produção:**
```bash
# Configurações SFTP
SFTP_HOST=seu_sftp_host
SFTP_USERNAME=seu_username
SFTP_PASSWORD=seu_password
SFTP_PORT=22
SFTP_REMOTE_PATH=/path/to/xml/files
SFTP_LOCAL_PATH=./downloads

# Configurações Supabase
SUPABASE_URL=sua_supabase_url
SUPABASE_KEY=sua_supabase_key

# Configurações Redis
REDIS_URL=redis://localhost:6379/0

# Configurações de Lote (ajuste conforme capacidade do servidor)
MAX_FILES_PER_BATCH=50
BATCH_SIZE_COMPANIES=1000
BATCH_SIZE_INVOICES=500
BATCH_SIZE_LINES=2000
BATCH_SIZE_LINKS=500

# Configuração de Limpeza Automática
CLEANUP_AFTER_PROCESSING=true
```

## 🐳 Deploy com Docker (Recomendado)

### 1. Dockerfile
```dockerfile
FROM python:3.11-slim

# Instalar dependências do sistema
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Definir diretório de trabalho
WORKDIR /app

# Copiar requirements e instalar dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código
COPY . .

# Criar usuário não-root
RUN useradd -m -u 1000 appuser && chown -R appuser:appuser /app
USER appuser

# Expor porta
EXPOSE 8000

# Comando padrão
CMD ["python", "main.py"]
```

### 2. docker-compose.yml
```yaml
version: '3.8'

services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
    restart: unless-stopped

  celery-worker:
    build: .
    command: celery -A celery.celery_config.celery_app worker --loglevel=info --concurrency=4
    volumes:
      - ./downloads:/app/downloads
      - ./dados_processados:/app/dados_processados
    environment:
      - REDIS_URL=redis://redis:6379/0
    env_file:
      - .env
    depends_on:
      - redis
    restart: unless-stopped

  celery-beat:
    build: .
    command: celery -A celery.celery_config.celery_app beat --loglevel=info
    volumes:
      - ./downloads:/app/downloads
      - ./dados_processados:/app/dados_processados
    environment:
      - REDIS_URL=redis://redis:6379/0
    env_file:
      - .env
    depends_on:
      - redis
    restart: unless-stopped

  flower:
    build: .
    command: celery -A celery.celery_config.celery_app flower --port=5555 --broker=redis://redis:6379/0
    ports:
      - "5555:5555"
    environment:
      - REDIS_URL=redis://redis:6379/0
    env_file:
      - .env
    depends_on:
      - redis
    restart: unless-stopped

  flask-app:
    build: .
    command: python main.py
    ports:
      - "8000:8000"
    volumes:
      - ./downloads:/app/downloads
      - ./dados_processados:/app/dados_processados
    environment:
      - REDIS_URL=redis://redis:6379/0
    env_file:
      - .env
    depends_on:
      - redis
    restart: unless-stopped

volumes:
  redis_data:
```

### 3. Deploy com Docker
```bash
# Construir e iniciar
docker-compose up -d

# Verificar logs
docker-compose logs -f

# Parar serviços
docker-compose down
```

## 🔧 Deploy Manual (Sem Docker)

### 1. Configurar Supervisor

**Criar arquivo: `/etc/supervisor/conf.d/celery.conf`**
```ini
[program:celery-worker]
command=/home/seu_usuario/server/venv/bin/celery -A celery.celery_config.celery_app worker --loglevel=info --concurrency=4
directory=/home/seu_usuario/server
user=seu_usuario
numprocs=1
stdout_logfile=/var/log/celery/worker.log
stderr_logfile=/var/log/celery/worker.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:celery-beat]
command=/home/seu_usuario/server/venv/bin/celery -A celery.celery_config.celery_app beat --loglevel=info
directory=/home/seu_usuario/server
user=seu_usuario
numprocs=1
stdout_logfile=/var/log/celery/beat.log
stderr_logfile=/var/log/celery/beat.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:flower]
command=/home/seu_usuario/server/venv/bin/celery -A celery.celery_config.celery_app flower --port=5555
directory=/home/seu_usuario/server
user=seu_usuario
numprocs=1
stdout_logfile=/var/log/celery/flower.log
stderr_logfile=/var/log/celery/flower.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:flask-app]
command=/home/seu_usuario/server/venv/bin/python main.py
directory=/home/seu_usuario/server
user=seu_usuario
numprocs=1
stdout_logfile=/var/log/flask/app.log
stderr_logfile=/var/log/flask/app.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600
```

### 2. Configurar Logs
```bash
# Criar diretórios de log
sudo mkdir -p /var/log/celery /var/log/flask
sudo chown seu_usuario:seu_usuario /var/log/celery /var/log/flask
```

### 3. Iniciar Serviços
```bash
# Recarregar configuração do supervisor
sudo supervisorctl reread
sudo supervisorctl update

# Iniciar serviços
sudo supervisorctl start celery-worker
sudo supervisorctl start celery-beat
sudo supervisorctl start flower
sudo supervisorctl start flask-app

# Verificar status
sudo supervisorctl status
```

## 🔒 Configurações de Segurança

### 1. Firewall
```bash
# Permitir apenas portas necessárias
sudo ufw allow 22    # SSH
sudo ufw allow 8000  # Flask API
sudo ufw allow 5555  # Flower (opcional)
sudo ufw enable
```

### 2. SSL/HTTPS (Opcional)
```bash
# Instalar Certbot
sudo apt install certbot python3-certbot-nginx

# Configurar certificado
sudo certbot --nginx -d seu-dominio.com
```

### 3. Variáveis Sensíveis
```bash
# Usar variáveis de ambiente do sistema
export SFTP_PASSWORD="senha_super_secreta"
export SUPABASE_KEY="chave_super_secreta"

# Ou usar arquivo .env com permissões restritas
chmod 600 .env
```

## 📊 Monitoramento

### 1. Logs
```bash
# Ver logs em tempo real
tail -f /var/log/celery/worker.log
tail -f /var/log/celery/beat.log
tail -f /var/log/flask/app.log
```

### 2. Status dos Serviços
```bash
# Supervisor
sudo supervisorctl status

# Redis
redis-cli ping

# Celery
celery -A celery.celery_config.celery_app inspect active
```

### 3. Health Check
```bash
# API Flask
curl http://localhost:8000/api/health

# Flower
curl http://localhost:5555
```

## 🔄 Manutenção

### 1. Atualizar Código
```bash
# Parar serviços
sudo supervisorctl stop all

# Atualizar código
git pull origin main

# Reinstalar dependências (se necessário)
pip install -r requirements.txt

# Reiniciar serviços
sudo supervisorctl start all
```

### 2. Backup
```bash
# Backup dos dados processados
tar -czf backup_$(date +%Y%m%d).tar.gz dados_processados/

# Backup do banco (configurar no Supabase)
```

### 3. Limpeza
```bash
# Limpar logs antigos
find /var/log/celery -name "*.log" -mtime +30 -delete

# Limpeza automática (configurada no .env)
# CLEANUP_AFTER_PROCESSING=true

# Limpeza manual via API
curl -X POST http://localhost:8000/api/cleanup

# Limpeza manual via script
python -c "from tasks import cleanup_processed_files; cleanup_processed_files()"
```

## 🚨 Troubleshooting

### Problemas Comuns

1. **Redis não conecta**
```bash
sudo systemctl restart redis
redis-cli ping
```

2. **Celery não inicia**
```bash
# Verificar logs
tail -f /var/log/celery/worker.log

# Verificar variáveis de ambiente
echo $REDIS_URL
```

3. **SFTP não conecta**
```bash
# Testar conexão manual
ssh usuario@host_sftp
```

4. **Supabase não conecta**
```bash
# Verificar chaves
echo $SUPABASE_URL
echo $SUPABASE_KEY
```

## 📈 Escalabilidade

### Para Alta Demanda
```bash
# Aumentar workers
celery -A celery.celery_config.celery_app worker --concurrency=8

# Usar Redis Cluster
# Configurar múltiplos servidores
# Usar load balancer
```

### Monitoramento Avançado
- **Prometheus + Grafana** para métricas
- **Sentry** para logs de erro
- **Datadog** para APM

## ✅ Checklist de Produção

- [ ] Redis configurado e rodando
- [ ] Variáveis de ambiente configuradas
- [ ] Supervisor configurado
- [ ] Logs configurados
- [ ] Firewall configurado
- [ ] SSL configurado (opcional)
- [ ] Backup configurado
- [ ] Monitoramento configurado
- [ ] Testes realizados
- [ ] Documentação atualizada

## 🎯 Comandos Rápidos

```bash
# Iniciar tudo
sudo supervisorctl start all

# Parar tudo
sudo supervisorctl stop all

# Reiniciar tudo
sudo supervisorctl restart all

# Ver status
sudo supervisorctl status

# Ver logs
tail -f /var/log/celery/worker.log
``` 