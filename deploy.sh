#!/bin/bash

# Script de Deploy Rápido
# Uso: ./deploy.sh [docker|manual]

set -e

echo "🚀 Deploy do Sistema de Processamento SFTP"
echo "=========================================="

# Verificar argumento
if [ "$1" = "docker" ]; then
    echo "🐳 Deploy com Docker"
    echo "===================="
    
    # Verificar se Docker está instalado
    if ! command -v docker &> /dev/null; then
        echo "❌ Docker não está instalado"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo "❌ Docker Compose não está instalado"
        exit 1
    fi
    
    # Verificar se .env existe
    if [ ! -f .env ]; then
        echo "❌ Arquivo .env não encontrado"
        echo "📝 Crie o arquivo .env com suas configurações"
        exit 1
    fi
    
    echo "🔧 Construindo e iniciando containers..."
    docker-compose up -d --build
    
    echo "⏳ Aguardando serviços iniciarem..."
    sleep 10
    
    echo "📊 Verificando status dos serviços..."
    docker-compose ps
    
    echo "✅ Deploy concluído!"
    echo "🌐 Serviços disponíveis:"
    echo "   - Flask API: http://localhost:8000"
    echo "   - Flower: http://localhost:5555"
    echo "   - Redis: localhost:6379"
    
elif [ "$1" = "manual" ]; then
    echo "🔧 Deploy Manual"
    echo "================"
    
    # Verificar se supervisor está instalado
    if ! command -v supervisorctl &> /dev/null; then
        echo "❌ Supervisor não está instalado"
        echo "💡 Execute: sudo apt install supervisor"
        exit 1
    fi
    
    # Verificar se Redis está rodando
    if ! systemctl is-active --quiet redis; then
        echo "❌ Redis não está rodando"
        echo "💡 Execute: sudo systemctl start redis"
        exit 1
    fi
    
    echo "🔧 Configurando supervisor..."
    
    # Criar configuração do supervisor
    sudo tee /etc/supervisor/conf.d/celery.conf > /dev/null <<EOF
[program:celery-worker]
command=$(pwd)/venv/bin/celery -A celery.celery_config.celery_app worker --loglevel=info --concurrency=4
directory=$(pwd)
user=$USER
numprocs=1
stdout_logfile=/var/log/celery/worker.log
stderr_logfile=/var/log/celery/worker.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:celery-beat]
command=$(pwd)/venv/bin/celery -A celery.celery_config.celery_app beat --loglevel=info
directory=$(pwd)
user=$USER
numprocs=1
stdout_logfile=/var/log/celery/beat.log
stderr_logfile=/var/log/celery/beat.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:flower]
command=$(pwd)/venv/bin/celery -A celery.celery_config.celery_app flower --port=5555
directory=$(pwd)
user=$USER
numprocs=1
stdout_logfile=/var/log/celery/flower.log
stderr_logfile=/var/log/celery/flower.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600

[program:flask-app]
command=$(pwd)/venv/bin/python main.py
directory=$(pwd)
user=$USER
numprocs=1
stdout_logfile=/var/log/flask/app.log
stderr_logfile=/var/log/flask/app.log
autostart=true
autorestart=true
startsecs=10
stopwaitsecs=600
EOF
    
    # Criar diretórios de log
    sudo mkdir -p /var/log/celery /var/log/flask
    sudo chown $USER:$USER /var/log/celery /var/log/flask
    
    echo "🔄 Recarregando supervisor..."
    sudo supervisorctl reread
    sudo supervisorctl update
    
    echo "🚀 Iniciando serviços..."
    sudo supervisorctl start celery-worker
    sudo supervisorctl start celery-beat
    sudo supervisorctl start flower
    sudo supervisorctl start flask-app
    
    echo "⏳ Aguardando serviços iniciarem..."
    sleep 5
    
    echo "📊 Verificando status dos serviços..."
    sudo supervisorctl status
    
    echo "✅ Deploy concluído!"
    echo "🌐 Serviços disponíveis:"
    echo "   - Flask API: http://localhost:8000"
    echo "   - Flower: http://localhost:5555"
    echo "   - Redis: localhost:6379"
    
else
    echo "❌ Uso: ./deploy.sh [docker|manual]"
    echo ""
    echo "Opções:"
    echo "  docker  - Deploy usando Docker Compose"
    echo "  manual  - Deploy manual usando Supervisor"
    echo ""
    echo "Exemplo:"
    echo "  ./deploy.sh docker"
    echo "  ./deploy.sh manual"
    exit 1
fi

echo ""
echo "📋 Comandos úteis:"
echo "  - Ver logs: docker-compose logs -f (Docker) ou tail -f /var/log/celery/worker.log (Manual)"
echo "  - Parar: docker-compose down (Docker) ou sudo supervisorctl stop all (Manual)"
echo "  - Status: docker-compose ps (Docker) ou sudo supervisorctl status (Manual)"
echo ""
echo "📖 Documentação completa: PRODUCAO.md" 