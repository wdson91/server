import os
from celery import Celery
from dotenv import load_dotenv

load_dotenv()

# Configuração do Celery
CELERY_BROKER_URL = os.getenv('REDIS_URL', 'redis://localhost:6379/0')
CELERY_RESULT_BACKEND = os.getenv('REDIS_URL', 'redis://localhost:6379/0')

# Criar instância do Celery
celery_app = Celery(
    'saft_processor',
    broker=CELERY_BROKER_URL,
    backend=CELERY_RESULT_BACKEND,
    include=['tasks']
)

# Configurações do Celery
celery_app.conf.update(
    task_serializer='json',
    accept_content=['json'],
    result_serializer='json',
    timezone='Europe/Lisbon',
    enable_utc=True,
    task_track_started=True,
    task_time_limit=30 * 60,  # 30 minutos
    task_soft_time_limit=25 * 60,  # 25 minutos
    worker_prefetch_multiplier=1,
    task_acks_late=True,
    worker_max_tasks_per_child=1000,
)

# Importar tarefas para garantir registro
import tasks

# Configuração para tarefas periódicas
celery_app.conf.beat_schedule = {
    'download-sftp-and-queue-files-every-5-minutes': {
        'task': 'tasks.download_and_queue_sftp_files',
        'schedule': 300.0,  # 5 minutos
    },
} 