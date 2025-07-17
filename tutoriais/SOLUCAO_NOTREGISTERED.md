# 🔧 Solução para Erro NotRegistered

## ❌ Erro: `NotRegistered('tasks.download_and_queue_sftp_files')`

Este erro indica que o Celery não consegue encontrar a tarefa `download_and_queue_sftp_files`. Vamos resolver isso!

## 🚀 Solução Rápida

### 1. **Parar todos os processos Celery**
```bash
# Parar processos existentes
pkill -f celery
```

### 2. **Testar configuração**
```bash
# Testar se as tarefas estão registradas
python test_celery_tasks.py
```

### 3. **Reiniciar com script especial**
```bash
# Reiniciar Celery com tarefas corretas
python restart_celery.py
```

### 4. **Ou reiniciar manualmente**
```bash
# Terminal 1 - Worker
celery -A celery_config.celery_app worker --loglevel=info --concurrency=2

# Terminal 2 - Beat
celery -A celery_config.celery_app beat --loglevel=info

# Terminal 3 - Flower
celery -A celery_config.celery_app flower --port=5555
```

## 🔍 Diagnóstico

### **Verificar se as tarefas estão definidas:**
```bash
# Verificar se o arquivo tasks.py tem as tarefas
grep -n "@celery_app.task" tasks.py
```

**Deve mostrar:**
```
226:@celery_app.task
271:@celery_app.task
307:@celery_app.task
```

### **Verificar imports:**
```python
# No arquivo celery_config.py deve ter:
import tasks
```

### **Verificar se o worker está carregando as tarefas:**
```bash
# Ver tarefas registradas
celery -A celery_config.celery_app inspect registered
```

## 🛠️ Soluções Detalhadas

### **Solução 1: Reiniciar Worker**
```bash
# 1. Parar worker
pkill -f "celery.*worker"

# 2. Iniciar worker novamente
celery -A celery_config.celery_app worker --loglevel=info
```

### **Solução 2: Limpar Cache Redis**
```bash
# 1. Conectar ao Redis
redis-cli

# 2. Limpar cache
FLUSHALL

# 3. Sair
exit

# 4. Reiniciar Celery
python restart_celery.py
```

### **Solução 3: Verificar Estrutura de Arquivos**
```bash
# Verificar se todos os arquivos existem
ls -la *.py
```

**Arquivos necessários:**
- `celery_config.py`
- `tasks.py`
- `main.py`

### **Solução 4: Testar Imports**
```python
# Criar arquivo test_imports.py
from celery_config import celery_app
from tasks import download_and_queue_sftp_files

print("✅ Imports funcionando!")
print(f"Tarefas registradas: {list(celery_app.tasks.keys())}")
```

## 🔧 Comandos de Debug

### **Verificar tarefas registradas:**
```bash
celery -A celery_config.celery_app inspect registered
```

### **Verificar worker ativo:**
```bash
celery -A celery_config.celery_app inspect active
```

### **Verificar estatísticas:**
```bash
celery -A celery_config.celery_app inspect stats
```

### **Verificar logs:**
```bash
# Ver logs do worker
tail -f celery.log
```

## 📋 Checklist de Verificação

- [ ] **Redis está rodando**: `redis-cli ping`
- [ ] **Arquivo tasks.py existe**: `ls tasks.py`
- [ ] **Tarefas estão definidas**: `grep "@celery_app.task" tasks.py`
- [ ] **Imports estão corretos**: `python test_celery_tasks.py`
- [ ] **Worker está rodando**: `celery -A celery_config.celery_app inspect active`
- [ ] **Tarefas estão registradas**: `celery -A celery_config.celery_app inspect registered`

## 🚨 Problemas Comuns

### **1. Worker não carrega tarefas**
```bash
# Solução: Reiniciar worker com --loglevel=debug
celery -A celery_config.celery_app worker --loglevel=debug
```

### **2. Tarefas não aparecem no Flower**
```bash
# Solução: Reiniciar Flower
pkill -f flower
celery -A celery_config.celery_app flower --port=5555
```

### **3. Beat não agenda tarefas**
```bash
# Solução: Verificar beat_schedule
cat celery_config.py | grep beat_schedule
```

## ✅ Teste Final

Após aplicar as soluções, teste:

```bash
# 1. Testar tarefas
python test_celery_tasks.py

# 2. Testar endpoint
curl -X POST http://localhost:8000/api/download-sftp-queue

# 3. Verificar no Flower
# Acesse: http://localhost:5555
```

## 🎯 Resultado Esperado

Após as correções, você deve ver:

- ✅ **Worker rodando** sem erros
- ✅ **Tarefas registradas** no Flower
- ✅ **Beat agendando** tarefas a cada 5 minutos
- ✅ **Endpoints funcionando** sem erro NotRegistered

## 📞 Se ainda não funcionar

1. **Verificar logs completos**:
```bash
celery -A celery_config.celery_app worker --loglevel=debug
```

2. **Verificar versões**:
```bash
pip list | grep celery
```

3. **Reinstalar dependências**:
```bash
pip install -r requirements.txt --force-reinstall
```

4. **Limpar e reinstalar**:
```bash
pip uninstall celery flower
pip install celery==5.3.4 flower==2.0.1
``` 