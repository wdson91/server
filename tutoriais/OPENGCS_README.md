# Sistema de Processamento OpenGCs

Este sistema processa arquivos XML OpenGCs do SFTP, converte-os para JSON e salva os dados no Supabase.

## 🚀 Funcionalidades

- **Download automático** de arquivos OpenGCs do SFTP a cada 5 minutos
- **Conversão XML para JSON** com parsing estruturado
- **Suporte a múltiplas codificações** (UTF-8, Latin-1, ISO-8859-1, CP1252)
- **Inserção no Supabase** na tabela `open_gcs_json`
- **Processamento assíncrono** com Celery
- **Exclusão automática** de arquivos após processamento bem-sucedido

## 📊 Estrutura de Dados

### Tabela do Supabase: `open_gcs_json`

```sql
CREATE TABLE open_gcs_json (
  loja_id text primary key,
  nif text,
  filial text,
  data jsonb not null,
  updated_at timestamptz default now()
);
```

**Lógica de Inserção/Atualização:**
- **Busca por NIF e Filial**: O sistema busca registros existentes usando `nif` e `filial`
- **Atualização**: Se encontrar registro existente, atualiza apenas os dados JSON
- **Inserção**: Se não encontrar, cria novo registro com `loja_id` único
- **Identificador Único**: `loja_id` é gerado como `{nif}_{filial}` ou apenas `{nif}` se não houver filial

### Tabela do Supabase: `filiais`

```sql
CREATE TABLE filiais (
  filial_id TEXT PRIMARY KEY,
  filial_number TEXT UNIQUE,  -- Campo único por filial
  company_id TEXT REFERENCES companies(company_id) ON DELETE CASCADE,
  nome TEXT NOT NULL,
  endereco TEXT,
  cidade TEXT,
  codigo_postal TEXT,
  pais TEXT,
  created_at TIMESTAMP DEFAULT now()
);
```

### Estrutura do JSON Processado

```json
{
  "arquivo_origem": "opengcs-123456789.xml",
  "data_processamento": "2025-01-15T10:30:00",
  "opengcs_total": 0.90,
  "opengcs_count": 4,
  "gcs": [
    {
      "number": 1,
      "open_time": "0000-00-00T00:00:00",
      "last_time": "2025-07-14T20:15:41",
      "guests": 0,
      "operator_no": 99,
      "operator_name": "Técnico",
      "start_operator_no": 0,
      "start_operator_name": "",
      "total": 0.30
    }
  ]
}
```

## 📁 Estrutura de Arquivos SFTP

O sistema procura por arquivos com diferentes padrões em cada pasta NIF:

```
uploads/
├── 123456789/
│   ├── opengcs-123456789.xml
│   ├── FR202Y2025_7-Gramido.xml
│   ├── FR201803Y2025_329-Douro.xml
│   └── NC987654321.xml
├── 987654321/
│   ├── opengcs-987654321.xml
│   └── FR202Y2025_15-Porto.xml
```

### Padrões de Arquivos:
- **OpenGCs**: `opengcs-{nif}-{filial}.xml` (ex: `opengcs-514244208-Douro.xml`)
- **Faturas**: `FR{ano}Y{ano}_{numero}-{filial}.xml` (ex: `FR202Y2025_7-Gramido.xml`)
- **Notas de Crédito**: `NC{ano}Y{ano}_{numero}-{filial}.xml`

### Extração de Filial:
- **Faturas**: `FR{ano}Y{ano}_{numero}-{filial}.xml`
- **Exemplo**: `FR202Y2025_7-Gramido.xml` → Filial: `Gramido`
- **OpenGCs**: `opengcs-{nif}-{filial}.xml`
- **Exemplo**: `opengcs-514244208-Douro.xml` → Filial: `Douro`
- **Tratamento**: Remove automaticamente a extensão `.xml` se presente

## 🔧 Como Usar

### 1. Execução Automática

O sistema executa automaticamente a cada 5 minutos, baixando e processando arquivos OpenGCs.

### 2. Execução Manual via API

#### Baixar e processar arquivos OpenGCs:
```bash
curl -X POST http://localhost:5000/api/download-opengcs-queue
```

#### Processar arquivos OpenGCs (alias):
```bash
curl -X POST http://localhost:5000/api/process-opengcs
```

### 3. Verificar Status de Tarefa

```bash
curl http://localhost:5000/api/task-status/{task_id}
```

## 📋 Processo de Processamento

### Para Arquivos OpenGCs:
1. **Download**: Baixa arquivos `opengcs-{nif}-{filial}` de cada pasta NIF
2. **Conversão**: Converte XML para JSON estruturado
3. **Inserção**: Salva dados no Supabase usando `nif` como chave primária
4. **Exclusão**: Remove arquivo do SFTP após processamento bem-sucedido
5. **Limpeza**: Remove arquivo local após processamento

### Para Faturas (FR/NC):
1. **Download**: Baixa arquivos `FR{nif}` e `NC{nif}` de cada pasta NIF
2. **Conversão**: Converte XML para JSON estruturado
3. **Inserção**: Salva dados no Supabase em múltiplas tabelas:
   - `companies`: Dados da empresa
   - `filiais`: Dados da filial (se filial existir)
   - `invoices`: Dados da fatura
   - `invoice_lines`: Linhas da fatura
   - `invoice_files`: Arquivo processado
   - `invoice_file_links`: Links entre arquivo e fatura
4. **Exclusão**: Remove arquivo do SFTP após processamento bem-sucedido
5. **Limpeza**: Remove arquivo local após processamento

## 🔍 Logs e Monitoramento

O sistema registra:
- Arquivos baixados do SFTP
- Conversão XML para JSON
- Inserção no Supabase
- Exclusão de arquivos
- Erros e avisos

### Exemplo de Logs:
```
🔄 Iniciando download de arquivos OpenGCs SFTP...
📊 Total de arquivos OpenGCs baixados: 2
📋 Tarefa OpenGCs criada para: ./downloads/opengcs-123456789.xml (ID: abc123)
✅ Arquivo OpenGCs processado com sucesso
```

## ⚙️ Configurações

### Variáveis de Ambiente

```bash
# Limite de arquivos processados por vez
MAX_FILES_PER_BATCH=50

# Configurações SFTP (já configuradas no código)
# host=13.48.69.154
# username=sftpuser
# password=fd4d41fd-8e17-3cfa-a193-34601e70baf8
```

### Agendamento

Para alterar o intervalo de execução, edite `celery_config.py`:

```python
celery_app.conf.beat_schedule = {
    'download-opengcs-and-queue-files-every-5-minutes': {
        'task': 'tasks.download_and_queue_opengcs_files',
        'schedule': 300.0,  # 5 minutos (em segundos)
    },
}
```

## 🚨 Tratamento de Erros

### Cenários de Erro

1. **Arquivo não encontrado**: Log de erro, tarefa falha
2. **XML inválido**: Log de erro, arquivo não excluído do SFTP
3. **Falha na inserção**: Log de erro, arquivo não excluído do SFTP
4. **Falha na exclusão SFTP**: Log de aviso, arquivo local removido
5. **Problemas de codificação**: Sistema tenta múltiplas codificações automaticamente

### Recuperação

- Arquivos não processados serão tentados novamente no próximo ciclo
- Dados já inseridos no Supabase não são duplicados (upsert)
- Logs detalhados para debugging
- **Tratamento automático de codificação**: Sistema tenta UTF-8, Latin-1, ISO-8859-1 e CP1252

## 📊 Monitoramento

### Flower (Interface Web)

Acesse `http://localhost:5555` para monitorar:
- Tarefas em execução
- Histórico de tarefas
- Logs em tempo real

### Logs do Sistema

```bash
# Ver logs do Celery
celery -A celery_config.celery_app worker --loglevel=info

# Ver logs do Beat (agendador)
celery -A celery_config.celery_app beat --loglevel=info
```

## 🔧 Troubleshooting

### Problemas Comuns

1. **SFTP não conecta**:
   - Verificar credenciais no `sftp_connection.py`
   - Verificar conectividade de rede

2. **Supabase não conecta**:
   - Verificar `SUPABASE_URL` e `SUPABASE_KEY` no `.env`
   - Verificar se a tabela `open_gcs_json` existe

3. **Arquivos não são baixados**:
   - Verificar se existem arquivos `opengcs-{nif}` no SFTP
   - Verificar permissões de pasta

4. **Tarefas não processam**:
   - Verificar se worker está rodando
   - Verificar logs do Celery

### Comandos Úteis

```bash
# Verificar status do worker
celery -A celery_config.celery_app inspect active

# Verificar tarefas agendadas
celery -A celery_config.celery_app inspect scheduled

# Limpar tarefas pendentes
celery -A celery_config.celery_app purge
```

## 📈 Performance

### Otimizações

- **Processamento em lote**: Limite configurável de arquivos por vez
- **Upsert**: Evita duplicação de dados
- **Exclusão condicional**: Só exclui arquivo se processamento for bem-sucedido
- **Logs estruturados**: Facilita debugging e monitoramento

### Métricas

O sistema registra:
- Número de arquivos baixados
- Número de GCs processados
- Tempo de processamento
- Taxa de sucesso/erro 