# Sistema de Processamento SFTP com Celery

Este sistema baixa arquivos XML de um servidor SFTP a cada 5 minutos, converte-os para JSON e insere os dados no Supabase usando Celery para processamento assíncrono.

## 🚀 Funcionalidades

- **Download automático** de arquivos XML do SFTP a cada 5 minutos
- **Conversão XML para JSON** com parsing estruturado
- **Inserção em lote** no Supabase para performance otimizada
- **Processamento assíncrono** com Celery
- **Monitoramento** com Flower
- **Limite configurável** de arquivos processados por vez
- **Inserção condicional** de linhas de faturas (só insere se a fatura for inserida com sucesso)

## 📊 Estrutura de Dados

### Tabelas do Supabase
- **companies**: Informações das empresas
- **invoices**: Faturas principais
- **invoice_lines**: Linhas de produtos das faturas
- **invoice_files**: Arquivos processados
- **invoice_file_links**: Links entre arquivos e faturas

### Lógica de Inserção Condicional

O sistema implementa uma lógica de inserção condicional para garantir integridade dos dados:

1. **Empresas**: Sempre inseridas primeiro (upsert)
2. **Faturas**: Inseridas em lote (upsert)
3. **Linhas de Faturas**: **SOMENTE** inseridas se:
   - A fatura correspondente for inserida com sucesso
   - A linha não existir previamente (prevenção de duplicação)
4. **Links**: Inseridos apenas para faturas inseridas com sucesso e que não tenham link existente

```python
# Exemplo da lógica
if invoices_response and invoices_response.data:
    # Só insere linhas se as faturas foram inseridas
    for fatura in data["faturas"]:
        invoice_id = invoice_mapping.get(fatura["InvoiceNo"])
        if invoice_id:  # Fatura inserida com sucesso
            # Inserir linhas desta fatura
            for linha in lines_by_invoice[fatura["InvoiceNo"]]:
                linha["invoice_id"] = invoice_id
                lines_batch.append(linha)
        else:
            # Fatura não foi inserida, ignorar linhas
            logger.warning(f"Fatura {fatura['InvoiceNo']} não foi inserida, linhas serão ignoradas")
```

## 📋 Pré-requisitos

1. **Redis**: Servidor Redis para broker do Celery
2. **Python 3.8+**: Versão do Python
3. **Variáveis de ambiente**: Configuradas no arquivo `.env`

## 🔧 Instalação

1. **Instalar dependências**:
```bash
pip install -r requirements.txt
```

2. **Configurar variáveis de ambiente** (arquivo `.env`):
```env
# Redis
REDIS_URL=redis://localhost:6379/0

# Supabase
SUPABASE_URL=sua_url_do_supabase
SUPABASE_KEY=sua_chave_do_supabase

# SFTP (já configurado no código)
# host=13.48.69.154
# username=sftpuser
# password=sftppass
# port=22

# Configurações de Lote
MAX_FILES_PER_BATCH=50
BATCH_SIZE_COMPANIES=1000
BATCH_SIZE_INVOICES=500
BATCH_SIZE_LINES=2000
BATCH_SIZE_LINKS=500

# Configuração de Limpeza Automática
CLEANUP_AFTER_PROCESSING=true
```

## 🚀 Uso

### 1. Iniciar Redis
```bash
redis-server
```

### 2. Iniciar Celery Worker
```bash
python -m celery -A celery.celery_config worker --loglevel=info
```

### 3. Iniciar Celery Beat (Agendador)
```bash
python -m celery -A celery.celery_config beat --loglevel=info
```

### 4. Iniciar Flower (Monitoramento)
```bash
python -m celery -A celery.celery_config flower --port=5555
```

### 5. Acessar Monitoramento
- **Flower**: http://localhost:5555
- **Redis**: http://localhost:6379

## 📊 Scripts de Teste

### Teste de Conexão com Supabase
```bash
python test_supabase_connection.py
```

### Teste de Processamento Completo
```bash
python test_complete_processing.py
```

### Teste de Invoice Lines
```bash
python test_invoice_lines.py
```

### Teste de Lógica de Inserção Condicional
```bash
python test_invoice_lines_logic.py
```

## 🔍 Monitoramento

### Flower Dashboard
- **URL**: http://localhost:5555
- **Funcionalidades**:
  - Visualizar tarefas em execução
  - Histórico de tarefas
  - Estatísticas de performance
  - Logs em tempo real

### Logs do Sistema
- **Worker**: Logs de processamento de tarefas
- **Beat**: Logs de agendamento
- **Flower**: Logs de monitoramento

## ⚙️ Configurações Avançadas

### Limites de Lote
```env
# Número máximo de arquivos processados por vez
MAX_FILES_PER_BATCH=50

# Tamanhos de lote para inserção
BATCH_SIZE_COMPANIES=1000
BATCH_SIZE_INVOICES=500
BATCH_SIZE_LINES=2000
BATCH_SIZE_LINKS=500
```

### Limpeza Automática
```env
# Remover arquivos após processamento
CLEANUP_AFTER_PROCESSING=true
```

## 🐛 Troubleshooting

### Problemas Comuns

1. **Celery NotRegistered Error**
   - Verificar se as tarefas estão sendo importadas corretamente
   - Executar: `python -c "from tasks import *"`

2. **Faturas não inseridas**
   - Verificar logs do worker
   - Verificar estrutura do XML
   - Verificar conexão com Supabase

3. **Linhas não inseridas**
   - Verificar se as faturas foram inseridas primeiro
   - Verificar estrutura das linhas no XML
   - Verificar logs de inserção condicional

### Logs Importantes
```bash
# Logs do Worker
tail -f celery.log

# Logs do Beat
tail -f beat.log

# Logs do Flower
tail -f flower.log
```

## 📈 Performance

### Otimizações Implementadas
- **Inserção em lote** para melhor performance
- **Limite de arquivos** por processamento
- **Inserção condicional** para evitar dados órfãos
- **Upsert** para evitar duplicatas

### Métricas de Performance
- **Empresas**: ~1000 registros/lote
- **Faturas**: ~500 registros/lote
- **Linhas**: ~2000 registros/lote
- **Links**: ~500 registros/lote

## 🔒 Segurança

### Boas Práticas
- **Variáveis de ambiente** para credenciais
- **Limpeza automática** de arquivos temporários
- **Validação de dados** antes da inserção
- **Logs detalhados** para auditoria

### Configurações de Segurança
```env
# Usar HTTPS para Supabase
SUPABASE_URL=https://...

# Configurar Redis com senha em produção
REDIS_URL=redis://:senha@localhost:6379/0
```

## 📝 Changelog

### v1.2.0
- ✅ Implementada inserção condicional de invoice_lines
- ✅ Melhorada lógica de tratamento de erros
- ✅ Adicionados logs detalhados para debugging
- ✅ Criados scripts de teste específicos

### v1.1.0
- ✅ Implementado sistema de limpeza automática
- ✅ Adicionado limite configurável de arquivos por lote
- ✅ Melhorada performance com inserção em lote

### v1.0.0
- ✅ Sistema básico de download e processamento SFTP
- ✅ Integração com Supabase
- ✅ Processamento assíncrono com Celery
- ✅ Monitoramento com Flower 