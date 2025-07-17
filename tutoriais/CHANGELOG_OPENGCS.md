# Changelog - Funcionalidade OpenGCs

## Versão 1.0.0 - 2025-01-15

### ✨ Novas Funcionalidades

#### 🔧 Processamento de Arquivos OpenGCs
- **Download automático** de arquivos `opengcs-{nif}` do SFTP
- **Conversão XML para JSON** com parsing estruturado
- **Inserção no Supabase** na tabela `open_gcs_json`
- **Exclusão automática** de arquivos após processamento bem-sucedido

#### 📊 Estrutura de Dados
- Nova tabela `open_gcs_json` no Supabase:
  ```sql
  CREATE TABLE open_gcs_json (
    loja_id text primary key,
    nif text,
    filial text,
    data jsonb not null,
    updated_at timestamptz default now()
  );
  ```
- **Lógica de Inserção/Atualização**: Busca por NIF e Filial, atualiza se existir ou insere novo registro
- Nova tabela `filiais` no Supabase:
  ```sql
  CREATE TABLE filiais (
    filial_id TEXT PRIMARY KEY,
    filial_number TEXT UNIQUE,
    company_id TEXT REFERENCES companies(company_id) ON DELETE CASCADE,
    nome TEXT NOT NULL,
    endereco TEXT,
    cidade TEXT,
    codigo_postal TEXT,
    pais TEXT,
    created_at TIMESTAMP DEFAULT now()
  );
  ```

#### 🔄 Tarefas Celery
- `download_and_queue_opengcs_files()`: Baixa arquivos e cria tarefas individuais
- `process_single_opengcs_file()`: Processa arquivo OpenGCs individual
- Agendamento automático a cada 5 minutos

#### 🌐 Endpoints API
- `POST /api/download-opengcs-queue`: Baixar e processar arquivos OpenGCs
- `POST /api/process-opengcs`: Alias para processamento manual

### 🔧 Funções Implementadas

#### Core Functions
- `download_opengcs_files_from_sftp()`: Baixa arquivos do SFTP
- `delete_opengcs_file_from_sftp()`: Exclui arquivos após processamento
- `parse_opengcs_xml_to_json()`: Converte XML para JSON
- `extract_nif_from_filename()`: Extrai NIF do nome do arquivo
- `extract_filial_from_filename()`: Extrai filial do nome do arquivo (Faturas)
- `extract_opengcs_filial_from_filename()`: Extrai filial do nome do arquivo (OpenGCs)
- `insert_opengcs_to_supabase()`: Insere dados no Supabase
- `read_xml_file_with_encoding()`: Lê arquivos XML com múltiplas codificações
- `insert_filiais_batch()`: Insere filiais em lote

#### Processamento de Dados
- Extração de `OpenGCsTotal` e `OpenGCs` do XML
- Processamento de múltiplos elementos `GC`
- Conversão de tipos (string para int/float)
- Tratamento de campos opcionais
- **Suporte a múltiplas codificações**: UTF-8, Latin-1, ISO-8859-1, CP1252
- **Processamento de filiais**: Extração e inserção de dados de filiais das faturas
- **Extração de filial do nome do arquivo**: Padrão `FR{ano}Y{ano}_{numero}-{filial}.xml` (remove .xml automaticamente)

### 📁 Estrutura de Arquivos

#### Arquivos Modificados
- `tasks.py`: Adicionadas funções OpenGCs
- `main.py`: Novos endpoints API
- `celery_config.py`: Agendamento de tarefas

#### Arquivos Criados
- `OPENGCS_README.md`: Documentação completa
- `test_opengcs.py`: Script de testes
- `CHANGELOG_OPENGCS.md`: Este changelog

### 🚀 Como Usar

#### Execução Automática
O sistema executa automaticamente a cada 5 minutos.

#### Execução Manual
```bash
# Via API
curl -X POST http://localhost:5000/api/download-opengcs-queue

# Via Celery
celery -A celery_config.celery_app call tasks.download_and_queue_opengcs_files
```

#### Testes
```bash
# Executar testes
python test_opengcs.py

# Verificar logs
celery -A celery_config.celery_app worker --loglevel=info
```

### 🔍 Monitoramento

#### Logs Estruturados
- Download de arquivos do SFTP
- Conversão XML para JSON
- Inserção no Supabase
- Exclusão de arquivos
- Erros e avisos

#### Flower Interface
- Acesse `http://localhost:5555`
- Monitore tarefas OpenGCs em tempo real

### ⚙️ Configurações

#### Variáveis de Ambiente
```bash
MAX_FILES_PER_BATCH=50  # Limite de arquivos por lote
```

#### Agendamento
```python
# Em celery_config.py
'download-opengcs-and-queue-files-every-5-minutes': {
    'task': 'tasks.download_and_queue_opengcs_files',
    'schedule': 300.0,  # 5 minutos
}
```

### 🚨 Tratamento de Erros

#### Cenários Cobertos
1. **Arquivo não encontrado**: Log de erro, tarefa falha
2. **XML inválido**: Log de erro, arquivo não excluído
3. **Falha na inserção**: Log de erro, arquivo não excluído
4. **Falha na exclusão SFTP**: Log de aviso, arquivo local removido

#### Recuperação
- Arquivos não processados serão tentados novamente
- Upsert evita duplicação de dados
- Logs detalhados para debugging

### 📊 Performance

#### Otimizações
- Processamento em lote configurável
- Upsert para evitar duplicação
- Exclusão condicional de arquivos
- Logs estruturados para monitoramento

#### Métricas
- Número de arquivos baixados
- Número de GCs processados
- Tempo de processamento
- Taxa de sucesso/erro

### 🔧 Próximos Passos

#### Melhorias Futuras
1. **Validação de dados**: Verificar integridade dos XMLs
2. **Retry automático**: Tentativas múltiplas em caso de falha
3. **Métricas avançadas**: Dashboard com estatísticas
4. **Notificações**: Alertas em caso de erro
5. **Backup**: Backup automático dos dados processados

#### Configurações Avançadas
1. **Intervalo configurável**: Permitir alterar intervalo via env
2. **Filtros**: Processar apenas arquivos específicos
3. **Transformações**: Adicionar transformações de dados
4. **Webhooks**: Notificar sistemas externos

### 🐛 Correções Conhecidas

#### Limitações Atuais
1. **Tabela obrigatória**: A tabela `open_gcs_json` deve existir no Supabase
2. **Credenciais SFTP**: Configuradas no código (não via env)
3. **Timezone**: Usa timezone do servidor

#### Correções Implementadas
1. **Erro de Constraint**: Corrigido erro "no unique or exclusion constraint matching the ON CONFLICT specification"
   - **Problema**: Tentativa de usar `nif` como chave de conflito sem constraint único
   - **Solução**: Implementada lógica de busca por NIF e Filial, seguida de update ou insert
   - **Resultado**: Sistema agora busca registros existentes e atualiza dados ou insere novos registros corretamente

#### Workarounds
1. Criar tabela manualmente no Supabase
2. Configurar credenciais SFTP no código
3. Verificar timezone do servidor

### 📝 Notas de Implementação

#### Decisões Técnicas
1. **Upsert**: Usado para evitar duplicação de dados
2. **Exclusão condicional**: Só exclui se processamento for bem-sucedido
3. **Logs estruturados**: Facilita debugging e monitoramento
4. **Testes unitários**: Cobertura de funções principais

#### Compatibilidade
- Compatível com sistema existente
- Não interfere com processamento de faturas
- Usa mesma infraestrutura (Celery, Redis, Supabase) 