-- invoices (faturas)
CREATE TABLE invoices (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_no TEXT NOT NULL UNIQUE,
  atcud TEXT,
  company_id TEXT REFERENCES companies(company_id),
  customer_id TEXT,
  invoice_date DATE,
  invoice_status_date DATE,
  invoice_status_time TIME,
  hash_extract TEXT,
  end_date DATE,
  tax_payable NUMERIC(10,2),
  net_total NUMERIC(10,2),
  gross_total NUMERIC(10,2),
  payment_amount NUMERIC(10,2),
  tax_type TEXT,
  filial TEXT,
  created_at TIMESTAMP DEFAULT now()
);

-- companies (empresa emissora)
CREATE TABLE companies (
  company_id TEXT PRIMARY KEY,
  company_name TEXT,
  address_detail TEXT,
  city TEXT,
  postal_code TEXT,
  country TEXT
);

-- invoice_lines (itens da fatura)
CREATE TABLE invoice_lines (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_id UUID REFERENCES invoices(id) ON DELETE CASCADE,
  line_number INTEGER,
  product_code TEXT,
  description TEXT,
  quantity NUMERIC(10,2),
  unit_price NUMERIC(10,2),
  credit_amount NUMERIC(10,2),
  tax_percentage NUMERIC(5,2),
  price_with_iva NUMERIC(10,2)
);

--  invoice_files (arquivo original processado)

CREATE TABLE invoice_files (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  filename TEXT,
  data_processamento TIMESTAMP,
  total_faturas INTEGER,
  created_at TIMESTAMP DEFAULT now()
);


--  invoice_file_links (ligação entre arquivo e fatura)
CREATE TABLE invoice_file_links (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  invoice_file_id UUID REFERENCES invoice_files(id) ON DELETE CASCADE,
  invoice_id UUID REFERENCES invoices(id) ON DELETE CASCADE
);


create table open_gcs_json (
  loja_id text primary key,
  data jsonb not null,
  filial text ,
  nif text,
  updated_at timestamptz default now()
);

CREATE TABLE filiais (
  filial_id TEXT PRIMARY KEY,
  filial_number TEXT UNIQUE,  -- valor único por filial
  company_id TEXT REFERENCES companies(company_id) ON DELETE CASCADE,
  nome TEXT NOT NULL,
  endereco TEXT,
  cidade TEXT,
  codigo_postal TEXT,
  pais TEXT,
  created_at TIMESTAMP DEFAULT now()
);
