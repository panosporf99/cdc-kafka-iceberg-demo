CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS public.customers (
  id         SERIAL PRIMARY KEY,
  name       TEXT NOT NULL,
  email      TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT now()
);

INSERT INTO public.customers (name,email) VALUES
('Ada Lovelace','ada@example.com'),
('Alan Turing','alan@example.com')
ON CONFLICT DO NOTHING;
