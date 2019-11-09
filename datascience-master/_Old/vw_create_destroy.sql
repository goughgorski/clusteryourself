SELECT 'CREATE OR REPLACE VIEW ' || table_schema || '.' || table_name ||  ' AS ' || regexp_replace(regexp_replace(view_definition, '\r|\n', ' ', 'g'), '\s+', ' ', 'g') 
  FROM information_schema.views
 WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
   	AND table_schema like 'datasci%'
 	AND table_name !~ '^pg_'