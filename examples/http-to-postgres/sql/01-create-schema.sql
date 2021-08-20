--
-- cleaning up if needed
--
drop schema if exists "http_to_postgres_schema" cascade;

--
-- creating schema
--
create schema "http_to_postgres_schema";
set search_path to "http_to_postgres_schema";