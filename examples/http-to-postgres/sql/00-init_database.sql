--
-- cleaning up if needed
--
drop database if exists "http_to_postgres_database";
drop user if exists "mano";

--
-- creating user and database;
--
create user "mano" with encrypted password 'brau';
create database "http_to_postgres_database" with owner "mano";
