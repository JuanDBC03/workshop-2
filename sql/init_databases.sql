-- Initialize operational source and analytical data warehouse databases
SELECT 'CREATE DATABASE grammy_source'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'grammy_source')\gexec

SELECT 'CREATE DATABASE music_dw'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'music_dw')\gexec
