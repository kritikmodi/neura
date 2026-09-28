# Local disposable fixture credentials only. Never use these in production.
export NEURA_TEST_POSTGRES_DSN='host=127.0.0.1 port=55432 user=neura_reader password=neura-test-reader-only dbname=neura sslmode=disable'
export AWS_ACCESS_KEY_ID='neura-reader'
export AWS_SECRET_ACCESS_KEY='neura-test-readonly-storage'
export AWS_DEFAULT_REGION='us-east-1'
export NEURA_API_TOKEN='neura-local-test-api-token-change-me'
export NEURA_PG_PASSWORD='neura-local-test-pg-password-change-me'
