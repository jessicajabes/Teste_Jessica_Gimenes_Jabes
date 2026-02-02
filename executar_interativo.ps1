# =======================================================
# EXECUTAR TODOS OS 4 EXERCÍCIOS (SEQUENCIAL)
# =======================================================

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

# Garantir que o container de backend está rodando (necessário para testes 1 e 2)
$backendContainer = docker ps --format "{{.Names}}" | Select-String "intuitive-care-integracao-api"
if (-not $backendContainer) {
    Write-Host "Iniciando container de integracao API..." -ForegroundColor Yellow
    docker-compose up -d integracao_api_publica
    Start-Sleep -Seconds 15
}

Write-Host "" 
Write-Host "[1/4] Executando Teste 1 - Integracao API Publica..." -ForegroundColor Cyan
docker exec -i intuitive-care-integracao-api python /app/1-integracao_api_publica/main.py

Write-Host "" 
Write-Host "[2/4] Executando Teste 2 - Transformacao e Validacao..." -ForegroundColor Cyan
docker exec -i intuitive-care-integracao-api python /app/2-transformacao_validacao/main.py

Write-Host "" 
Write-Host "[3/4] Executando Teste 3 - Banco de Dados (import + analytics)..." -ForegroundColor Cyan
$importScript = Join-Path $ROOT "testes\3-teste_de_banco_de_dados\import_csvs.ps1"
powershell -ExecutionPolicy Bypass -File $importScript

Write-Host "" 
Write-Host "[4/4] Executando Teste 4 - API + Frontend..." -ForegroundColor Cyan
docker-compose up -d api_operadoras frontend_operadoras

Write-Host "" 
Write-Host "=======================================" -ForegroundColor Green
Write-Host "TODOS OS EXERCICIOS EXECUTADOS" -ForegroundColor Green
Write-Host "Frontend: http://localhost:5173" -ForegroundColor Yellow
Write-Host "API:      http://localhost:8000" -ForegroundColor Yellow
Write-Host "=======================================" -ForegroundColor Green
