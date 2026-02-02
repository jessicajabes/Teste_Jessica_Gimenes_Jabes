# =======================================================
# EXECUTAR UM EXERCÍCIO POR VEZ
# =======================================================

$ROOT = Split-Path -Parent $MyInvocation.MyCommand.Path

function Ensure-Containers {
    # Garantir que o container de backend está rodando (necessário para testes 1 e 2)
    $backendContainer = docker ps --format "{{.Names}}" | Select-String "intuitive-care-integracao-api"
    if (-not $backendContainer) {
        Write-Host "Iniciando container de integracao API..." -ForegroundColor Yellow
        docker-compose up -d integracao_api_publica
        Start-Sleep -Seconds 15
    }
}

Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "Executar um exercicio" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "1 - Teste 1 (Integracao API Publica)" -ForegroundColor White
Write-Host "2 - Teste 2 (Transformacao e Validacao)" -ForegroundColor White
Write-Host "3 - Teste 3 (Banco de Dados: import + analytics)" -ForegroundColor White
Write-Host "4 - Teste 4 (API + Frontend)" -ForegroundColor White
Write-Host "Q - Sair" -ForegroundColor White

$opcao = Read-Host "Selecione uma opcao"

if ($opcao -eq 'Q' -or $opcao -eq 'q') {
    Write-Host "Saindo..." -ForegroundColor Yellow
    exit 0
}

Ensure-Containers

switch ($opcao) {
    '1' {
        Write-Host "Executando Teste 1..." -ForegroundColor Cyan
        docker exec -i intuitive-care-integracao-api python /app/1-integracao_api_publica/main.py
    }
    '2' {
        Write-Host "Executando Teste 2..." -ForegroundColor Cyan
        docker exec -i intuitive-care-integracao-api python /app/2-transformacao_validacao/main.py
    }
    '3' {
        Write-Host "Executando Teste 3..." -ForegroundColor Cyan
        $importScript = Join-Path $ROOT "backend\3-teste_de_banco_de_dados\import_csvs.ps1"
        powershell -ExecutionPolicy Bypass -File $importScript
    }
    '4' {
        Write-Host "Executando Teste 4..." -ForegroundColor Cyan
        docker-compose up -d api_operadoras frontend_operadoras
        Write-Host "Frontend: http://localhost:5173" -ForegroundColor Yellow
        Write-Host "API:      http://localhost:8000" -ForegroundColor Yellow
    }
    Default {
        Write-Host "Opcao invalida." -ForegroundColor Red
        exit 1
    }
}
