# Verificar se os containers estão rodando
$containers = docker ps --format "{{.Names}}" | Select-String "intuitive-care"

if (-not $containers) {
    Write-Host "Iniciando containers..." -ForegroundColor Yellow
    docker-compose up -d
    Start-Sleep -Seconds 10
}

Write-Host "Conectando ao container de forma interativa..." -ForegroundColor Green
Write-Host ""

# Executar de forma interativa
docker exec -it intuitive-care-integracao-api python main.py
