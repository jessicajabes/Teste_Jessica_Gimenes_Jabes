# Script para executar SQLs e importar CSVs no container Docker PostgreSQL

Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "Importacao de dados - Teste 3" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan

# Configuracoes do banco de dados
$CONTAINER = "intuitive-care"
$DB = "intuitive_care"
$USER = "jessica"
$env:PGPASSWORD = "1234"

# Caminhos dos arquivos
$SCRIPT_DIR = Split-Path -Parent $MyInvocation.MyCommand.Path
$DOWNLOADS = Join-Path $SCRIPT_DIR "..\downloads"
$DDL_SQL = Join-Path $SCRIPT_DIR "01_ddl.sql"
$IMPORT_SQL = Join-Path $SCRIPT_DIR "04_import_data.sql"
$ANALYTICS_SQL = Join-Path $SCRIPT_DIR "03_analytics.sql"

Write-Host "`nPasso 1/4: Criando tabelas (DDL)..." -ForegroundColor Yellow
docker cp "$DDL_SQL" "${CONTAINER}:/tmp/" 2>&1 | Out-Null
docker exec $CONTAINER psql -U $USER -d $DB -f "/tmp/01_ddl.sql" 2>&1 | Out-Null
Write-Host "OK - Tabelas criadas" -ForegroundColor Green

Write-Host "`nPasso 2/4: Preparando arquivos CSV..." -ForegroundColor Yellow
docker exec $CONTAINER mkdir -p /tmp/csvs 2>&1 | Out-Null

$count = 0

# Extrair ZIPs necessários
$zipConsolidados = Join-Path $DOWNLOADS "1-trimestres_consolidados\consolidado_despesas.zip"
$zipAgregados = Join-Path $DOWNLOADS "2-tranformacao_validacao\Teste_Jessica_Jabes.zip"

$extractConsolidados = Join-Path $DOWNLOADS "1-trimestres_consolidados\extracted"
$extractAgregados = Join-Path $DOWNLOADS "2-tranformacao_validacao\extracted"

if (Test-Path $zipConsolidados) {
    Expand-Archive -Path $zipConsolidados -DestinationPath $extractConsolidados -Force
}

if (Test-Path $zipAgregados) {
    Expand-Archive -Path $zipAgregados -DestinationPath $extractAgregados -Force
}

if (Test-Path "$DOWNLOADS/operadoras/operadoras_ativas.csv") {
    docker cp "$DOWNLOADS/operadoras/operadoras_ativas.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - operadoras_ativas.csv" -ForegroundColor Green
    $count++
}

if (Test-Path "$DOWNLOADS/operadoras/operadoras_canceladas.csv") {
    docker cp "$DOWNLOADS/operadoras/operadoras_canceladas.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - operadoras_canceladas.csv" -ForegroundColor Green
    $count++
}

if (Test-Path "$extractConsolidados/sinistro_sem_deducoes.csv") {
    docker cp "$extractConsolidados/sinistro_sem_deducoes.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - sinistro_sem_deducoes.csv" -ForegroundColor Green
    $count++
}

if (Test-Path "$extractConsolidados/consolidado_despesas_sinistros_c_deducoes.csv") {
    docker cp "$extractConsolidados/consolidado_despesas_sinistros_c_deducoes.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - consolidado_despesas_sinistros_c_deducoes.csv" -ForegroundColor Green
    $count++
}

if (Test-Path "$extractAgregados/despesas_agregadas.csv") {
    docker cp "$extractAgregados/despesas_agregadas.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - despesas_agregadas.csv" -ForegroundColor Green
    $count++
}

if (Test-Path "$extractAgregados/despesas_agregadas_c_deducoes.csv") {
    docker cp "$extractAgregados/despesas_agregadas_c_deducoes.csv" "${CONTAINER}:/tmp/csvs/" 2>&1 | Out-Null
    Write-Host "  - despesas_agregadas_c_deducoes.csv" -ForegroundColor Green
    $count++
}

Write-Host "OK - $count arquivos copiados" -ForegroundColor Green

Write-Host "`nPasso 3/4: Importando dados..." -ForegroundColor Yellow
docker cp "$IMPORT_SQL" "${CONTAINER}:/tmp/" 2>&1 | Out-Null
docker exec $CONTAINER psql -U $USER -d $DB -f "/tmp/04_import_data.sql" 2>&1 | Out-Null
Write-Host "OK - Dados importados" -ForegroundColor Green

Write-Host "`nPasso 4/4: Resumo da importacao..." -ForegroundColor Yellow
Write-Host ""

docker exec $CONTAINER psql -U $USER -d $DB -c "SELECT 'OPERADORAS' as tabela, COUNT(*) as registros FROM operadoras UNION ALL SELECT 'DESPESAS AGREGADAS SEM DEDUCAO', COUNT(*) FROM despesas_agregadas UNION ALL SELECT 'DESPESAS AGREGADAS COM DEDUCAO', COUNT(*) FROM despesas_agregadas_c_deducoes UNION ALL SELECT 'CONSOLIDADOS SEM DEDUCAO', COUNT(*) FROM consolidados_despesas UNION ALL SELECT 'CONSOLIDADOS COM DEDUCAO', COUNT(*) FROM consolidados_despesas_c_deducoes;" 2>&1 | Select-Object -Skip 2

Write-Host ""
Write-Host "Status das operadoras:" -ForegroundColor Cyan
docker exec $CONTAINER psql -U $USER -d $DB -c "SELECT status, COUNT(*) as quantidade FROM operadoras GROUP BY status ORDER BY status;" 2>&1 | Select-Object -Skip 2


Write-Host ""
Write-Host "Despesas por trimestre (com deducao):" -ForegroundColor Cyan
docker exec $CONTAINER psql -U $USER -d $DB -c "SELECT trimestre, COUNT(*) as registros, ROUND(SUM(valor_despesas)::numeric, 2) as total_despesas FROM consolidados_despesas_c_deducoes GROUP BY trimestre ORDER BY trimestre;" 2>&1 | Select-Object -Skip 2

Write-Host ""
Write-Host "Totais de despesas agregadas:" -ForegroundColor Cyan
$totais = docker exec $CONTAINER psql -U $USER -d $DB -t -A -F "|" -c "SELECT 'SEM DEDUCAO' AS tipo, ROUND(SUM(total_despesas)::numeric, 2) AS total FROM despesas_agregadas UNION ALL SELECT 'COM DEDUCAO', ROUND(SUM(total_despesas)::numeric, 2) FROM despesas_agregadas_c_deducoes;" 2>&1
$linhasTotais = $totais | Where-Object { $_ -match '\|' }
foreach ($linha in $linhasTotais) {
    $partes = $linha -split '\|'
    if ($partes.Length -ge 2) {
        Write-Host (" {0} | {1}" -f $partes[0].Trim(), $partes[1].Trim())
    }
}

Write-Host ""
Write-Host "=======================================" -ForegroundColor Green
Write-Host "Importacao concluida com sucesso!" -ForegroundColor Green
Write-Host "=======================================" -ForegroundColor Green

Write-Host "" 
Write-Host "Passo 5/5: Executando analytics (03_analytics.sql)..." -ForegroundColor Yellow
docker cp "$ANALYTICS_SQL" "${CONTAINER}:/tmp/" 2>&1 | Out-Null
docker exec $CONTAINER psql -U $USER -d $DB -f "/tmp/03_analytics.sql"
Write-Host "" 
Write-Host "OK - Analytics executado" -ForegroundColor Green
