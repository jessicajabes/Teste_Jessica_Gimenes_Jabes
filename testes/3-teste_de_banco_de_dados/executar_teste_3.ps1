param(
    [string]$Container = "intuitive-care",
    [string]$User = "jessica",
    [string]$Database = "intuitive_care",
    [string]$DownloadsPath = "c:\Users\jessi\Documents\PROJETOS\Teste_Jessica_Jabes\testes\downloads"
)

$InfoColor = "Cyan"
$SuccessColor = "Green"
$ErrorColor = "Red"
$WarningColor = "Yellow"

Write-Host ""
Write-Host "==== TESTE 3 - BANCO DE DADOS E ANALYTICS ===" -ForegroundColor $InfoColor
Write-Host "Container: $Container | Database: $Database" -ForegroundColor $InfoColor
Write-Host ""

# ============================================================================
# PASSO 1: PREPARAR DADOS
# ============================================================================

Write-Host "PASSO 1: Preparar e copiar arquivos" -ForegroundColor $InfoColor
Write-Host "-----------------------------------" -ForegroundColor $InfoColor
Write-Host ""

Write-Host ">>> Extraindo ZIPs automaticamente..." -ForegroundColor $WarningColor

# Extrair ZIP do Teste 1
$Zip1 = "$DownloadsPath\1-trimestres_consolidados\consolidado_despesas.zip"
$Extract1 = "$DownloadsPath\1-trimestres_consolidados\extracted"
if (Test-Path $Zip1) {
    Write-Host "  Extraindo consolidado_despesas.zip..." -ForegroundColor $WarningColor -NoNewline
    Expand-Archive -Path $Zip1 -DestinationPath $Extract1 -Force 2>&1 | Out-Null
    Write-Host " OK" -ForegroundColor $SuccessColor
} else {
    Write-Host "  X consolidado_despesas.zip nao encontrado" -ForegroundColor $ErrorColor
}

# Extrair ZIP do Teste 2
$Zip2 = "$DownloadsPath\2-tranformacao_validacao\Teste_Jessica_Jabes.zip"
$Extract2 = "$DownloadsPath\2-tranformacao_validacao\extracted"
if (Test-Path $Zip2) {
    Write-Host "  Extraindo Teste_Jessica_Jabes.zip..." -ForegroundColor $WarningColor -NoNewline
    Expand-Archive -Path $Zip2 -DestinationPath $Extract2 -Force 2>&1 | Out-Null
    Write-Host " OK" -ForegroundColor $SuccessColor
} else {
    Write-Host "  X Teste_Jessica_Jabes.zip nao encontrado" -ForegroundColor $ErrorColor
}

Write-Host ""
Write-Host ">>> Gerando operadoras_clean.csv..." -ForegroundColor $WarningColor

# Criar arquivo limpo combinando ativas + canceladas
$AtivosPath = "$DownloadsPath\operadoras\operadoras_ativas.csv"
$CanceladosPath = "$DownloadsPath\operadoras\operadoras_canceladas.csv"
$CleanPath = "$DownloadsPath\operadoras\operadoras_clean.csv"

if ((Test-Path $AtivosPath) -and (Test-Path $CanceladosPath)) {
    # Ler arquivos originais com encoding UTF-8
    $Ativos = Import-Csv $AtivosPath -Delimiter ';' -Encoding UTF8
    $Cancelados = Import-Csv $CanceladosPath -Delimiter ';' -Encoding UTF8
    
    # Processar ativos
    $ProcessadosAtivos = $Ativos | Select-Object @{N='reg_ans';E={$_.REGISTRO_OPERADORA}}, CNPJ, @{N='razao_social';E={$_.Razao_Social}}, Modalidade, UF, @{N='status';E={'ATIVO'}}
    
    # Processar cancelados
    $ProcessadosCancelados = $Cancelados | Select-Object @{N='reg_ans';E={$_.REGISTRO_OPERADORA}}, CNPJ, @{N='razao_social';E={$_.Razao_Social}}, Modalidade, UF, @{N='status';E={'CANCELADO'}}
    
    # Combinar e exportar com UTF-8
    $Todos = $ProcessadosAtivos + $ProcessadosCancelados
    $Todos | Export-Csv $CleanPath -Delimiter ';' -NoTypeInformation -Encoding UTF8
    
    Write-Host "  OK Arquivo criado: $($Todos.Count) operadoras" -ForegroundColor $SuccessColor
} else {
    Write-Host "  AVISO: Arquivos ativas/canceladas nao encontrados, usando operadoras_clean.csv existente" -ForegroundColor $WarningColor
}

Write-Host ""
Write-Host ">>> Localizando arquivos CSV..." -ForegroundColor $WarningColor

$FilesNeeded = @(
    @{Name = "consolidado_despesas_sinistros_c_deducoes.csv"; Path = "$DownloadsPath\1-trimestres_consolidados\extracted\consolidado_despesas_sinistros_c_deducoes.csv"},
    @{Name = "sinistro_sem_deducoes.csv"; Path = "$DownloadsPath\1-trimestres_consolidados\extracted\sinistro_sem_deducoes.csv"},
    @{Name = "despesas_agregadas.csv"; Path = "$DownloadsPath\2-tranformacao_validacao\extracted\despesas_agregadas.csv"},
    @{Name = "despesas_agregadas_c_deducoes.csv"; Path = "$DownloadsPath\2-tranformacao_validacao\extracted\despesas_agregadas_c_deducoes.csv"},
    @{Name = "operadoras_clean.csv"; Path = "$DownloadsPath\operadoras\operadoras_clean.csv"}
)

$FilesFound = @()
$FilesMissing = @()

foreach ($File in $FilesNeeded) {
    if (Test-Path $File.Path) {
        Write-Host "  OK $($File.Name)" -ForegroundColor $SuccessColor
        $FilesFound += $File
    } else {
        Write-Host "  X $($File.Name)" -ForegroundColor $ErrorColor
        $FilesMissing += $File.Name
    }
}

if ($FilesMissing.Count -gt 0) {
    Write-Host ""
    Write-Host "ERRO: Arquivos nao encontrados!" -ForegroundColor $ErrorColor
    Write-Host "Certifique-se de executar os Testes 1 e 2 primeiro." -ForegroundColor $ErrorColor
    exit 1
}

Write-Host ""
Write-Host "OK Todos os arquivos encontrados!" -ForegroundColor $SuccessColor

Write-Host ""
Write-Host ">>> Preparando container..." -ForegroundColor $WarningColor
docker exec $Container mkdir -p /mnt/csvs 2>&1 | Out-Null

Write-Host ">>> Copiando arquivos..." -ForegroundColor $WarningColor

foreach ($File in $FilesFound) {
    Write-Host "  Copiando $($File.Name)..." -ForegroundColor $WarningColor -NoNewline
    docker cp "$($File.Path)" "$Container`:/mnt/csvs/$($File.Name)" 2>&1 | Out-Null
    Write-Host " OK" -ForegroundColor $SuccessColor
}

Write-Host ""
Write-Host "OK DADOS PREPARADOS!" -ForegroundColor $SuccessColor

# ============================================================================
# PASSO 2: EXECUTAR SCRIPTS SQL
# ============================================================================

Write-Host ""
Write-Host "PASSO 2: Executar scripts SQL" -ForegroundColor $InfoColor
Write-Host "------------------------------" -ForegroundColor $InfoColor

$Scripts = @(
    @{File = "01_ddl.sql"; Desc = "Criar tabelas"},
    @{File = "02_import_clean.sql"; Desc = "Importar operadoras"},
    @{File = "04_import_data.sql"; Desc = "Importar consolidados"},
    @{File = "03_analytics.sql"; Desc = "Analytics"}
)

$Success = 0
$Failed = 0

foreach ($Script in $Scripts) {
    Write-Host ""
    Write-Host ">>> Executando: $($Script.File) - $($Script.Desc)" -ForegroundColor $WarningColor
    
    $Path = Join-Path $PSScriptRoot $Script.File
    
    if (Test-Path $Path) {
        $Content = Get-Content $Path -Raw
        $Output = $Content | docker exec -i $Container psql -U $User -d $Database 2>&1
        
        if ($LASTEXITCODE -eq 0) {
            Write-Host "OK - Script executado" -ForegroundColor $SuccessColor
            $Success++
            
            # Mostrar apenas as linhas com dados reais (filtrando vazio e dashes)
            $Output | Where-Object { $_ -and $_ -notmatch "^-+$" } | ForEach-Object {
                Write-Host "   $_" -ForegroundColor "Gray"
            }
        } else {
            Write-Host "ERRO - Falha na execucao" -ForegroundColor $ErrorColor
            $Output | Select-Object -Last 10 | ForEach-Object {
                Write-Host "   $_" -ForegroundColor $ErrorColor
            }
            $Failed++
        }
    } else {
        Write-Host "ERRO - Arquivo nao encontrado: $Path" -ForegroundColor $ErrorColor
        $Failed++
    }
}

# ============================================================================
# RESUMO FINAL
# ============================================================================

Write-Host ""
Write-Host "==== RESUMO ===" -ForegroundColor $InfoColor
Write-Host "Scripts executados: $($Scripts.Count)" -ForegroundColor $InfoColor
Write-Host "Sucesso: $Success" -ForegroundColor $SuccessColor
Write-Host "Falhas: $Failed" -ForegroundColor $(if ($Failed -gt 0) { $ErrorColor } else { $SuccessColor })

if ($Failed -eq 0) {
    Write-Host ""
    Write-Host "OK TESTE 3 CONCLUIDO COM SUCESSO!" -ForegroundColor $SuccessColor
} else {
    Write-Host ""
    Write-Host "ERRO TESTE 3 CONCLUIDO COM FALHAS!" -ForegroundColor $ErrorColor
}

Write-Host ""
exit $Failed
