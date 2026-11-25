<#
.SYNOPSIS
  Atajos para usar el proyecto en Windows sin recordar comandos largos.

.EXAMPLE
  .\run.ps1                      # muestra la ayuda
  .\run.ps1 pipeline             # corre las 5 etapas para 2025-06 (y 2024-06)
  .\run.ps1 pipeline -Mes 2025-05 -Modelo dbt
  .\run.ps1 monitoreo            # levanta Prometheus + Grafana (http://localhost:3001)
  .\run.ps1 dashboard            # abre el dashboard de resultados (http://localhost:8501)
#>
param(
    [Parameter(Position = 0)]
    [ValidateSet("ayuda", "instalar", "test", "pipeline", "insights", "dashboard", "dagster",
                 "monitoreo", "apagar", "docker", "dbt-docs", "todo")]
    [string]$Accion = "ayuda",
    [string]$Mes = "2025-06",
    [ValidateSet("python", "dbt")]
    [string]$Modelo = "python"
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot
$Py = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"

function Assert-Venv {
    if (-not (Test-Path $Py)) {
        throw "No existe .venv. Corré primero: .\run.ps1 instalar"
    }
}

function Test-Monitoreo {
    # Si Prometheus está corriendo, el pipeline publica métricas en vivo para Grafana.
    try { return [bool](docker ps -q --filter "name=nyc_taxi_prometheus" 2>$null) } catch { return $false }
}

function Enable-Metricas {
    if (Test-Monitoreo) {
        $env:NYC_TAXI_METRICS_PORT = "9108"
        Write-Host "Monitoreo activo: mirá Grafana en http://localhost:3001" -ForegroundColor Cyan
    } else {
        Remove-Item Env:NYC_TAXI_METRICS_PORT -ErrorAction SilentlyContinue
    }
}

switch ($Accion) {
    "ayuda" {
        Write-Host @"
Uso: .\run.ps1 <accion> [-Mes AAAA-MM] [-Modelo python|dbt]

  instalar    crea .venv e instala dependencias (pipeline + dashboard)
  test        corre los tests (pytest)
  pipeline    corre las 5 etapas para -Mes y el mismo mes del año anterior
  insights    genera INSIGHTS.md (7 consultas extra) leyendo el warehouse
  dashboard   abre el dashboard de resultados     -> http://localhost:8501
  dagster     abre la interfaz de Dagster          -> http://localhost:3000
  monitoreo   levanta Prometheus + Grafana         -> http://localhost:3001
  apagar      apaga Prometheus y Grafana
  docker      corre el pipeline dentro de Docker (con límite de 4 GB)
  dbt-docs    genera y abre la documentación dbt   -> http://localhost:8081
  todo        test + pipeline + insights
"@
    }
    "instalar" {
        if (-not (Test-Path $Py)) { py -3.12 -m venv .venv }
        & $Py -m pip install --upgrade pip
        & $Py -m pip install -r requirements-dashboard.txt
        & $Py -m pip check
    }
    "test" { Assert-Venv; & $Py -m pytest -q }
    "pipeline" {
        Assert-Venv; Enable-Metricas
        & $Py -m src.pipeline --mes $Mes --modelo $Modelo
    }
    "insights" { Assert-Venv; & $Py -m src.insights --modelo $Modelo }
    "dashboard" { Assert-Venv; & $Py -m streamlit run dashboard/app.py }
    "dagster" {
        Assert-Venv; Enable-Metricas
        & (Join-Path $PSScriptRoot ".venv\Scripts\dagster.exe") dev -m src.orchestration
    }
    "monitoreo" {
        docker compose --profile monitoreo up -d prometheus grafana
        Write-Host "Grafana:    http://localhost:3001  (tablero 'NYC Taxi: pipeline en tiempo real')" -ForegroundColor Green
        Write-Host "Prometheus: http://localhost:9090" -ForegroundColor Green
        Write-Host "Ahora corré el pipeline en otra terminal (.\run.ps1 pipeline o .\run.ps1 docker)."
        Start-Process "http://localhost:3001"
    }
    "apagar" { docker compose --profile monitoreo stop prometheus grafana }
    "docker" { docker compose up --build pipeline }
    "dbt-docs" { Assert-Venv; & $Py -m src.dbt_runner docs --mes $Mes --serve }
    "todo" {
        Assert-Venv; Enable-Metricas
        & $Py -m pytest -q
        if ($LASTEXITCODE -ne 0) { throw "Fallaron los tests" }
        & $Py -m src.pipeline --mes $Mes --modelo $Modelo
        if ($LASTEXITCODE -ne 0) { throw "Falló el pipeline" }
        & $Py -m src.insights --modelo $Modelo
    }
}
exit $LASTEXITCODE
