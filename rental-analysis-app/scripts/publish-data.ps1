param(
    [string]$RentalSource,
    [string]$FleetSource,
    [string]$InvoiceSource,
    [string]$Branch = "main",
    [string]$CommitMessage,
    [switch]$TriggerRenderHook,
    [string]$RenderDeployHookUrl = $env:RENDER_DEPLOY_HOOK_URL
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Resolve-GitCommand {
    $gitCmd = Get-Command git -ErrorAction SilentlyContinue
    if ($gitCmd) {
        return $gitCmd.Source
    }

    $candidate = "C:\Users\$env:USERNAME\AppData\Local\Programs\Git\cmd\git.exe"
    if (Test-Path $candidate) {
        return $candidate
    }

    throw "Git no esta disponible. Instala Git o agrega git al PATH."
}

function Copy-IfProvided {
    param(
        [string]$Source,
        [string]$Destination,
        [string]$Label
    )

    if ([string]::IsNullOrWhiteSpace($Source)) {
        Write-Host "[$Label] sin source; se asume que ya actualizaste el archivo en el repo." -ForegroundColor Yellow
        return
    }

    if (-not (Test-Path $Source)) {
        throw "[$Label] no existe el archivo source: $Source"
    }

    Copy-Item -Path $Source -Destination $Destination -Force
    Write-Host "[$Label] actualizado: $Destination" -ForegroundColor Green
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$projectDir = Split-Path -Parent $scriptDir
$repoDir = Split-Path -Parent $projectDir

$rentalTarget = Join-Path $projectDir "PastRentalDetails_2026-2-25.xlsx"
$fleetTarget = Join-Path $projectDir "Kinto Fleet_3-19-26.xlsx"
$invoiceTarget = Join-Path $projectDir "Invoices consolidated.xlsx"

$gitExe = Resolve-GitCommand

Copy-IfProvided -Source $RentalSource -Destination $rentalTarget -Label "Rental"
Copy-IfProvided -Source $FleetSource -Destination $fleetTarget -Label "Fleet"
Copy-IfProvided -Source $InvoiceSource -Destination $invoiceTarget -Label "Invoice"

Set-Location $repoDir

& $gitExe add -- "rental-analysis-app/PastRentalDetails_2026-2-25.xlsx" "rental-analysis-app/Kinto Fleet_3-19-26.xlsx" "rental-analysis-app/Invoices consolidated.xlsx"

$staged = & $gitExe diff --cached --name-only
if ([string]::IsNullOrWhiteSpace(($staged -join ""))) {
    Write-Host "No hay cambios de data para commitear." -ForegroundColor Yellow
    exit 0
}

if ([string]::IsNullOrWhiteSpace($CommitMessage)) {
    $CommitMessage = "Data refresh " + (Get-Date -Format "yyyy-MM-dd HH:mm")
}

& $gitExe commit -m $CommitMessage
& $gitExe push origin $Branch

Write-Host "Push completado en branch '$Branch'. Render deberia iniciar deploy automatico." -ForegroundColor Cyan

if ($TriggerRenderHook) {
    if ([string]::IsNullOrWhiteSpace($RenderDeployHookUrl)) {
        throw "TriggerRenderHook activo pero falta RenderDeployHookUrl (parametro o variable RENDER_DEPLOY_HOOK_URL)."
    }

    $response = Invoke-WebRequest -Uri $RenderDeployHookUrl -Method Post -UseBasicParsing
    Write-Host "Deploy hook disparado. HTTP status: $($response.StatusCode)" -ForegroundColor Cyan
}
