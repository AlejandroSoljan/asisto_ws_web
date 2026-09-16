param(
    [string]$BootstrapFile = ''
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

# El paquete es común a todos los clientes. El archivo de alta, emitido para un
# dominio por el superadmin, contiene la única credencial privada necesaria.
$identity = [Security.Principal.WindowsIdentity]::GetCurrent()
$principal = [Security.Principal.WindowsPrincipal]::new($identity)
if (-not $principal.IsInRole([Security.Principal.WindowsBuiltInRole]::Administrator)) {
    throw 'Ejecutá el instalador como administrador para registrar la tarea programada.'
}
if (-not [Environment]::Is64BitOperatingSystem) { throw 'Se requiere Windows de 64 bits.' }

$domain = (Read-Host 'Dominio').Trim().ToUpperInvariant()
if ($domain -notmatch '^[A-Z0-9_-]{2,80}$') { throw 'Dominio inválido.' }
$rawDestination = (Read-Host 'Carpeta de instalación (ej.: C:\Asisto)').Trim().Trim('"')
if (-not $rawDestination) { throw 'La carpeta de instalación es obligatoria.' }
$destination = [IO.Path]::GetFullPath($rawDestination)
if ($destination -eq [IO.Path]::GetPathRoot($destination)) { throw 'No se permite instalar en la raíz del disco.' }
if (Test-Path -LiteralPath $destination) {
    if (@(Get-ChildItem -LiteralPath $destination -Force).Count -gt 0) {
        throw "La carpeta $destination ya contiene archivos. Este instalador no sobrescribe instalaciones ni autenticaciones existentes."
    }
}

if (-not $BootstrapFile) {
    $candidates = @(
        (Join-Path $PSScriptRoot "bootstrap-$domain.json"),
        (Join-Path ([Environment]::GetFolderPath('UserProfile')) "Downloads\bootstrap-$domain.json")
    )
    $foundBootstrap = @($candidates | Where-Object { Test-Path -LiteralPath $_ } | Select-Object -First 1)
    $BootstrapFile = if ($foundBootstrap.Count) { [string]$foundBootstrap[0] } else { '' }
}
if (-not $BootstrapFile -or -not (Test-Path -LiteralPath $BootstrapFile)) {
    throw "Falta bootstrap-$domain.json junto al instalador o en Descargas. Cada dominio necesita su credencial privada; el paquete genérico nunca incluye claves de clientes."
}
$bootstrap = Get-Content -LiteralPath $BootstrapFile -Raw -Encoding UTF8 | ConvertFrom-Json
if (([string]$bootstrap.tenantId).Trim().ToUpperInvariant() -ne $domain) { throw 'El archivo de alta pertenece a otro dominio.' }
$numero = ([string]$bootstrap.numero) -replace '\D', ''
$token = ([string]$bootstrap.control_api_token).Trim()
$targetTag = ([string]$bootstrap.target_tag).Trim()
if ($numero.Length -lt 8 -or $token.Length -lt 16 -or $targetTag -notmatch '^v\d+(?:\.\d+){2,3}$') {
    throw 'El archivo de alta está incompleto o no indica una versión válida.'
}
$controlUrl = ([string]$bootstrap.control_api_url).Trim()
if (-not $controlUrl) { $controlUrl = 'https://asistobot.com.ar/api/ext/wweb/agent' }
$uri = [Uri]$controlUrl
if ($uri.Scheme -ne 'https' -or $uri.Host -notin @('asistobot.com.ar', 'www.asistobot.com.ar') -or $uri.AbsolutePath -ne '/api/ext/wweb/agent') {
    throw 'La URL de control no corresponde al servidor oficial de Asisto.'
}

$git = Get-Command git.exe -ErrorAction SilentlyContinue
$node = Get-Command node.exe -ErrorAction SilentlyContinue
$npm = Get-Command npm.cmd -ErrorAction SilentlyContinue
if (-not $git -or -not $node -or -not $npm) {
    throw 'Instalá Git y Node.js con npm antes de continuar. Ambos son necesarios para las actualizaciones automáticas.'
}
$taskName = "Asisto-$domain"
if (Get-ScheduledTask -TaskName $taskName -ErrorAction SilentlyContinue) { throw "Ya existe la tarea $taskName. Usá un procedimiento de actualización, no una instalación nueva." }
if (Get-NetTCPConnection -LocalPort 8001 -State Listen -ErrorAction SilentlyContinue) { throw 'El puerto 8001 ya está en uso.' }

# Verifica la credencial antes de crear archivos o registrar tareas.
$ping = Invoke-RestMethod -Method Post -Uri "$controlUrl/ping" -Headers @{ 'x-api-key' = $token } -ContentType 'application/json' -Body (@{ tenantId = $domain; numero = $numero } | ConvertTo-Json -Compress)
if ($ping.ok -ne $true) { throw 'Asisto rechazó el alta de este dominio.' }

Write-Host "Descargando versión $targetTag desde GitHub..."
& $git.Source clone --depth 1 --branch $targetTag 'https://github.com/AlejandroSoljan/asisto_ws_web.git' $destination
if ($LASTEXITCODE -ne 0) { throw 'Falló la descarga de la versión. No se registró ninguna tarea.' }

try {
    $env:PUPPETEER_CACHE_DIR = Join-Path $destination 'browser-cache'
    Push-Location $destination
    try {
        Write-Host 'Instalando dependencias con npm...'
        & $npm.Source install --omit=dev
        if ($LASTEXITCODE -ne 0) { throw 'npm install falló. No se registró ninguna tarea.' }
    } finally { Pop-Location }

    $config = [ordered]@{
        tenantId = $domain
        numero = $numero
        control_api_enabled = $true
        control_api_url = $controlUrl
        control_api_token = $token
        auto_update = [ordered]@{
            enabled = $true
            repo_path = $destination
            remote = 'origin'
            source = 'tag'
            target_tag = $targetTag
            check_every_ms = 600000
            startup_delay_ms = 120000
            require_clean = $true
            restart_on_apply = $true
            run_npm_install = $true
        }
    }
    $utf8 = [System.Text.UTF8Encoding]::new($false)
    [IO.File]::WriteAllText((Join-Path $destination 'configuracion.json'), ($config | ConvertTo-Json -Depth 6), $utf8)
    New-Item -ItemType Directory -Path (Join-Path $destination 'auth'), (Join-Path $destination 'logs') -Force | Out-Null

    $runner = @"
@echo off
setlocal
cd /d "$destination"
set "ASISTO_AUTH_PATH=$destination\auth"
set "PUPPETEER_CACHE_DIR=$destination\browser-cache"
set "PATH=$([IO.Path]::GetDirectoryName($git.Source));$([IO.Path]::GetDirectoryName($node.Source));%PATH%"
:run
"$($node.Source)" "$destination\app_asisto_ws.js" >> "$destination\logs\asisto-ws.log" 2>&1
timeout /t 5 /nobreak >nul
goto run
"@
    [IO.File]::WriteAllText((Join-Path $destination 'Iniciar-Asisto.cmd'), $runner, $utf8)
    $action = New-ScheduledTaskAction -Execute "$env:SystemRoot\System32\cmd.exe" -Argument ('/d /c "{0}"' -f (Join-Path $destination 'Iniciar-Asisto.cmd')) -WorkingDirectory $destination
    $trigger = New-ScheduledTaskTrigger -AtStartup
    $taskPrincipal = New-ScheduledTaskPrincipal -UserId 'SYSTEM' -LogonType ServiceAccount -RunLevel Highest
    $settings = New-ScheduledTaskSettingsSet -ExecutionTimeLimit ([TimeSpan]::Zero) -RestartInterval (New-TimeSpan -Minutes 1) -RestartCount 3 -StartWhenAvailable
    Register-ScheduledTask -TaskName $taskName -Action $action -Trigger $trigger -Principal $taskPrincipal -Settings $settings -Description "Asisto WhatsApp $domain; actualización automática por GitHub" | Out-Null
    Start-ScheduledTask -TaskName $taskName
    Write-Host "Instalación completa: $destination"
    Write-Host "Dominio: $domain · Versión: $targetTag · Actualización automática: habilitada"
} catch {
    # Se conserva la carpeta para diagnóstico; nunca se borra una instalación.
    throw
}
