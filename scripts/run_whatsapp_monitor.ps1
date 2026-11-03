$ErrorActionPreference = 'Stop'
$source = Join-Path $PSScriptRoot 'whatsapp_monitor_report.cjs'
$policy = [IO.File]::ReadAllText((Join-Path $PSScriptRoot 'send_hours.js'))
$script = [IO.File]::ReadAllText($source).Replace('// SEND_HOURS_MODULE', "const sendHours = (() => { const module = {exports:{}}; $policy; return module.exports; })();")
$encoded = [Convert]::ToBase64String([Text.Encoding]::UTF8.GetBytes($script))
$output = & ssh asisto-aws-monitor "hostname; systemctl is-active asisto-production mongod; echo $encoded | base64 -d | sudo -n -u asisto sh -c 'cd /opt/asisto/current && DOTENV_CONFIG_PATH=/etc/asisto/production.env node -r dotenv/config'"
if ($LASTEXITCODE -ne 0) { throw 'Fallo el acceso o el informe productivo. No utilizar una base alternativa.' }
if ($output[0] -ne 'ip-172-26-0-152' -or $output[1] -ne 'active' -or $output[2] -ne 'active') { throw 'Host o servicios inesperados. No continuar.' }
$line = $output | Where-Object { $_.StartsWith('ASISTO_MONITOR_JSON=') } | Select-Object -Last 1
if (-not $line) { throw 'El servidor no devolvio el informe esperado.' }
$json = $line.Substring('ASISTO_MONITOR_JSON='.Length)
$data = $json | ConvertFrom-Json
if ($data.database -ne 'Cluster0') { throw 'Base inesperada.' }
$json
