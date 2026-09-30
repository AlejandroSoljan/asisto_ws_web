param(
  [Parameter(Mandatory=$true)][string]$Phone,
  [Parameter(Mandatory=$false)][string]$FromDate,
  [Parameter(Mandatory=$false)][string]$ToDate,
  [Parameter(Mandatory=$false)][string]$DsnName,
  [Parameter(Mandatory=$false)][string]$ClientQuery,
  [Parameter(Mandatory=$false)][switch]$Diagnostics
)
$ErrorActionPreference = 'Stop'

$today = Get-Date
if (-not $FromDate) { $FromDate = Get-Date -Date ([datetime]::new($today.Year, $today.Month, 1)) -Format 'yyyy-MM-dd' }
if (-not $ToDate) { $ToDate = Get-Date -Date $today -Format 'yyyy-MM-dd' }
if ($FromDate -notmatch '^\d{4}-\d{2}-\d{2}$' -or $ToDate -notmatch '^\d{4}-\d{2}-\d{2}$') { throw 'invalid_date_range' }
try { $fromValue = [datetime]::ParseExact($FromDate, 'yyyy-MM-dd', $null); $toValue = [datetime]::ParseExact($ToDate, 'yyyy-MM-dd', $null) }
catch { throw 'invalid_date_range' }
if ($fromValue -gt $toValue -or ($toValue - $fromValue).TotalDays -gt 1095) { throw 'invalid_date_range' }

function Get-Digits([string]$Value) { return ($Value -replace '[^0-9]', '') }
function Get-NormalizedText([string]$Value) {
  if (-not $Value) { return '' }
  $formD = $Value.Normalize([Text.NormalizationForm]::FormD)
  $chars = $formD.ToCharArray() | Where-Object { [Globalization.CharUnicodeInfo]::GetUnicodeCategory($_) -ne [Globalization.UnicodeCategory]::NonSpacingMark }
  return (-join $chars).ToLowerInvariant().Trim()
}
function Select-ClientMatches([object[]]$Matches, [string]$Query) {
  $ordered = @($Matches | Sort-Object score -Descending)
  if (-not $Query) { return $ordered }
  $queryText = Get-NormalizedText $Query
  $queryDigits = Get-Digits $Query
  return @($ordered | Where-Object {
    $client = $_.client
    $reason = Get-NormalizedText ([string]$client.razonSocial)
    $cuit = Get-Digits ([string]$client.cuit)
    $code = Get-NormalizedText ([string]$client.codigo)
    ($queryText -and ($reason.Contains($queryText) -or $queryText.Contains($reason) -or $code -eq $queryText)) -or
      ($queryDigits -and $queryDigits.Length -ge 6 -and $cuit -eq $queryDigits)
  })
}
function Get-Variants([string]$Value) {
  $raw = Get-Digits $Value
  $items = New-Object 'System.Collections.Generic.HashSet[string]'
  if (-not $raw) { return @() }
  [void]$items.Add($raw)
  if ($raw.StartsWith('00')) { [void]$items.Add($raw.Substring(2)) }
  $local = if ($raw.StartsWith('54')) { $raw.Substring(2) } else { $raw }
  [void]$items.Add($local)
  if ($local.StartsWith('9')) { [void]$items.Add($local.Substring(1)) }
  if ($local.StartsWith('0')) { [void]$items.Add($local.Substring(1)) }
  foreach ($item in @($items)) {
    $candidate = if ($item.StartsWith('0')) { $item.Substring(1) } else { $item }
    foreach ($areaLength in 2..4) {
      if ($candidate.Length -gt ($areaLength + 2) -and $candidate.Substring($areaLength, 2) -eq '15') {
        [void]$items.Add($candidate.Substring(0, $areaLength) + $candidate.Substring($areaLength + 2))
      }
    }
  }
  foreach ($item in @($items)) {
    if ($item.Length -ge 8) { [void]$items.Add($item.Substring($item.Length - 8)) }
    if ($item.Length -ge 10) { [void]$items.Add($item.Substring($item.Length - 10)) }
  }
  return @($items | Where-Object Length -ge 8 | Sort-Object Length -Descending)
}

Add-Type -AssemblyName System.Data
$dsn = Get-OdbcDsn | Where-Object {
  if ($DsnName) { $_.Name -eq $DsnName -or $_.Name.Replace([string][char]31, '') -eq $DsnName.Replace([string][char]31, '') }
  else { $_.Name -like '*manager_demo_tra_5' }
} | Select-Object -First 1
if (-not $dsn) { throw 'manager_dsn_not_found' }
$settings = Get-ItemProperty ('HKCU:\Software\ODBC\ODBC.INI\' + $dsn.Name)
$builder = New-Object System.Data.Odbc.OdbcConnectionStringBuilder
$builder.Driver = 'SQL Anywhere 11'
$builder['UID'] = $settings.UserID; $builder['PWD'] = $settings.Password
$builder['ENG'] = $settings.ServerName
$builder['INT'] = $settings.Integrated
if ($settings.DatabaseName) {
  $builder['DBN'] = $settings.DatabaseName
  if ($settings.CommLinks) { $builder['LINKS'] = $settings.CommLinks }
  $builder['ASTOP'] = 'NO'
} else {
  $builder['DBF'] = $settings.DatabaseFile
  $builder['ASTOP'] = $settings.AutoStop
}
$connection = New-Object System.Data.Odbc.OdbcConnection($builder.ConnectionString)
$rows = @()
$diagnosticInfo = $null

function Invoke-Rows([string]$Sql, [object[]]$Parameters) {
  $query = $connection.CreateCommand()
  $query.CommandTimeout = 8
  $query.CommandText = $Sql
  foreach ($item in $Parameters) {
    $parameter = $query.Parameters.Add('@value', [System.Data.Odbc.OdbcType]::VarChar)
    $parameter.Value = [string]$item
  }
  $result = @()
  $data = $query.ExecuteReader()
  try {
    while ($data.Read()) {
      $row = [ordered]@{}
      for ($index = 0; $index -lt $data.FieldCount; $index++) {
        $row[$data.GetName($index)] = if ($data.IsDBNull($index)) { $null } else { $data.GetValue($index) }
      }
      $result += [pscustomobject]$row
    }
  } finally { $data.Close() }
  return @($result)
}

try {
  $connection.Open()
  $wanted = @(Get-Variants $Phone)
  if (-not $wanted.Count) { throw 'invalid_phone' }
  if ($Diagnostics) {
    $databaseRows = @(Invoke-Rows "SELECT DB_NAME() AS databaseName FROM dummy" @())
    $clientCountRows = @(Invoke-Rows "SELECT COUNT(*) AS clientCount FROM DBA.clientes" @())
    $exactRows = @(Invoke-Rows "SELECT codigo, razon_social, tel_celular FROM DBA.clientes WHERE TRIM(tel_celular) = ?" @((Get-Digits $Phone)))
    $diagnosticInfo = [ordered]@{
      dsn = $dsn.Name
      platform = $dsn.Platform
      configuredServer = [string]$settings.ServerName
      configuredDatabase = [string]$settings.DatabaseName
      connectionDatabase = if ($databaseRows.Count) { $databaseRows[0].databaseName } else { $null }
      visibleClients = if ($clientCountRows.Count) { $clientCountRows[0].clientCount } else { $null }
      exactMatches = $exactRows.Count
      exactRows = @($exactRows)
      variants = @($wanted)
    }
  }
  # Los valores ya contienen solamente dígitos. Filtrar en SQL evita que el
  # driver ODBC transforme el tipo/representación de tel_celular antes de
  # compararlo (la misma consulta directa funciona en ISQL de Manager).
  $phoneFilter = if ($ClientQuery) { '' } else {
    $phoneLiterals = @($wanted | ForEach-Object { "'$_'" }) -join ','
    "WHERE REPLACE(REPLACE(REPLACE(REPLACE(TRIM(tel_celular), ' ', ''), '-', ''), '(', ''), ')', '') IN ($phoneLiterals)"
  }
  $command = $connection.CreateCommand()
  $command.CommandTimeout = 8
  $command.CommandText = @"
SELECT codigo, razon_social, nombre, apellido, cuit, direccion,
       telefono, tel_celular, tel_particular, email, saldo_cc,
       limite_credito, activo_sn, observacion
FROM DBA.clientes
$phoneFilter
ORDER BY codigo
"@
  $reader = $command.ExecuteReader()
  while ($reader.Read()) {
    $cell = if ($reader.IsDBNull(7)) { '' } else { $reader.GetValue(7).ToString() }
    $stored = Get-Variants $cell
    $score = 0
    foreach ($left in $wanted) { foreach ($right in $stored) { if ($left -eq $right -and $left.Length -gt $score) { $score = $left.Length } } }
    $value = { param($index) if ($reader.IsDBNull($index)) { return $null }; return $reader.GetValue($index) }
    $reason = & $value 1
    if (-not $reason) { $reason = ((& $value 3), (& $value 2) -join ' ').Trim() }
    $queryText = Get-NormalizedText $ClientQuery
    $queryDigits = Get-Digits $ClientQuery
    $reasonText = Get-NormalizedText ([string]$reason)
    $clientCuit = Get-Digits ([string](& $value 4))
    $clientCode = Get-NormalizedText ([string](& $value 0))
    $matchesExplicitQuery = $ClientQuery -and (
      ($queryText -and ($reasonText.Contains($queryText) -or $queryText.Contains($reasonText) -or $clientCode -eq $queryText)) -or
      ($queryDigits -and $queryDigits.Length -ge 6 -and $clientCuit -eq $queryDigits)
    )
    # Una aclaración explícita puede identificar un cliente existente aunque
    # el teléfono no esté guardado en esa misma ficha.
    if ($score -gt 0 -or $matchesExplicitQuery) {
      $rows += [pscustomobject]@{ score=$score; client=[ordered]@{
        codigo=& $value 0; razonSocial=$reason; cuit=& $value 4; direccion=& $value 5
        telefono=& $value 6; telCelular=$cell; telParticular=& $value 8; email=& $value 9
        saldoCuentaCorriente=& $value 10; limiteCredito=& $value 11
        activo=((& $value 12) -eq 'S'); observacion=& $value 13
      }}
    }
  }
  $reader.Close()
  # Capacidad genérica: cuando un teléfono corresponde a varios clientes,
  # adjuntar la fecha de su última compra. La regla de negocio que decide
  # usar este dato se configura en el comportamiento del dominio.
  if (-not $ClientQuery -and $rows.Count -gt 1) {
    $placeholders = (@($rows | ForEach-Object { '?' }) -join ',')
    $codes = @($rows | ForEach-Object { [string]$_.client.codigo })
    $latestPurchases = Invoke-Rows ("SELECT cliente, MAX(fecha) AS ultimaCompra FROM DBA.ven_remitos_cabecera WHERE cliente IN ($placeholders) GROUP BY cliente") $codes
    foreach ($match in $rows) {
      $code = ([string]$match.client.codigo).Trim()
      $purchase = @($latestPurchases | Where-Object { ([string]$_.cliente).Trim() -eq $code } | Select-Object -First 1)
      $match.client['ultimaCompra'] = if ($purchase.Count) { $purchase[0].ultimaCompra } else { $null }
    }
    $withPurchase = @($rows | Where-Object { $_.client.ultimaCompra } | Sort-Object { [datetime]$_.client.ultimaCompra } -Descending)
    if ($withPurchase.Count) {
      $selected = $withPurchase[0]
      $rows = @($selected) + @($rows | Where-Object { $_ -ne $selected })
      $selected.client['seleccion'] = 'ultima_compra_ven_remitos_cabecera'
    }
  }
  $ordered = @(Select-ClientMatches $rows $ClientQuery)
  if ($ordered.Count) {
    $clientCode = [string]$ordered[0].client.codigo
    $facturas = Invoke-Rows @'
SELECT TOP 200 v.transaccion, v.tipocomprobante, v.ptodeventa,
       v.nrotransaccion, v.fecha,
       SUM(p.montoapagar) AS importe,
       SUM(p.montopagado) AS pagado,
       SUM(p.montoapagar - p.montopagado) AS pendiente,
       MAX(p.fechavencimiento) AS vencimiento
FROM DBA.ven_ventas v
JOIN DBA.ven_pagos p ON p.transaccion = v.transaccion
 AND p.tipocomprobante = v.tipocomprobante
 AND p.ptodeventa = v.ptodeventa
 AND p.nrotransaccion = v.nrotransaccion
WHERE v.cliente = ? AND v.activo = 'S'
  AND v.fecha >= ? AND v.fecha < DATEADD(day, 1, ?)
GROUP BY v.transaccion, v.tipocomprobante, v.ptodeventa, v.nrotransaccion, v.fecha
ORDER BY v.fecha DESC, v.nrotransaccion DESC
'@ @($clientCode, $FromDate, $ToDate)

    $recibos = Invoke-Rows @'
SELECT TOP 200 r.nro, r.ptodeventa, r.tipo_recibo AS tipoRecibo,
       r.fecha, r.importe, r.importe_imputado AS importeImputado,
       r.observaciones
FROM DBA.ven_recibos r
WHERE r.cliente = ? AND r.activo = 'S'
  AND r.fecha >= ? AND r.fecha < DATEADD(day, 1, ?)
ORDER BY r.fecha DESC, r.nro DESC
'@ @($clientCode, $FromDate, $ToDate)
    $receiptApplications = Invoke-Rows @'
SELECT d.nrorecibo, d.ptodeventa_recibo AS puntoRecibo, d.tipo_recibo AS tipoRecibo,
       d.transaccion, d.tipocomprobante, d.ptodeventa,
       d.nrotransaccion, d.cuota, d.monto_pagado AS montoPagado
FROM DBA.ven_detalle_recibos d
JOIN DBA.ven_recibos r ON r.nro = d.nrorecibo
 AND r.ptodeventa = d.ptodeventa_recibo AND r.tipo_recibo = d.tipo_recibo
WHERE r.cliente = ? AND r.activo = 'S'
  AND r.fecha >= ? AND r.fecha < DATEADD(day, 1, ?)
ORDER BY d.renglon
'@ @($clientCode, $FromDate, $ToDate)
    foreach ($recibo in $recibos) {
      $applications = @($receiptApplications | Where-Object {
        $_.nrorecibo -eq $recibo.nro -and $_.puntoRecibo -eq $recibo.ptodeventa -and $_.tipoRecibo -eq $recibo.tipoRecibo
      })
      $recibo | Add-Member -NotePropertyName aplicaciones -NotePropertyValue $applications
    }

    $movimientos = Invoke-Rows @'
SELECT TOP 200 c.fecha, c.transaccion, c.tipocomprobante,
       c.ptoventa, c.nro, c.importe, c.importe_pagado AS importePagado,
       c.saldo, c.tipodepago AS tipoDePago
FROM DBA.ven_cuentas_corrientes c
WHERE c.cliente = ? AND c.activo = 'S'
  AND c.fecha >= ? AND c.fecha < DATEADD(day, 1, ?)
ORDER BY c.fecha DESC, c.nro DESC
'@ @($clientCode, $FromDate, $ToDate)
    $ordered[0].client['finanzas'] = [ordered]@{
      desde = $FromDate
      hasta = $ToDate
      facturas = @($facturas)
      recibos = @($recibos)
      movimientos = @($movimientos)
    }
  }
} finally { if ($connection.State -eq 'Open') { $connection.Close() } }
$ordered = @(Select-ClientMatches $rows $ClientQuery)
if (-not $ordered.Count) {
  $response = [ordered]@{ found=$false; matches=0 }
  if ($Diagnostics) { $response['diagnostics'] = $diagnosticInfo }
  [pscustomobject]$response | ConvertTo-Json -Depth 6 -Compress
  exit 0
}
if (-not $ClientQuery) {
  $selectedByPurchase = @($ordered | Where-Object { [string]$_.client.seleccion -eq 'ultima_compra_ven_remitos_cabecera' } | Select-Object -First 1)
  if ($selectedByPurchase.Count) {
    $selected = $selectedByPurchase[0]
    $ordered = @($selected) + @($ordered | Where-Object { $_ -ne $selected })
  }
}
$resolvedByLatestPurchase = (-not $ClientQuery -and [string]$ordered[0].client.seleccion -eq 'ultima_compra_ven_remitos_cabecera')
$candidates = @($ordered | Select-Object -First 8 | ForEach-Object { [ordered]@{ codigo=$_.client.codigo; razonSocial=$_.client.razonSocial; cuit=$_.client.cuit; ultimaCompra=$_.client.ultimaCompra } })
$response = [ordered]@{ found=$true; ambiguous=($ordered.Count -gt 1 -and -not $resolvedByLatestPurchase); matches=$ordered.Count; resolvedByLatestPurchase=$resolvedByLatestPurchase; candidates=$candidates; client=$ordered[0].client }
if ($Diagnostics) { $response['diagnostics'] = $diagnosticInfo }
[pscustomobject]$response | ConvertTo-Json -Depth 8 -Compress
