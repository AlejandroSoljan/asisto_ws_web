param(
  [Parameter(Mandatory=$true)][string]$Phone,
  [Parameter(Mandatory=$false)][string]$FromDate,
  [Parameter(Mandatory=$false)][string]$ToDate,
  [Parameter(Mandatory=$false)][string]$DsnName,
  [Parameter(Mandatory=$false)][string]$ClientQuery
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
$builder['DBF'] = $settings.DatabaseFile; $builder['ENG'] = $settings.ServerName
$builder['ASTOP'] = $settings.AutoStop; $builder['INT'] = $settings.Integrated
$connection = New-Object System.Data.Odbc.OdbcConnection($builder.ConnectionString)
$rows = @()

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
  $command = $connection.CreateCommand()
  $command.CommandTimeout = 8
  $command.CommandText = @'
SELECT codigo, razon_social, nombre, apellido, cuit, direccion,
       telefono, tel_celular, tel_particular, email, saldo_cc,
       limite_credito, activo_sn, observacion
FROM DBA.clientes
WHERE tel_celular IS NOT NULL AND LENGTH(TRIM(tel_celular)) > 0
ORDER BY codigo
'@
  $reader = $command.ExecuteReader()
  while ($reader.Read()) {
    $cell = if ($reader.IsDBNull(7)) { '' } else { $reader.GetValue(7).ToString() }
    $wanted = Get-Variants $Phone
    $stored = Get-Variants $cell
    $score = 0
    foreach ($left in $wanted) { foreach ($right in $stored) { if ($left -eq $right -and $left.Length -gt $score) { $score = $left.Length } } }
    if ($score -gt 0) {
      $value = { param($index) if ($reader.IsDBNull($index)) { return $null }; return $reader.GetValue($index) }
      $reason = & $value 1
      if (-not $reason) { $reason = ((& $value 3), (& $value 2) -join ' ').Trim() }
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
    foreach ($match in $rows) {
      $latestPurchase = Invoke-Rows @'
SELECT TOP 1 fecha
FROM DBA.ven_remitos_cabecera
WHERE cliente = ?
ORDER BY fecha DESC, nrotransaccion DESC
'@ @([string]$match.client.codigo)
      $match.client['ultimaCompra'] = if ($latestPurchase.Count) { $latestPurchase[0].fecha } else { $null }
    }
    $withPurchase = @($rows | Where-Object { $_.client.ultimaCompra } | Sort-Object { [datetime]$_.client.ultimaCompra } -Descending)
    if ($withPurchase.Count) {
      $selected = $withPurchase[0]
      $rows = @($selected) + @($rows | Where-Object { $_ -ne $selected })
      $selected.client['seleccion'] = 'ultima_compra_ven_remitos_cabecera'
    }
  }
  $ordered = Select-ClientMatches $rows $ClientQuery
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
$ordered = Select-ClientMatches $rows $ClientQuery
if (-not $ordered.Count) { [pscustomobject]@{ found=$false; matches=0 } | ConvertTo-Json -Compress; exit 0 }
if (-not $ClientQuery) {
  $selectedByPurchase = @($ordered | Where-Object { [string]$_.client.seleccion -eq 'ultima_compra_ven_remitos_cabecera' } | Select-Object -First 1)
  if ($selectedByPurchase.Count) {
    $selected = $selectedByPurchase[0]
    $ordered = @($selected) + @($ordered | Where-Object { $_ -ne $selected })
  }
}
$resolvedByLatestPurchase = (-not $ClientQuery -and [string]$ordered[0].client.seleccion -eq 'ultima_compra_ven_remitos_cabecera')
[pscustomobject]@{ found=$true; ambiguous=($ordered.Count -gt 1 -and -not $resolvedByLatestPurchase); matches=$ordered.Count; resolvedByLatestPurchase=$resolvedByLatestPurchase; client=$ordered[0].client } | ConvertTo-Json -Depth 8 -Compress
