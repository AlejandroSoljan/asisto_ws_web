param(
  [Parameter(Mandatory=$true)][ValidateSet('sale','receipt','statement')][string]$Kind,
  [Parameter(Mandatory=$false)][string]$DsnName,
  [Parameter(Mandatory=$true)][string]$ManagerFolder,
  [Parameter(Mandatory=$true)][string]$Output,
  [string]$Transaction,
  [string]$VoucherType,
  [string]$PointOfSale,
  [string]$Number,
  [string]$ClientCode,
  [string]$FromDate,
  [string]$ToDate
)
$ErrorActionPreference = 'Stop'

function Assert-Plain([string]$Value, [string]$Name, [int]$Maximum = 100) {
  if (-not $Value -or $Value.Length -gt $Maximum -or $Value -match '[\r\n]') { throw "invalid_$Name" }
}
if ($DsnName) { Assert-Plain $DsnName 'odbc' 128 }
if ($Kind -eq 'statement') {
  Assert-Plain $ClientCode 'client_code'
  Assert-Plain $FromDate 'from_date'
  Assert-Plain $ToDate 'to_date'
} else {
  Assert-Plain $PointOfSale 'point_of_sale'
  Assert-Plain $Number 'number'
  if ($Kind -eq 'sale') { Assert-Plain $Transaction 'transaction'; Assert-Plain $VoucherType 'voucher_type' }
}

$manager = (Resolve-Path -LiteralPath $ManagerFolder).Path
if (-not (Test-Path -LiteralPath (Join-Path $manager 'manager_msm.exe'))) { throw 'manager_executable_not_found' }
$libraries = @(Get-ChildItem -LiteralPath $manager -Filter '*.pbd' -File | Sort-Object Name | ForEach-Object Name)
if (-not $libraries.Count) { throw 'manager_libraries_not_found' }

Add-Type -AssemblyName System.Data
$dsn = Get-OdbcDsn | Where-Object {
  if ($DsnName) { $_.Name -eq $DsnName -or $_.Name.Replace([string][char]31, '') -eq $DsnName.Replace([string][char]31, '') }
  else { $_.Name -like '*manager_demo_tra_5' }
} | Select-Object -First 1
if (-not $dsn) { throw 'manager_dsn_not_found' }
$settings = Get-ItemProperty ('HKCU:\Software\ODBC\ODBC.INI\' + $dsn.Name)
$uid = [string]$settings.UserID; $pwd = [string]$settings.Password
if (-not $uid) { throw 'manager_dsn_credentials_not_found' }

$mappingTransaction = if ($Kind -eq 'receipt') { 'RC' } else { $Transaction }
$mappingLetter = if ($Kind -eq 'receipt') { 'R' } else { $VoucherType }
$builder = New-Object System.Data.Odbc.OdbcConnectionStringBuilder
$builder.Driver = 'SQL Anywhere 11'
$builder['UID'] = $uid; $builder['PWD'] = $pwd
$builder['DBF'] = $settings.DatabaseFile; $builder['ENG'] = $settings.ServerName
$builder['ASTOP'] = $settings.AutoStop; $builder['INT'] = $settings.Integrated
$connection = New-Object System.Data.Odbc.OdbcConnection($builder.ConnectionString)
$connection.Open()
try {
  if ($Kind -eq 'statement') {
    $dataObject = 'd_ven_cons_cuentas_ctes_cpbte_compo'
    $parityCommand = $connection.CreateCommand()
    $parityCommand.CommandTimeout = 8
    $parityCommand.CommandText = 'SELECT cod_moneda, paridad FROM DBA.gen_monedas ORDER BY cod_moneda'
    $parityReader = $parityCommand.ExecuteReader()
    $parities = ''
    try {
      while ($parityReader.Read()) {
        $code = [string]$parityReader.GetValue(0)
        $parity = ([decimal]$parityReader.GetValue(1)).ToString('000.000', [Globalization.CultureInfo]::InvariantCulture)
        $parities += '*' + $code + ':' + $parity
      }
    } finally { $parityReader.Dispose() }
    if (-not $parities) { throw 'manager_currency_parities_not_found' }
  } else {
    $command = $connection.CreateCommand()
    $command.CommandTimeout = 8
    $command.CommandText = 'SELECT TOP 1 dataobject_mail, dataobject FROM DBA.ven_numero_cpbtes WHERE transaccion = ? AND prefijo_cpbte = ? AND letra_cpbte = ?'
    foreach ($value in @($mappingTransaction, $PointOfSale, $mappingLetter)) {
      $parameter = $command.Parameters.Add('@value', [System.Data.Odbc.OdbcType]::VarChar)
      $parameter.Value = $value
    }
    $reader = $command.ExecuteReader()
    try {
      if (-not $reader.Read()) { throw 'manager_print_object_not_configured' }
      $mailObject = if ($reader.IsDBNull(0)) { '' } else { [string]$reader.GetValue(0) }
      $printObject = if ($reader.IsDBNull(1)) { '' } else { [string]$reader.GetValue(1) }
      $dataObject = if ($mailObject.Trim()) { $mailObject.Trim() } else { $printObject.Trim() }
    } finally { $reader.Dispose() }
  }
} finally { $connection.Dispose() }
if (-not $dataObject) { throw 'manager_print_object_not_configured' }

$helper = Join-Path $PSScriptRoot 'asisto_manager_pdf.exe'
if (-not (Test-Path -LiteralPath $helper)) { throw 'pdf_helper_not_found' }
$targetDirectory = Split-Path -Parent $Output
New-Item -ItemType Directory -Path $targetDirectory -Force | Out-Null
$request = Join-Path $targetDirectory (([guid]::NewGuid().ToString('N')) + '.ini')
try {
  $content = @(
    '[document]', "kind=$Kind", "dataobject=$dataObject", "output=$Output",
    '[manager]', ('folder=' + $manager), ('libraries=' + ($libraries -join ',')),
    '[database]', ('dsn=' + $dsn.Name), "uid=$uid", "pwd=$pwd",
    '[arguments]', "arg1=$Transaction", "arg2=$VoucherType", "arg3=$PointOfSale", "arg4=$Number"
  )
  if ($Kind -eq 'receipt') { $content = @('[document]', "kind=$Kind", "dataobject=$dataObject", "output=$Output", '[manager]', ('folder=' + $manager), ('libraries=' + ($libraries -join ',')), '[database]', ('dsn=' + $dsn.Name), "uid=$uid", "pwd=$pwd", '[arguments]', "arg1=$PointOfSale", "arg2=$Number", 'arg3=', 'arg4=') }
  if ($Kind -eq 'statement') { $content = @('[document]', "kind=$Kind", "dataobject=$dataObject", "output=$Output", '[manager]', ('folder=' + $manager), ('libraries=' + ($libraries -join ',')), '[database]', ('dsn=' + $dsn.Name), "uid=$uid", "pwd=$pwd", '[arguments]', 'arg1=*', "arg2=$FromDate", "arg3=$ToDate", "arg4=$ClientCode", 'arg5=S', 'arg6=', "arg7=$parities", 'arg8=yes', 'arg9=1') }
  Set-Content -LiteralPath $request -Value $content -Encoding Default
  $env:PATH = $manager + ';' + $env:PATH
  for ($attempt = 1; $attempt -le 2; $attempt++) {
    # La aplicación compilada necesita encontrar su propio
    # app_asisto_manager_pdf.pbd junto al EXE. Los PBD de Manager se cargan
    # luego desde la carpeta informada en el pedido; sus runtimes ya están en
    # PATH. Si se inicia con Manager como directorio actual, el host queda vivo
    # pero el evento Open de la aplicación nunca llega a ejecutarse.
    $process = Start-Process -FilePath $helper -ArgumentList $request -WorkingDirectory $PSScriptRoot -WindowStyle Hidden -PassThru
    if (-not $process.WaitForExit(30000)) {
      try { $process.Kill() } catch {}
      throw 'pdf_helper_timeout'
    }
    $attemptOk = [string](Get-Content -LiteralPath $request | Select-String '^ok=' | ForEach-Object { $_.Line.Substring(3) })
    if ($attemptOk -eq '1' -and (Test-Path -LiteralPath $Output)) { break }
    if ($attempt -lt 2) { Start-Sleep -Milliseconds 500 }
  }
  $ok = [string](Get-Content -LiteralPath $request | Select-String '^ok=' | ForEach-Object { $_.Line.Substring(3) })
  $errorText = [string](Get-Content -LiteralPath $request | Select-String '^error=' | ForEach-Object { $_.Line.Substring(6) })
  $stage = [string](Get-Content -LiteralPath $request | Select-String '^stage=' | ForEach-Object { $_.Line.Substring(6) })
  $describedArguments = [string](Get-Content -LiteralPath $request | Select-String '^arguments=' | ForEach-Object { $_.Line.Substring(10) })
  $libraryResult = [string](Get-Content -LiteralPath $request | Select-String '^library_result=' | ForEach-Object { $_.Line.Substring(15) })
  if ($ok -ne '1' -or -not (Test-Path -LiteralPath $Output)) { throw $(if ($errorText) { $errorText + ': ' + $stage + ': ' + $dataObject + ': args=' + $describedArguments + ': libraries=' + $libraryResult } else { 'pdf_generation_failed: ' + $stage + ': ' + $dataObject + ': args=' + $describedArguments + ': libraries=' + $libraryResult }) }
  $file = Get-Item -LiteralPath $Output
  if ($file.Length -lt 100 -or -not ([System.IO.File]::ReadAllBytes($file.FullName)[0..3] -join ',' -eq '37,80,68,70')) { throw 'invalid_pdf_output' }
  [pscustomobject]@{ ok = $true; path = $file.FullName; bytes = $file.Length; dataObject = $dataObject } | ConvertTo-Json -Compress
} finally {
  Remove-Item -LiteralPath $request -Force -ErrorAction SilentlyContinue
}
