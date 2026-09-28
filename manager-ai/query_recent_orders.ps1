param(
  [Parameter(Mandatory=$true)][string]$Phone,
  [Parameter(Mandatory=$false)][string]$DsnName = 'msm_manager',
  [int]$Limit = 10
)
$ErrorActionPreference = 'Stop'
$digits = $Phone -replace '[^0-9]', ''
if ($digits.Length -lt 8) { throw 'invalid_phone' }
$last10 = if ($digits.Length -gt 10) { $digits.Substring($digits.Length - 10) } else { $digits }
$Limit = [Math]::Max(1, [Math]::Min(10, $Limit))

Add-Type -AssemblyName System.Data
$dsn = Get-OdbcDsn | Where-Object { $_.Name -eq $DsnName -or $_.Name.Replace([string][char]31, '') -eq $DsnName.Replace([string][char]31, '') } | Select-Object -First 1
if (-not $dsn) { throw 'manager_dsn_not_found' }
$settings = Get-ItemProperty ('HKCU:\Software\ODBC\ODBC.INI\' + $dsn.Name)
$builder = New-Object System.Data.Odbc.OdbcConnectionStringBuilder
$builder.Driver = 'SQL Anywhere 11'
$builder['UID'] = $settings.UserID; $builder['PWD'] = $settings.Password
$builder['DBF'] = $settings.DatabaseFile; $builder['ENG'] = $settings.ServerName
$builder['ASTOP'] = $settings.AutoStop; $builder['INT'] = $settings.Integrated
$connection = New-Object System.Data.Odbc.OdbcConnection($builder.ConnectionString)
$connection.Open()
try {
  $command = $connection.CreateCommand()
  $command.CommandTimeout = 12
  $command.CommandText = @'
SELECT
  rm_transaccion, rm_letra, rm_ptodeventa, rm_nrotransaccion, rm_fecha,
  q.rm_activo, q.rm_total, q.rm_observaciones, q.rm_renglon, q.producto, a.descripcion AS producto_descripcion, q.cantidad, q.precio_final,
  cliente_codigo, cliente_razon_social, cliente_tel_celular,
  venta_transaccion, venta_tipocomprobante, venta_ptodeventa, venta_nrotransaccion, venta_fecha,
  entrega_cod_horario, entrega_forma_pago, entrega_direccion, entrega_telefono,
  entrega_email, entrega_observaciones, entrega_fecha_calificacion,
  entrega_observacion_cliente, entrega_calificacion, entrega_estado,
  horario_fecha, horario_desde, horario_hasta, horario_disponible
FROM DBA.v_ven_remitos_clientes_ventas q
LEFT JOIN DBA.articulos a ON a.producto = q.producto
WHERE q.cliente_tel_celular LIKE ?
ORDER BY rm_fecha DESC, rm_transaccion DESC, rm_letra DESC, rm_ptodeventa DESC, rm_nrotransaccion DESC, rm_renglon
'@
  $parameter = $command.Parameters.Add('@phone', [System.Data.Odbc.OdbcType]::VarChar)
  $parameter.Value = '%' + $last10 + '%'
  $reader = $command.ExecuteReader()
  $orders = [ordered]@{}
  try {
    while ($reader.Read()) {
      $key = ([string]$reader['rm_transaccion']) + '|' + ([string]$reader['rm_letra']) + '|' + ([string]$reader['rm_ptodeventa']) + '|' + ([string]$reader['rm_nrotransaccion'])
      if (-not $orders.Contains($key)) {
        if ($orders.Count -ge $Limit) { break }
        $orders[$key] = [ordered]@{
          transaccion=[string]$reader['rm_transaccion']; letra=[string]$reader['rm_letra']; ptodeventa=[string]$reader['rm_ptodeventa']; numero=[string]$reader['rm_nrotransaccion']
          fecha=$reader['rm_fecha']; activo=$reader['rm_activo']; total=$reader['rm_total']; observaciones=$reader['rm_observaciones']
          cliente_codigo=[string]$reader['cliente_codigo']; cliente_razon_social=[string]$reader['cliente_razon_social']
          venta= if ($reader['venta_nrotransaccion'] -is [DBNull]) { $null } else { [ordered]@{ transaccion=[string]$reader['venta_transaccion']; tipo=[string]$reader['venta_tipocomprobante']; ptodeventa=[string]$reader['venta_ptodeventa']; numero=[string]$reader['venta_nrotransaccion']; fecha=$reader['venta_fecha'] } }
          entrega=[ordered]@{ cod_horario=[string]$reader['entrega_cod_horario']; forma_pago=[string]$reader['entrega_forma_pago']; direccion=[string]$reader['entrega_direccion']; telefono=[string]$reader['entrega_telefono']; email=[string]$reader['entrega_email']; observaciones=[string]$reader['entrega_observaciones']; estado=[string]$reader['entrega_estado']; calificacion=[string]$reader['entrega_calificacion']; observacion_cliente=[string]$reader['entrega_observacion_cliente']; horario_fecha=$reader['horario_fecha']; horario_desde=$reader['horario_desde']; horario_hasta=$reader['horario_hasta']; horario_disponible=$reader['horario_disponible'] }
          productos=New-Object System.Collections.ArrayList
        }
      }
      [void]$orders[$key].productos.Add([ordered]@{ renglon=$reader['rm_renglon']; codigo=[string]$reader['producto']; descripcion=[string]$reader['producto_descripcion']; cantidad=$reader['cantidad']; precio_final=$reader['precio_final'] })
    }
  } finally { $reader.Dispose() }
  [pscustomobject]@{ ok=$true; phone=$last10; count=$orders.Count; orders=@($orders.Values) } | ConvertTo-Json -Depth 8 -Compress
} finally { $connection.Dispose() }
