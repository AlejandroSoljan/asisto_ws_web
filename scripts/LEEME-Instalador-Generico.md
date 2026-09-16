# Instalador genérico de Asisto (Windows)

Este paquete no contiene `node_modules`, sesiones de WhatsApp ni claves de clientes. Sirve para cualquier dominio.

## Preparación

1. Instalar Node.js (con npm) y Git en el equipo del cliente.
2. Colocar el archivo privado `bootstrap-DOMINIO.json` junto a este instalador o en Descargas del administrador que lo ejecutará. No enviarlo al repositorio ni compartirlo con otros clientes.
3. Abrir PowerShell como administrador y ejecutar `./Instalar-Asisto-Generico.ps1`.
4. Ingresar el dominio y la carpeta de instalación. La carpeta debe estar vacía.

El instalador valida la credencial del dominio contra Asisto, descarga la versión indicada desde GitHub, ejecuta `npm install --omit=dev`, habilita las actualizaciones automáticas y registra la tarea `Asisto-DOMINIO`. El archivo privado sólo se usa para construir la configuración local. Si ya existe una instalación, el instalador se detiene sin sobrescribirla: la migración necesita preservar autenticación y datos locales.

## Formato del archivo de alta

```json
{
  "tenantId": "DOMINIO",
  "numero": "549XXXXXXXXXX",
  "control_api_token": "CREDENCIAL_PRIVADA_DEL_DOMINIO",
  "target_tag": "v4.04.56",
  "control_api_url": "https://asistobot.com.ar/api/ext/wweb/agent"
}
```

El archivo de alta debe emitirse desde la administración segura de Asisto para el dominio correspondiente. Nunca incluir credenciales reales en el ZIP genérico.
