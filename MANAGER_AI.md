# IA de WhatsApp con documentos de Manager

Esta función corre dentro de `app_asisto_ws.js`. No usa ni requiere la extensión de Chrome.

Se configura por dominio en `tenant_config` y queda desactivada por defecto:

```json
{
  "habilitar_bot": true,
  "wweb_bot_logic_mode": "chatgpt",
  "manager_ai_enabled": true,
  "manager_document_send_enabled": true,
  "habilitar_odbc_manager": true,
  "dsn": "NOMBRE_DSN_MANAGER",
  "manager_folder": "C:\\Manager",
  "manager_document_lookup_days": 365
}
```

El comportamiento conversacional se administra con la configuración **Comportamiento** ya existente del dominio. La herramienta local sólo interviene cuando el cliente pide que le envíen una factura o un recibo.

Controles aplicados:

- busca el cliente por el teléfono real de WhatsApp, admitiendo etiquetas como `(ale)` en `tel_celular`; el prefiltro SQL no sustituye la validación posterior de dígitos;
- consulta Manager por ODBC en modo lectura;
- si hay varios documentos, solicita el número antes de enviar;
- ante varios clientes, la búsqueda incluye su última compra para resolver la asociación; si sigue ambigua, solicita aclaración;
- genera el PDF con los objetos de impresión configurados en Manager;
- elimina el PDF temporal después de enviarlo;
- nunca entrega credenciales ODBC al modelo de IA.

Las respuestas de documentos se componen mediante IA con el comportamiento del
dominio y el resultado real de la herramienta. Se conserva el mensaje original
durante la selección de cliente/documento, para no perder solicitudes adicionales
como el alias de transferencia. Un PDF preparado no se informa como ya enviado.

Los archivos de soporte incluidos en `manager-ai/` forman parte del cliente de WhatsApp y no dependen del proyecto de la extensión.
