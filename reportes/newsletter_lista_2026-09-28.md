# Quién quedó en la lista de la newsletter — 2026-09-28

Revisión adelantada (estaba para el 2026-10-04) y decisión cerrada ese día. Consulta read-only
sobre `newsletter_subs` en `catastro-db` (VPS 188). Solo conteos agregados.

## Conteo por estado

| Estado | 2026-09-20 | 2026-09-28 |
|---|---|---|
| sin_respuesta | 574 | 582 |
| invalido | 20 | 20 |
| opt_in | 4 | **10** |
| opt_out | 3 | 4 |
| bounce | 3 | 3 |
| **Total** | 604 | **619** |

## Lo que cambia la lectura

- **Las 15 altas nuevas (21 al 29-09) son todas segmento `intencion`, origen `solicitud`** —
  el formulario de solicitud de licencias. **Nunca recibieron la campaña** (`ultimo_envio_at`
  nulo), así que su `sin_respuesta` no es silencio ante el re-permiso.
- **opt-in por fecha:** 14-09 (2), 16-09, 18-09 → los 4 de la campaña. 21, 22 (2), 23, 25, 26-09
  → los 6 del formulario.
- **opt-out por fecha:** 16, 17, 19 y 24-09.

## Decisión

La campaña de re-permiso rindió **4 opt-in sobre 589 (0,7%)** y ninguno después del 18-09. El
canal que suma consentimiento es el **formulario de solicitud** (6 en una semana), con gente que
ya tiene intención de compra. **No hay newsletter recurrente**: con 10 opt-in no es un canal.
Coherente con que el negocio son contratos y llegan por LinkedIn.
