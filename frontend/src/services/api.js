/**
 * Unified API client — merges roles-frontend (public) + prediosChile (commerce)
 */
const API = '/api'

// ─── Generic fetch helpers ─────────────────────────────────────────────────

async function fetchJSON(url) {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`HTTP ${res.status}`)
  return res.json()
}

async function apiFetch(endpoint, options = {}) {
  const url = `${API}${endpoint}`
  const config = {
    credentials: 'include',
    headers: { 'Content-Type': 'application/json', ...options.headers },
    ...options,
  }
  const response = await fetch(url, config)
  const data = await response.json()
  if (!response.ok) {
    const error = new Error(data.error || `Request failed (${response.status})`)
    error.status = response.status
    error.data = data
    throw error
  }
  return data
}

// ─── Public catastral data (from roles-backend) ────────────────────────────

export function searchPredios(params) {
  const qs = new URLSearchParams()
  Object.entries(params).forEach(([k, v]) => {
    if (v !== '' && v !== null && v !== undefined) qs.set(k, v)
  })
  return fetchJSON(`${API}/predios?${qs}`)
}

export function getPredio(comuna, manzana, predio) {
  return fetchJSON(`${API}/predios/${comuna}/${manzana}/${predio}`)
}

export function getEvolucion(comuna, manzana, predio) {
  return fetchJSON(`${API}/predios/${comuna}/${manzana}/${predio}/evolucion`)
}

export function getEdificio(comuna, manzana, predio) {
  return fetchJSON(`${API}/predios/${comuna}/${manzana}/${predio}/edificio`)
}

export function getEdificio3d(comuna, manzana, predio) {
  return fetchJSON(`${API}/predios/${comuna}/${manzana}/${predio}/edificio3d`)
}

export function getComunas() {
  return fetchJSON(`${API}/comunas`)
}

export function getDestinos() {
  return fetchJSON(`${API}/destinos`)
}

export function getStatsResumen() {
  return fetchJSON(`${API}/estadisticas/resumen`)
}

export function getStatsComunas() {
  return fetchJSON(`${API}/estadisticas/comunas`)
}

export function getStatsComunaDetail(codigo) {
  return fetchJSON(`${API}/estadisticas/comunas/${codigo}`)
}

export function getHealth() {
  return fetchJSON(`${API}/health`)
}

export function resolveComuna(nombre) {
  return fetchJSON(`${API}/comunas/resolve?nombre=${encodeURIComponent(nombre)}`)
}

export function autocompletePredios(q, comuna = null) {
  const qs = new URLSearchParams({ q })
  if (comuna) qs.set('comuna', comuna)
  return fetchJSON(`${API}/predios/autocomplete?${qs}`)
}

export function searchPrediosNearby(lat, lon, radius = 300, page = 1, limit = 25, direccion = null) {
  const qs = new URLSearchParams({ lat, lon, radius, page, limit })
  if (direccion) qs.set('direccion', direccion)
  return fetchJSON(`${API}/predios/nearby?${qs}`)
}

export function getNearbyMarkers(lat, lon, radius = 300) {
  return fetchJSON(`${API}/predios/nearby/markers?${new URLSearchParams({ lat, lon, radius })}`)
}

// ─── GeoJSON polygons (for map display) ───────────────────────────────────

export function getPropertyPolygon(comuna, manzana, predio) {
  return fetchJSON(`${API}/geojson/predio/${comuna}/${manzana}/${predio}`)
}

export function getNearbyPolygons(lat, lon, radius = 300, limit = 50) {
  return fetchJSON(`${API}/geojson/nearby?${new URLSearchParams({ lat, lon, radius, limit })}`)
}

// ─── LinkedIn share (access gate) ─────────────────────────────────────────

export const confirmShare = (postUrl, linkedinUsername = null) =>
  apiFetch('/share/confirm', { method: 'POST', body: JSON.stringify({ postUrl, linkedinUsername }) })

export const recoverShare = (linkedinUsername) =>
  apiFetch('/share/recover', { method: 'POST', body: JSON.stringify({ linkedinUsername }) })

export const getShareStatus = () =>
  apiFetch('/share/status')

export const trustShare = () =>
  apiFetch('/share/trust', { method: 'POST' })

// ─── Descargas públicas ───────────────────────────────────────────────────

export function getDescargas() {
  return fetchJSON(`${API}/descargas`)
}

export function getDescargaUrl(periodoId) {
  return fetchJSON(`${API}/descargas/${periodoId}/url`)
}

// ─── Auth (from prediosChile) ──────────────────────────────────────────────

export const requestOtpCode = (email) =>
  apiFetch('/auth/request-code', { method: 'POST', body: JSON.stringify({ email }) })

export const verifyOtpCode = (email, code) =>
  apiFetch('/auth/verify-code', { method: 'POST', body: JSON.stringify({ email, code }) })

export const apiLogout = () =>
  apiFetch('/auth/logout', { method: 'POST' })

// ─── Marketplace (from prediosChile) ───────────────────────────────────────

export const getCatalog = () => apiFetch('/catalog')

export const getAvailability = () => apiFetch('/availability')

export const getComunaStats = () => apiFetch('/comuna-stats')

export const getMetadataContent = (comunaId) => apiFetch(`/metadata/${comunaId}/content`)

export const getMyPurchases = () => apiFetch('/my-purchases')

export const getSecureDownloadLinks = (comunaId) => apiFetch(`/secure-download/${comunaId}`)

export const getDownloadLinks = (paymentId) => apiFetch(`/download/${paymentId}`)

// ─── Payments (from prediosChile) ──────────────────────────────────────────

export const createPayment = (comunaId, comunaName) =>
  apiFetch('/payment/create', { method: 'POST', body: JSON.stringify({ comunaId, comunaName }) })

export const createCartPayment = (items) =>
  apiFetch('/payment/create-cart', {
    method: 'POST',
    body: JSON.stringify({ items: items.map(i => ({ comunaId: i.id, comunaName: i.nombre })) }),
  })

// ─── Admin (from prediosChile) ─────────────────────────────────────────────

export const getAdminUsers = () => apiFetch('/admin/users')
export const deleteAdminUser = (userId) => apiFetch(`/admin/users/${userId}`, { method: 'DELETE' })
export const getAdminGrants = () => apiFetch('/admin/grants')
export const getAdminPurchases = () => apiFetch('/admin/purchases')
export const createGrant = (email, bundleId, durationDays) =>
  apiFetch('/admin/grants', { method: 'POST', body: JSON.stringify({ email, bundleId, durationDays }) })
export const revokeGrant = (grantId) => apiFetch(`/admin/grants/${grantId}`, { method: 'DELETE' })
export const getAdminDomainGrants = () => apiFetch('/admin/domain-grants')
export const createDomainGrant = (domain, durationDays) =>
  apiFetch('/admin/domain-grants', { method: 'POST', body: JSON.stringify({ domain, durationDays }) })
export const revokeDomainGrant = (grantId) => apiFetch(`/admin/domain-grants/${grantId}`, { method: 'DELETE' })
export const getAdminShares = () => apiFetch('/admin/shares')
export const revokeShare = (shareId) => apiFetch(`/admin/shares/${shareId}`, { method: 'DELETE' })
