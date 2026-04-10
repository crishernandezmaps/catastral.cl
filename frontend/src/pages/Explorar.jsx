import { useState, useEffect, useCallback, useRef } from 'react'
import { Link } from 'react-router-dom'
import { Database, MapPin, TrendingUp, Loader2, Hash } from 'lucide-react'
import { getHealth, searchPredios, searchPrediosNearby, resolveComuna } from '../services/api'
import AddressSearch from '../components/AddressSearch'
import RolSearch from '../components/RolSearch'
import Pagination from '../components/Pagination'

// Códigos de destino SII (PDF estructura_detalle_catastral.pdf)
const DESTINO_LABELS = {
  A: 'Agrícola', B: 'Agroindustrial', C: 'Comercial', D: 'Deporte',
  E: 'Educación', F: 'Forestal', G: 'Hotel', H: 'Habitacional',
  I: 'Industrial', L: 'Bodega', M: 'Minería', O: 'Oficina',
  P: 'Admin. Pública', Q: 'Culto', S: 'Salud', T: 'Transporte',
  V: 'Otros', W: 'Sitio Eriazo', Y: 'Gallineros', Z: 'Estacionamiento',
}

function formatCLP(v) {
  if (!v) return '—'
  return '$' + v.toLocaleString('es-CL')
}

function formatM2(v) {
  if (!v) return '—'
  return v.toLocaleString('es-CL') + ' m²'
}

export default function Home() {
  const [stats, setStats] = useState(null)
  const [results, setResults] = useState(null)
  const [loading, setLoading] = useState(false)
  const [searchInfo, setSearchInfo] = useState(null)
  const [page, setPage] = useState(1)
  const [currentSearch, setCurrentSearch] = useState('')
  const [comunaCodigo, setComunaCodigo] = useState(null)
  const [searchMode, setSearchMode] = useState('direccion')

  useEffect(() => { getHealth().then(setStats).catch(() => {}) }, [])

  // ─── Callback de selección desde AddressSearch ─────────────────────────
  // Recibe { lat, lon, searchTerm, display, comuna } del componente.
  // Si hay coordenadas (Nominatim/mapa), usa búsqueda espacial primero.
  // Si no hay resultados espaciales, cae al string matching como fallback.
  const handleLocationSelect = useCallback(async ({ lat, lon, searchTerm, display, comuna }) => {
    setSearchInfo({ searchTerm, display, comuna })
    setPage(1)
    setCurrentSearch(searchTerm)

    // Si hay coordenadas, búsqueda espacial + texto combinada (300m, boost por dirección)
    if (lat && lon) {
      setLoading(true)
      try {
        const nearbyData = await searchPrediosNearby(lat, lon, 300, 1, 25, searchTerm)
        if (nearbyData.pagination.total > 0) {
          lastSearchRef.current = { term: searchTerm, comunaCode: null, lat, lon, radius: nearbyData.radius_used, mode: 'spatial' }
          setResults(nearbyData)
          setSearchInfo(prev => ({ ...prev, mode: 'spatial', radius: nearbyData.radius_used }))
          setLoading(false)
          return
        }
      } catch {}
      setLoading(false)
      // Si no hay resultados espaciales, seguir con string matching
    }

    // Resolver nombre de comuna (Nominatim) a código SII
    let codigo = null
    if (comuna) {
      try {
        const resolved = await resolveComuna(comuna)
        codigo = resolved.codigo
      } catch {}
    }
    setComunaCodigo(codigo)
    doSearch(searchTerm, 1, codigo)
  }, [])

  // ─── BUGFIX: paginación con stale closures ────────────────────────────
  // Problema: doSearch es una función normal (no useCallback), así que al
  // definirse captura el valor de comunaCodigo/currentSearch del render
  // actual. Cuando handlePageChange la llama, esos valores pueden ser stale
  // (especialmente comunaCodigo que arranca en null).
  //
  // Solución: lastSearchRef guarda los parámetros EXACTOS (term + comunaCode)
  // que produjeron los resultados actuales. La paginación siempre lee de
  // este ref, garantizando que la página 2 use los mismos params que la 1.
  //
  // IMPORTANTE: si la cascada de fallback cambia el term (sin_numero) o
  // el comunaCode (sin_comuna), lastSearchRef se actualiza con los nuevos
  // valores, así la paginación usa los params que realmente funcionaron.
  const lastSearchRef = useRef({ term: '', comunaCode: null, lat: null, lon: null, radius: null, mode: 'text' })

  // ─── Búsqueda con cascada de fallback ─────────────────────────────────
  // Siempre recibe los 3 params explícitos. NUNCA usar defaults del state.
  async function doSearch(term, p, comunaCode) {
    if (!term) return
    setLoading(true)
    try {
      const params = { direccion: term, page: p, limit: 25 }
      if (comunaCode) params.comuna = comunaCode

      let data = await searchPredios(params)

      // Cascada de fallback solo en página 1 — si la búsqueda directa
      // no encuentra resultados, se intenta con menos restricciones.
      if (data.pagination.total === 0 && p === 1) {
        // Fallback 1: quitar número de calle, mantener comuna
        if (/\d/.test(term)) {
          const nameOnly = term.replace(/\d+/g, '').trim()
          if (nameOnly.length >= 3) {
            const p1 = { direccion: nameOnly, page: 1, limit: 25 }
            if (comunaCode) p1.comuna = comunaCode
            data = await searchPredios(p1)
            if (data.pagination.total > 0) {
              term = nameOnly
              setSearchInfo(prev => ({ ...prev, searchTerm: nameOnly, fallback: 'sin_numero', originalTerm: term }))
              setCurrentSearch(nameOnly)
            }
          }
        }
        // Fallback 2: quitar filtro de comuna (búsqueda nacional)
        if (data.pagination.total === 0 && comunaCode) {
          data = await searchPredios({ direccion: term, page: 1, limit: 25 })
          if (data.pagination.total > 0) {
            comunaCode = null
            setSearchInfo(prev => ({ ...prev, fallback: 'sin_comuna', originalTerm: term }))
          }
        }
      }

      // Guardar params que funcionaron para paginación (BUGFIX)
      lastSearchRef.current = { term, comunaCode }
      setResults(data)
    } catch { setResults(null) }
    setLoading(false)
  }

  // ─── Paginación ───────────────────────────────────────────────────────
  // Lee de lastSearchRef para evitar stale closures (BUGFIX)
  async function handlePageChange(p) {
    setPage(p)
    const { term, comunaCode, lat, lon, radius, mode } = lastSearchRef.current
    if (mode === 'spatial' && lat && lon) {
      setLoading(true)
      try {
        const data = await searchPrediosNearby(lat, lon, radius || 300, p, 25, term)
        setResults(data)
      } catch { setResults(null) }
      setLoading(false)
    } else {
      doSearch(term, p, comunaCode)
    }
  }

  return (
    <div className="container" style={{ paddingTop: 48, paddingBottom: 64 }}>
      {/* Hero */}
      <div style={{ textAlign: 'center', maxWidth: 700, margin: '0 auto', marginBottom: 32 }}>
        <p style={{ fontSize: '0.75rem', color: '#999', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 16 }}>
          Datos publicos SII · 2018 — 2025
        </p>

        <h1 style={{ fontSize: '2.5rem', marginBottom: 12 }}>
          Explorador Catastral
        </h1>

        <p style={{ fontSize: '1rem', color: '#777', marginBottom: 28 }}>
          Busca cualquier direccion en Chile. Selecciona en el mapa o escribe la direccion.
        </p>
      </div>

      {/* Tabs de modo de búsqueda: Dirección vs Rol SII */}
      <div style={{
        display: 'flex', justifyContent: 'center', gap: 0, marginBottom: 24,
        borderRadius: 9999,
        maxWidth: 300, margin: '0 auto 24px',
        border: '1px solid var(--color-border)',
        overflow: 'hidden',
      }}>
        <button
          onClick={() => setSearchMode('direccion')}
          style={{
            flex: 1, padding: '8px 16px', border: 'none',
            fontSize: '0.82rem', fontWeight: 500, transition: 'all 150ms',
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            background: searchMode === 'direccion' ? '#000' : 'transparent',
            color: searchMode === 'direccion' ? '#fff' : '#999',
          }}
        >
          <MapPin size={14} /> Direccion
        </button>
        <button
          onClick={() => setSearchMode('rol')}
          style={{
            flex: 1, padding: '8px 16px', border: 'none',
            borderLeft: '1px solid var(--color-border)',
            fontSize: '0.82rem', fontWeight: 500, transition: 'all 150ms',
            display: 'flex', alignItems: 'center', justifyContent: 'center', gap: 6,
            background: searchMode === 'rol' ? '#000' : 'transparent',
            color: searchMode === 'rol' ? '#fff' : '#999',
          }}
        >
          <Hash size={14} /> Rol SII
        </button>
      </div>

      {/* Componente de búsqueda activo */}
      <div style={{ maxWidth: 800, margin: '0 auto', marginBottom: 32 }}>
        {searchMode === 'direccion' ? (
          <AddressSearch onLocationSelect={handleLocationSelect} />
        ) : (
          <RolSearch />
        )}
      </div>

      {/* Stats removed — shown on Home page */}

      {/* Loading */}
      {loading && (
        <div style={{ textAlign: 'center', padding: 40, color: 'var(--color-text-muted)' }}>
          <Loader2 size={24} style={{ animation: 'spin 1s linear infinite' }} />
        </div>
      )}

      {/* Tabla de resultados */}
      {!loading && results && (
        <div style={{ maxWidth: 1000, margin: '0 auto' }}>
          {searchInfo && (
            <div style={{ marginBottom: 16, fontSize: '0.85rem', color: 'var(--color-text-muted)' }}>
              {searchInfo.mode === 'spatial'
                ? <>Predios cercanos a <strong style={{ color: 'var(--color-text-primary)' }}>"{searchInfo.searchTerm}"</strong> (radio {searchInfo.radius}m) — {results.pagination.total.toLocaleString('es-CL')} resultados</>
                : searchInfo.fallback === 'sin_numero'
                ? <>No se encontró <strong style={{ color: 'var(--color-text-primary)' }}>"{searchInfo.originalTerm}"</strong> exacto. Mostrando resultados para <strong style={{ color: '#000', fontWeight: 600 }}>"{searchInfo.searchTerm}"</strong> — {results.pagination.total.toLocaleString('es-CL')} resultados</>
                : searchInfo.fallback === 'sin_comuna'
                ? <>No se encontró en la comuna. Mostrando resultados de <strong style={{ color: '#000', fontWeight: 600 }}>todo Chile</strong> — {results.pagination.total.toLocaleString('es-CL')} resultados</>
                : <>Buscando <strong style={{ color: 'var(--color-text-primary)' }}>"{searchInfo.searchTerm}"</strong>{searchInfo.comuna ? <> en <strong style={{ color: '#000', fontWeight: 600 }}>{searchInfo.comuna}</strong></> : ''} — {results.pagination.total.toLocaleString('es-CL')} resultados</>
              }
            </div>
          )}

          <div style={{ overflowX: 'auto' }}>
            <table>
              <thead>
                <tr>
                  <th>Rol</th>
                  <th>Dirección</th>
                  <th>Comuna</th>
                  <th>Destino</th>
                  <th style={{ textAlign: 'right' }}>Superficie</th>
                  <th style={{ textAlign: 'right' }}>Avalúo Total</th>
                </tr>
              </thead>
              <tbody>
                {results.data.map((p, i) => (
                  <tr key={i}>
                    <td>
                      <Link to={`/predio/${p.comuna}/${p.manzana}/${p.predio}`}
                        style={{ fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
                        {p.comuna}-{p.manzana}-{p.predio}
                      </Link>
                    </td>
                    <td style={{ maxWidth: 280, overflow: 'hidden', textOverflow: 'ellipsis', whiteSpace: 'nowrap' }}>
                      {p.rc_direccion || '—'}
                    </td>
                    <td>{p.comuna_nombre || p.comuna}</td>
                    <td>
                      <span className={`tag ${p.rc_cod_ubicacion === 'U' ? 'tag-urban' : 'tag-rural'}`}>
                        {DESTINO_LABELS[p.dc_cod_destino] || p.dc_cod_destino || '—'}
                      </span>
                    </td>
                    <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{formatM2(p.superficie)}</td>
                    <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{formatCLP(p.rc_avaluo_total)}</td>
                  </tr>
                ))}
                {results.data.length === 0 && (
                  <tr><td colSpan={6} style={{ textAlign: 'center', padding: 40, color: 'var(--color-text-muted)' }}>
                    Sin resultados para esta dirección. Intenta con otra búsqueda o haz clic en el mapa.
                  </td></tr>
                )}
              </tbody>
            </table>
          </div>

          <Pagination
            page={results.pagination.page}
            pages={results.pagination.pages}
            total={results.pagination.total}
            onPage={handlePageChange}
          />
        </div>
      )}
    </div>
  )
}
