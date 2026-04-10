import { useState, useEffect, useRef, useCallback, useMemo } from 'react'
import { MapContainer, TileLayer, Marker, Popup, CircleMarker, Tooltip as LTooltip, useMap } from 'react-leaflet'
import { useNavigate } from 'react-router-dom'
import L from 'leaflet'
import { Search, MapPin, Loader2, X, Database } from 'lucide-react'
import { useDebounce } from '../hooks/useDebounce'
import { autocompletePredios, getNearbyMarkers, getNearbyPolygons } from '../services/api'
import PropertyPolygons from './PropertyPolygons'

import 'leaflet/dist/leaflet.css'

// ─── Normalización de direcciones ────────────────────────────────────────────
// HERE devuelve nombres completos ("Avenida Condell 738") pero el SII
// los abrevia ("CONDELL 738"). Estos regex strip prefijos viales y títulos
// para mejorar el match contra la base de datos SII.
const STREET_PREFIXES = /^(avenida|av\.|av |calle|pasaje|psje\.|psje |pje\.|pje |camino|ruta|autopista|boulevard|bvd\.|costanera|diagonal|circunvalación|gran )/i
const TITLE_WORDS = /\b(presidente|general|coronel|capitán|capitan|teniente|sargento|almirante|doctor|doctora|profesor|profesora|libertador|bernardo|santo|santa|san |don |doña |comandante|brigadier|mariscal|obispo|monseñor|cardenal|padre |madre |fray |sor |hermano|lateral|poniente|oriente|norte|sur)\b/gi
function cleanStreet(road, number) {
  let clean = road.replace(STREET_PREFIXES, '').trim()
  clean = clean.replace(TITLE_WORDS, '').replace(/\s+/g, ' ').trim()
  return (clean + ' ' + number).trim()
}

// ─── Leaflet marker icons ────────────────────────────────────────────────────
delete L.Icon.Default.prototype._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
})

const limeIcon = new L.Icon({
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
  iconSize: [25, 41], iconAnchor: [12, 41], popupAnchor: [1, -34],
  className: 'lime-marker',
})

function FlyTo({ center }) {
  const map = useMap()
  useEffect(() => {
    if (center) map.flyTo(center, 17, { duration: 1.2 })
  }, [center, map])
  return null
}

async function geocode(query) {
  const res = await fetch(`/api/geocode?` + new URLSearchParams({ q: query }))
  if (!res.ok) return []
  return res.json()
}

async function reverseGeocode(lat, lon) {
  const res = await fetch(`/api/revgeocode?` + new URLSearchParams({ lat, lon }))
  if (!res.ok) return null
  return res.json()
}

// ─── Componente principal ────────────────────────────────────────────────────
// Búsqueda dual: HERE (geocoding) + autocomplete SII en paralelo.
// Emite onLocationSelect({ lat, lon, searchTerm, display, comuna }) al padre (Home).
const DESTINO_COLORS = {
  H: '#000', C: '#444', O: '#666', I: '#888',
  L: '#aaa', Z: '#bbb', E: '#333', S: '#555',
}

export default function AddressSearch({ onLocationSelect }) {
  const navigate = useNavigate()
  const [markers, setMarkers] = useState([])
  const [polygons, setPolygons] = useState(null)
  const [query, setQuery] = useState('')
  const [suggestions, setSuggestions] = useState([])
  const [siiSuggestions, setSiiSuggestions] = useState([])
  const [loading, setLoading] = useState(false)
  const [selected, setSelected] = useState(null)
  const [mapCenter, setMapCenter] = useState([-33.45, -70.65])
  const [showMap, setShowMap] = useState(false)
  const [showDropdown, setShowDropdown] = useState(true)
  const debounced = useDebounce(query, 500)
  const inputRef = useRef(null)

  // ─── BUGFIX: dropdown race condition ─────────────────────────────────────
  // Problema: el useEffect dispara un fetch con debounce. Si el usuario
  // selecciona una sugerencia antes de que el fetch resuelva, el .then()
  // vuelve a llenar suggestions y reabre el dropdown.
  //
  // Solución: dismissedRef es un ref (no state, para evitar re-renders)
  // que bloquea tanto el disparo de nuevos fetches como el procesamiento
  // de resultados de fetches en vuelo. Se usa en conjunto con showDropdown
  // (state) que controla la visibilidad del render.
  //
  // - dismissedRef = true  → en TODOS los handlers de selección
  // - dismissedRef = false → solo cuando el usuario vuelve a escribir (onChange)
  // - Se chequea ANTES del fetch Y dentro del .then() callback
  const dismissedRef = useRef(false)

  // Detecta input directo de rol catastral (ej: 15103-12-45)
  const isRol = /^\d{1,5}-\d{1,4}-\d{1,5}$/.test(query.trim())

  // ─── Autocomplete con debounce ───────────────────────────────────────────
  useEffect(() => {
    if (!debounced || debounced.length < 3) { setSuggestions([]); setSiiSuggestions([]); return }
    if (isRol) {
      setSuggestions([])
      setSiiSuggestions([])
      return
    }
    // No buscar si el usuario ya seleccionó algo (ver BUGFIX arriba)
    if (dismissedRef.current) return

    setLoading(true)
    Promise.all([
      geocode(debounced).then(r => r.slice(0, 5)).catch(() => []),
      autocompletePredios(debounced).catch(() => []),
    ]).then(([geo, sii]) => {
      // Descartar si el usuario seleccionó mientras el fetch volaba
      if (dismissedRef.current) return
      setSuggestions(geo)
      setSiiSuggestions(sii)
      if (geo.length > 0 && !showMap) setShowMap(true)
    }).finally(() => { if (!dismissedRef.current) setLoading(false) })
  }, [debounced])

  // ─── Selección de sugerencia Nominatim ───────────────────────────────────
  const handleSelect = useCallback((item) => {
    const lat = parseFloat(item.lat)
    const lon = parseFloat(item.lon)
    setSelected({ lat, lon, display: item.display_name, address: item.address })
    setMapCenter([lat, lon])
    // Limpiar dropdown y bloquear fetches en vuelo (BUGFIX)
    setSuggestions([])
    setSiiSuggestions([])
    setShowDropdown(false)
    dismissedRef.current = true
    setShowMap(true)

    // Extraer nombre de calle + número para búsqueda SII
    const addr = item.address || {}
    const road = addr.road || ''
    const number = addr.house_number || ''
    const searchTerm = cleanStreet(road, number)

    if (searchTerm && onLocationSelect) {
      onLocationSelect({ lat, lon, searchTerm, display: item.display_name, comuna: addr.city || addr.town || addr.suburb || '' })
    }
    getNearbyMarkers(lat, lon, 300).then(setMarkers).catch(() => {})
    getNearbyPolygons(lat, lon, 300, 200).then(data => { console.log('Polygons loaded:', data?.features?.length); setPolygons(data) }).catch(e => { console.error('Polygons error:', e); setPolygons(null) })
  }, [onLocationSelect])

  // ─── Búsqueda espacial desde coordenadas (click o drag) ─────────────────
  const searchFromCoords = useCallback(async (lat, lng) => {
    setLoading(true)
    const fallbackDisplay = `${lat.toFixed(5)}, ${lng.toFixed(5)}`
    let searchTerm = fallbackDisplay
    let display = fallbackDisplay
    let comuna = ''

    try {
      const result = await reverseGeocode(lat, lng)
      if (result) {
        const addr = result.address || {}
        const road = addr.road || ''
        const number = addr.house_number || ''
        const cleaned = cleanStreet(road, number)
        if (cleaned) searchTerm = cleaned
        display = result.display_name || fallbackDisplay
        comuna = addr.city || addr.town || addr.suburb || ''
        setSelected({ lat, lon: lng, display, address: addr })
      } else {
        setSelected({ lat, lon: lng, display: fallbackDisplay, address: {} })
      }
    } catch {
      setSelected({ lat, lon: lng, display: fallbackDisplay, address: {} })
    }

    setLoading(false)
    if (onLocationSelect) {
      onLocationSelect({ lat, lon: lng, searchTerm, display, comuna })
    }
    getNearbyMarkers(lat, lng, 300).then(setMarkers).catch(() => {})
    getNearbyPolygons(lat, lng, 300, 50).then(setPolygons).catch(() => setPolygons(null))
  }, [onLocationSelect])

  // ─── Click en mapa ──────────────────────────────────────────────────────────
  const handleMapClick = useCallback((e) => {
    searchFromCoords(e.latlng.lat, e.latlng.lng)
  }, [searchFromCoords])

  // ─── Drag marker ───────────────────────────────────────────────────────────
  const handleMarkerDrag = useCallback(({ lat, lng }) => {
    searchFromCoords(lat, lng)
  }, [searchFromCoords])

  // ─── Selección de sugerencia SII ─────────────────────────────────────────
  function handleSiiSelect(item) {
    // Limpiar dropdown y bloquear fetches en vuelo (BUGFIX)
    setSuggestions([])
    setSiiSuggestions([])
    setShowDropdown(false)
    dismissedRef.current = true
    if (onLocationSelect) {
      onLocationSelect({ searchTerm: item.direccion, display: `${item.direccion}, ${item.comuna_nombre}`, comuna: item.comuna_nombre })
    }
  }

  // ─── Búsqueda directa por rol (ej: 15103-12-45) ─────────────────────────
  function handleRolSubmit() {
    const rol = query.trim()
    // Limpiar dropdown y bloquear fetches en vuelo (BUGFIX)
    setSuggestions([])
    setSiiSuggestions([])
    setShowDropdown(false)
    dismissedRef.current = true
    if (onLocationSelect) {
      onLocationSelect({ searchTerm: rol, display: `Rol ${rol}`, comuna: '' })
    }
  }

  function clear() {
    setQuery('')
    setSuggestions([])
    setSiiSuggestions([])
    setSelected(null)
  }

  // ─── Render ──────────────────────────────────────────────────────────────
  return (
    <div>
      {/* Search input */}
      <div style={{ position: 'relative', maxWidth: 600, margin: '0 auto', zIndex: 1000 }}>
        <MapPin size={18} style={{ position: 'absolute', left: 14, top: '50%', transform: 'translateY(-50%)', color: 'var(--color-text-muted)' }} />
        <input
          ref={inputRef}
          value={query}
          // Al escribir: reabrir dropdown y desbloquear fetches (BUGFIX)
          onChange={e => { setQuery(e.target.value); setShowDropdown(true); dismissedRef.current = false }}
          onKeyDown={e => { if (e.key === 'Enter' && isRol) handleRolSubmit() }}
          placeholder="Dirección o rol (ej: Condell 738, Providencia o 15103-12-45)"
          style={{ width: '100%', paddingLeft: 42, paddingRight: 40, padding: '14px 40px 14px 42px', fontSize: '1rem' }}
        />
        {loading && <Loader2 size={18} style={{ position: 'absolute', right: 14, top: '50%', transform: 'translateY(-50%)', color: '#000', animation: 'spin 1s linear infinite' }} />}
        {query && !loading && <X size={18} onClick={clear} style={{ position: 'absolute', right: 14, top: '50%', transform: 'translateY(-50%)', color: 'var(--color-text-muted)', cursor: 'pointer' }} />}
        <style>{`@keyframes spin { to { transform: translateY(-50%) rotate(360deg) } }`}</style>

        {/* Dropdown de sugerencias — controlado por showDropdown (state) + dismissedRef (ref) */}
        {showDropdown && (suggestions.length > 0 || siiSuggestions.length > 0 || isRol) && (
          <div style={{
            position: 'absolute', top: '100%', left: 0, right: 0, zIndex: 200,
            background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
            borderRadius: '0 0 var(--radius-md) var(--radius-md)',
            maxHeight: 340, overflowY: 'auto',
            boxShadow: '0 8px 30px rgba(0,0,0,0.08)',
          }}>
            {/* Sugerencia de rol directo */}
            {isRol && (
              <div onClick={handleRolSubmit} style={{
                padding: '10px 14px', cursor: 'pointer', fontSize: '0.85rem',
                color: '#000', borderBottom: '1px solid var(--color-border)',
                display: 'flex', alignItems: 'center', gap: 8,
                background: 'rgba(0,0,0,0.05)',
              }}
                onMouseEnter={e => e.currentTarget.style.background = 'rgba(0,0,0,0.1)'}
                onMouseLeave={e => e.currentTarget.style.background = 'rgba(0,0,0,0.05)'}
              >
                <Search size={14} />
                <span>Buscar rol <strong>{query.trim()}</strong></span>
              </div>
            )}

            {/* Sugerencias Nominatim (geocoding) */}
            {suggestions.map((s, i) => (
              <div key={`nom-${i}`} onClick={() => handleSelect(s)} style={{
                padding: '10px 14px', cursor: 'pointer', fontSize: '0.85rem',
                color: 'var(--color-text-secondary)', borderBottom: '1px solid var(--color-border)',
                display: 'flex', alignItems: 'flex-start', gap: 8,
                transition: 'background 100ms',
              }}
                onMouseEnter={e => e.currentTarget.style.background = 'rgba(0,0,0,0.05)'}
                onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
              >
                <MapPin size={14} style={{ marginTop: 2, flexShrink: 0, color: '#000' }} />
                <span>{s.display_name}</span>
              </div>
            ))}

            {/* Sugerencias de direcciones SII reales */}
            {siiSuggestions.length > 0 && (
              <>
                <div style={{
                  padding: '6px 14px', fontSize: '0.7rem', fontWeight: 600,
                  color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em',
                  background: 'rgba(255,255,255,0.02)', borderBottom: '1px solid var(--color-border)',
                  display: 'flex', alignItems: 'center', gap: 6,
                }}>
                  <Database size={10} /> Direcciones SII
                </div>
                {siiSuggestions.map((s, i) => (
                  <div key={`sii-${i}`} onClick={() => handleSiiSelect(s)} style={{
                    padding: '10px 14px', cursor: 'pointer', fontSize: '0.85rem',
                    color: 'var(--color-text-secondary)', borderBottom: '1px solid var(--color-border)',
                    display: 'flex', alignItems: 'flex-start', gap: 8,
                    transition: 'background 100ms',
                  }}
                    onMouseEnter={e => e.currentTarget.style.background = 'rgba(0,0,0,0.05)'}
                    onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
                  >
                    <Database size={14} style={{ marginTop: 2, flexShrink: 0, color: 'var(--color-text-muted)' }} />
                    <span>{s.direccion} <span style={{ color: 'var(--color-text-muted)', fontSize: '0.75rem' }}>· {s.comuna_nombre}</span></span>
                  </div>
                ))}
              </>
            )}
          </div>
        )}
      </div>

      {/* Mapa Leaflet */}
      {showMap && (
        <div style={{
          marginTop: 20, borderRadius: 'var(--radius-lg)', overflow: 'hidden',
          border: '1px solid var(--color-border)', height: 350,
        }}>
          <MapContainer
            center={mapCenter} zoom={13} style={{ height: '100%', width: '100%' }}
            scrollWheelZoom={true}
          >
            <TileLayer
              attribution='Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics'
              url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
            />
            <FlyTo center={selected ? [selected.lat, selected.lon] : null} />
            <PropertyPolygons data={polygons} />
            {selected && (
              <DraggableMarker position={[selected.lat, selected.lon]} icon={limeIcon} onDragEnd={handleMarkerDrag}>
                <Popup>
                  <div style={{ fontSize: '0.8rem', maxWidth: 200 }}>
                    {selected.display}
                  </div>
                </Popup>
              </DraggableMarker>
            )}
            <MapClickHandler onClick={handleMapClick} />
            {/* Predios cercanos: individuales o agrupados por edificio */}
            <GroupedMarkers markers={markers} navigate={navigate} />
          </MapContainer>
        </div>
      )}

      {/* Ubicación seleccionada */}
      {selected && (
        <div style={{
          marginTop: 12, padding: '10px 16px',
          background: 'rgba(0,0,0,0.05)', border: '1px solid rgba(0,0,0,0.2)',
          borderRadius: 'var(--radius-md)', fontSize: '0.8rem', color: 'var(--color-text-secondary)',
          display: 'flex', alignItems: 'center', gap: 8,
        }}>
          <MapPin size={14} color="#000000" />
          {selected.display}
        </div>
      )}
    </div>
  )
}

function DraggableMarker({ position, icon, onDragEnd, children }) {
  const markerRef = useRef(null)
  const eventHandlers = useMemo(() => ({
    dragend() {
      const marker = markerRef.current
      if (marker) {
        const { lat, lng } = marker.getLatLng()
        onDragEnd({ lat, lng })
      }
    },
  }), [onDragEnd])

  return (
    <Marker position={position} icon={icon} draggable={true} eventHandlers={eventHandlers} ref={markerRef}>
      {children}
    </Marker>
  )
}

function GroupedMarkers({ markers, navigate }) {
  const groups = useMemo(() => {
    const map = new Map()
    for (const p of markers) {
      const key = `${p.lat},${p.lon}`
      if (!map.has(key)) map.set(key, [])
      map.get(key).push(p)
    }
    return Array.from(map.values())
  }, [markers])

  return groups.map(group => {
    if (group.length === 1) {
      const p = group[0]
      return (
        <CircleMarker
          key={`${p.c}-${p.m}-${p.p}`}
          center={[p.lat, p.lon]}
          radius={5}
          pathOptions={{
            color: DESTINO_COLORS[p.t] || '#000000',
            fillColor: DESTINO_COLORS[p.t] || '#000000',
            fillOpacity: 0.6,
            weight: 1,
          }}
          eventHandlers={{ click: () => navigate(`/predio/${p.c}/${p.m}/${p.p}`) }}
        >
          <LTooltip direction="top" offset={[0, -5]} className="predio-tooltip">
            <span style={{ fontSize: '0.7rem' }}>
              <strong>{p.c}-{p.m}-{p.p}</strong> {p.d || ''}
            </span>
          </LTooltip>
        </CircleMarker>
      )
    }
    const p0 = group[0]
    return (
      <CircleMarker
        key={`bldg-${p0.lat}-${p0.lon}`}
        center={[p0.lat, p0.lon]}
        radius={10}
        pathOptions={{
          color: '#fff',
          fillColor: '#8b5cf6',
          fillOpacity: 0.85,
          weight: 2,
        }}
      >
        <LTooltip direction="top" offset={[0, -8]} className="predio-tooltip" permanent={false}>
          <span style={{ fontSize: '0.7rem' }}>
            <strong>Edificio</strong> · {group.length} unidades
          </span>
        </LTooltip>
        <BuildingPopup units={group} navigate={navigate} />
      </CircleMarker>
    )
  })
}

const ITEMS_PER_PAGE = 5

function BuildingPopup({ units, navigate }) {
  const [page, setPage] = useState(0)
  // Sort: H (habitacional) first, then by predio ascending
  const sorted = [...units].sort((a, b) => {
    const aH = a.t === 'H' ? 0 : a.t === 'C' ? 1 : a.t === 'O' ? 2 : 3
    const bH = b.t === 'H' ? 0 : b.t === 'C' ? 1 : b.t === 'O' ? 2 : 3
    if (aH !== bH) return aH - bH
    return (a.p || 0) - (b.p || 0)
  })
  const totalPages = Math.ceil(sorted.length / ITEMS_PER_PAGE)
  const slice = sorted.slice(page * ITEMS_PER_PAGE, (page + 1) * ITEMS_PER_PAGE)

  return (
    <Popup>
      <div style={{ minWidth: 220, fontSize: '0.78rem', fontFamily: 'DM Sans, sans-serif' }}>
        <div style={{ fontWeight: 700, marginBottom: 6, display: 'flex', justifyContent: 'space-between', alignItems: 'center' }}>
          <span>Edificio · {units.length} unidades</span>
          {totalPages > 1 && (
            <span style={{ fontWeight: 400, fontSize: '0.7rem', color: '#888' }}>
              {page + 1}/{totalPages}
            </span>
          )}
        </div>
        {slice.map(p => (
          <div
            key={`${p.c}-${p.m}-${p.p}`}
            onClick={() => navigate(`/predio/${p.c}/${p.m}/${p.p}`)}
            style={{
              padding: '4px 6px', margin: '2px 0', borderRadius: 4, cursor: 'pointer',
              display: 'flex', justifyContent: 'space-between', alignItems: 'center',
              transition: 'background 100ms',
            }}
            onMouseEnter={e => e.currentTarget.style.background = '#f0f0f0'}
            onMouseLeave={e => e.currentTarget.style.background = 'transparent'}
          >
            <span>
              <strong style={{ color: DESTINO_COLORS[p.t] || '#333' }}>{p.c}-{p.m}-{p.p}</strong>
              {p.d && <span style={{ marginLeft: 4, color: '#666' }}>{p.d}</span>}
            </span>
            <span style={{
              background: DESTINO_COLORS[p.t] || '#eee',
              color: '#000', fontSize: '0.6rem', fontWeight: 700,
              padding: '1px 5px', borderRadius: 3,
            }}>{p.t || '?'}</span>
          </div>
        ))}
        {totalPages > 1 && (
          <div style={{ display: 'flex', justifyContent: 'center', gap: 8, marginTop: 6 }}>
            <button
              onClick={e => { e.stopPropagation(); setPage(p => Math.max(0, p - 1)) }}
              disabled={page === 0}
              style={{
                background: 'none', border: '1px solid #ccc', borderRadius: 4,
                padding: '2px 10px', cursor: page === 0 ? 'default' : 'pointer',
                opacity: page === 0 ? 0.3 : 1, fontSize: '0.75rem',
              }}
            >←</button>
            <button
              onClick={e => { e.stopPropagation(); setPage(p => Math.min(totalPages - 1, p + 1)) }}
              disabled={page === totalPages - 1}
              style={{
                background: 'none', border: '1px solid #ccc', borderRadius: 4,
                padding: '2px 10px', cursor: page === totalPages - 1 ? 'default' : 'pointer',
                opacity: page === totalPages - 1 ? 0.3 : 1, fontSize: '0.75rem',
              }}
            >→</button>
          </div>
        )}
      </div>
    </Popup>
  )
}

function MapClickHandler({ onClick }) {
  const map = useMap()
  useEffect(() => {
    map.on('click', onClick)
    return () => map.off('click', onClick)
  }, [map, onClick])
  return null
}
