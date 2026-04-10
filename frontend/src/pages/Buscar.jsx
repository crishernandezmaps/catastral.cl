import { useState, useEffect, useCallback } from 'react'
import { useSearchParams, Link } from 'react-router-dom'
import { Search, Filter, X, Loader2 } from 'lucide-react'
import { searchPredios, getComunas, getDestinos } from '../services/api'
import { useDebounce } from '../hooks/useDebounce'
import Pagination from '../components/Pagination'

const DESTINO_LABELS = {
  H: 'Habitacional', C: 'Comercial', I: 'Industrial',
  O: 'Oficina', E: 'Educación', S: 'Salud',
  A: 'Agrícola', F: 'Forestal', M: 'Minería',
  D: 'Deportes', G: 'Estacionamiento', T: 'Transporte',
  B: 'Bodega', L: 'Hotel/Motel', Z: 'Otros',
}

function formatCLP(v) {
  if (!v) return '—'
  return '$' + v.toLocaleString('es-CL')
}

function formatM2(v) {
  if (!v) return '—'
  return v.toLocaleString('es-CL') + ' m²'
}

export default function Buscar() {
  const [searchParams, setSearchParams] = useSearchParams()
  const [comunas, setComunas] = useState([])
  const [destinos, setDestinos] = useState([])
  const [showFilters, setShowFilters] = useState(false)
  const [loading, setLoading] = useState(false)
  const [result, setResult] = useState(null)

  const [filters, setFilters] = useState({
    direccion: searchParams.get('direccion') || '',
    comuna: searchParams.get('comuna') || '',
    destino: searchParams.get('destino') || '',
    sup_min: searchParams.get('sup_min') || '',
    sup_max: searchParams.get('sup_max') || '',
    avaluo_min: searchParams.get('avaluo_min') || '',
    avaluo_max: searchParams.get('avaluo_max') || '',
    page: parseInt(searchParams.get('page') || '1'),
  })

  const debouncedDir = useDebounce(filters.direccion)

  useEffect(() => {
    getComunas().then(setComunas).catch(() => {})
    getDestinos().then(setDestinos).catch(() => {})
  }, [])

  const doSearch = useCallback(async () => {
    setLoading(true)
    try {
      const params = { ...filters, direccion: debouncedDir }
      const data = await searchPredios(params)
      setResult(data)
    } catch { setResult(null) }
    setLoading(false)
  }, [filters.comuna, filters.destino, filters.sup_min, filters.sup_max,
      filters.avaluo_min, filters.avaluo_max, filters.page, debouncedDir])

  useEffect(() => { doSearch() }, [doSearch])

  function updateFilter(key, value) {
    setFilters(prev => ({ ...prev, [key]: value, page: key === 'page' ? value : 1 }))
  }

  function clearFilters() {
    setFilters({ direccion: '', comuna: '', destino: '', sup_min: '', sup_max: '', avaluo_min: '', avaluo_max: '', page: 1 })
  }

  const hasFilters = filters.comuna || filters.destino || filters.sup_min || filters.sup_max || filters.avaluo_min || filters.avaluo_max

  return (
    <div className="container" style={{ paddingTop: 32, paddingBottom: 48 }}>
      <div style={{ display: 'flex', gap: 12, marginBottom: 20, alignItems: 'center' }}>
        <div style={{ flex: 1, position: 'relative' }}>
          <Search size={18} style={{
            position: 'absolute', left: 14, top: '50%', transform: 'translateY(-50%)',
            color: 'var(--color-text-muted)',
          }} />
          <input
            value={filters.direccion}
            onChange={e => updateFilter('direccion', e.target.value)}
            placeholder="Buscar por dirección..."
            style={{ width: '100%', paddingLeft: 42, padding: '10px 14px 10px 42px' }}
          />
        </div>
        <button className="btn-ghost" onClick={() => setShowFilters(!showFilters)}
          style={{ display: 'flex', alignItems: 'center', gap: 6, color: hasFilters ? '#000000' : undefined }}>
          <Filter size={16} /> Filtros {hasFilters && '•'}
        </button>
      </div>

      {showFilters && (
        <div style={{
          display: 'grid', gridTemplateColumns: 'repeat(auto-fill, minmax(200px, 1fr))', gap: 12,
          padding: 16, marginBottom: 20,
          background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
          borderRadius: 'var(--radius-lg)',
        }}>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Comuna</label>
            <select value={filters.comuna} onChange={e => updateFilter('comuna', e.target.value)} style={{ width: '100%' }}>
              <option value="">Todas</option>
              {comunas.map(c => <option key={c.codigo} value={c.codigo}>{c.nombre}</option>)}
            </select>
          </div>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Destino</label>
            <select value={filters.destino} onChange={e => updateFilter('destino', e.target.value)} style={{ width: '100%' }}>
              <option value="">Todos</option>
              {destinos.map(d => <option key={d} value={d}>{DESTINO_LABELS[d] || d}</option>)}
            </select>
          </div>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Sup. mín (m²)</label>
            <input type="number" value={filters.sup_min} onChange={e => updateFilter('sup_min', e.target.value)} style={{ width: '100%' }} />
          </div>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Sup. máx (m²)</label>
            <input type="number" value={filters.sup_max} onChange={e => updateFilter('sup_max', e.target.value)} style={{ width: '100%' }} />
          </div>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Avalúo mín ($)</label>
            <input type="number" value={filters.avaluo_min} onChange={e => updateFilter('avaluo_min', e.target.value)} style={{ width: '100%' }} />
          </div>
          <div>
            <label style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', display: 'block', marginBottom: 4 }}>Avalúo máx ($)</label>
            <input type="number" value={filters.avaluo_max} onChange={e => updateFilter('avaluo_max', e.target.value)} style={{ width: '100%' }} />
          </div>
          <div style={{ display: 'flex', alignItems: 'end' }}>
            <button className="btn-ghost" onClick={clearFilters} style={{ display: 'flex', alignItems: 'center', gap: 4 }}>
              <X size={14} /> Limpiar
            </button>
          </div>
        </div>
      )}

      {loading && (
        <div style={{ textAlign: 'center', padding: 40, color: 'var(--color-text-muted)' }}>
          <Loader2 size={24} style={{ animation: 'spin 1s linear infinite' }} />
          <style>{`@keyframes spin { to { transform: rotate(360deg) } }`}</style>
        </div>
      )}

      {!loading && result && (
        <>
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
                {result.data.map((p, i) => (
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
                    <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                      {formatM2(p.superficie)}
                    </td>
                    <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>
                      {formatCLP(p.rc_avaluo_total)}
                    </td>
                  </tr>
                ))}
                {result.data.length === 0 && (
                  <tr><td colSpan={6} style={{ textAlign: 'center', padding: 40, color: 'var(--color-text-muted)' }}>
                    Sin resultados
                  </td></tr>
                )}
              </tbody>
            </table>
          </div>
          <Pagination
            page={result.pagination.page}
            pages={result.pagination.pages}
            total={result.pagination.total}
            onPage={p => updateFilter('page', p)}
          />
        </>
      )}
    </div>
  )
}
