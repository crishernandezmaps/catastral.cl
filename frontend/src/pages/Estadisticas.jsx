import { useState, useEffect } from 'react'
import { Search, TrendingUp, Home, Building2, Landmark, MapPin } from 'lucide-react'
import { BarChart, Bar, XAxis, YAxis, Tooltip, ResponsiveContainer, Cell, PieChart, Pie } from 'recharts'

const API = '/api'

const COLORS = ['#000000', '#3b82f6', '#f59e0b', '#ef4444', '#8b5cf6', '#06b6d4', '#22c55e', '#ec4899']

function formatCLP(v) {
  if (!v) return '—'
  if (v >= 1e12) return `$${(v / 1e12).toFixed(1)}T`
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`
  return '$' + v.toLocaleString('es-CL')
}

function formatNum(v) {
  if (!v) return '—'
  return v.toLocaleString('es-CL')
}

function StatCard({ icon: Icon, label, value, sub }) {
  return (
    <div className="stat-card">
      <Icon size={20} color="#000000" style={{ marginBottom: 8 }} />
      <div className="stat-value" style={{ fontSize: '1.6rem' }}>{value}</div>
      <div className="stat-label">{label}</div>
      {sub && <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', marginTop: 4 }}>{sub}</div>}
    </div>
  )
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{ background: '#1f1f1f', border: '1px solid #27272a', borderRadius: 8, padding: '10px 14px', fontSize: '0.8rem' }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{label || payload[0]?.name}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color || '#000000' }}>
          {p.dataKey === 'total' || p.name === 'total' ? formatNum(p.value) + ' predios' : formatCLP(p.value)}
        </div>
      ))}
    </div>
  )
}

export default function Estadisticas() {
  const [resumen, setResumen] = useState(null)
  const [comunas, setComunas] = useState([])
  const [search, setSearch] = useState('')
  const [sortKey, setSortKey] = useState('mediana_avaluo')
  const [sortDir, setSortDir] = useState(-1)

  const [loadError, setLoadError] = useState(false)

  useEffect(() => {
    // Resumen loads from static file (regenerated each semester)
    fetch('/stats-resumen.json')
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json() })
      .then(setResumen)
      .catch(() => setLoadError(true))
    // Comunas detail still from API (for sorting/filtering)
    fetch(`${API}/estadisticas/comunas`)
      .then(r => { if (!r.ok) throw new Error(r.status); return r.json() })
      .then(setComunas)
      .catch(() => {})
  }, [])

  function toggleSort(key) {
    if (sortKey === key) setSortDir(d => d * -1)
    else { setSortKey(key); setSortDir(-1) }
  }

  const filtered = comunas
    .filter(c => !search || c.nombre?.toLowerCase().includes(search.toLowerCase()) || c.region?.toLowerCase().includes(search.toLowerCase()))
    .sort((a, b) => ((a[sortKey] ?? 0) > (b[sortKey] ?? 0) ? 1 : -1) * sortDir)

  const SortIcon = ({ k }) => sortKey === k ? (sortDir > 0 ? ' ↑' : ' ↓') : ''

  return (
    <div className="container" style={{ paddingTop: 32, paddingBottom: 48 }}>
      <h2 style={{ marginBottom: 8 }}>Estadísticas Catastrales</h2>
      <p style={{ color: 'var(--color-text-muted)', fontSize: '0.9rem', marginBottom: 24 }}>
        Datos del semestre 2025-S2 · {resumen ? formatNum(resumen.total_predios) : '—'} predios en 342 comunas
      </p>

      {!resumen && !loadError && (
        <div style={{ padding: '16px 0 24px' }}>
          <div style={{ height: 3, background: 'var(--color-border)', borderRadius: 2, overflow: 'hidden' }}>
            <div style={{
              height: '100%', width: '40%', background: 'var(--color-accent-primary)',
              borderRadius: 2, animation: 'progress-slide 1.2s ease-in-out infinite',
            }} />
          </div>
          <style>{`@keyframes progress-slide { 0% { margin-left: 0%; width: 30%; } 50% { margin-left: 35%; width: 50%; } 100% { margin-left: 100%; width: 30%; } }`}</style>
        </div>
      )}
      {loadError && (
        <div style={{ padding: '16px 0', color: 'var(--color-text-muted)', fontSize: '0.9rem' }}>
          Error al cargar estadisticas. <button onClick={() => window.location.reload()} style={{ color: 'var(--color-accent-primary)', textDecoration: 'underline' }}>Reintentar</button>
        </div>
      )}

      {resumen && (
        <>
          {/* Summary cards */}
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(180px, 1fr))', gap: 12, marginBottom: 32 }}>
            <StatCard icon={Landmark} label="Predios Totales" value={formatNum(resumen.total_predios)} />
            <StatCard icon={TrendingUp} label="Mediana Avalúo" value={formatCLP(resumen.mediana_avaluo)} sub="Valor central de todos los predios" />
            <StatCard icon={Building2} label="Avalúo Promedio" value={formatCLP(resumen.avg_avaluo)} />
            <StatCard icon={MapPin} label="Comunas" value="343" sub="16 regiones" />
          </div>

          {/* Charts row */}
          <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 16, marginBottom: 32 }}>
            {/* By region */}
            <div style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)', borderRadius: 'var(--radius-lg)', padding: 20 }}>
              <h3 style={{ fontSize: '0.85rem', color: '#000000', marginBottom: 16 }}>Predios por Región</h3>
              <div style={{ height: 300 }}>
                <ResponsiveContainer>
                  <BarChart data={resumen.por_region.slice(0, 10)} layout="vertical" margin={{ left: 10, right: 10 }}>
                    <XAxis type="number" tickFormatter={v => v >= 1e6 ? `${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `${(v/1e3).toFixed(0)}K` : v} tick={{ fontSize: 10, fill: '#71717a' }} />
                    <YAxis type="category" dataKey="region" width={100} tick={{ fontSize: 11, fill: '#a1a1aa' }} />
                    <Tooltip content={<CustomTooltip />} />
                    <Bar dataKey="total" radius={[0, 4, 4, 0]}>
                      {resumen.por_region.slice(0, 10).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                    </Bar>
                  </BarChart>
                </ResponsiveContainer>
              </div>
            </div>

            {/* By destino */}
            <div style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)', borderRadius: 'var(--radius-lg)', padding: 20 }}>
              <h3 style={{ fontSize: '0.85rem', color: '#000000', marginBottom: 16 }}>Distribución por Destino</h3>
              <div style={{ height: 300, display: 'flex', alignItems: 'center' }}>
                <ResponsiveContainer>
                  <PieChart>
                    <Pie data={resumen.por_destino.slice(0, 6)} dataKey="total" nameKey="nombre" cx="50%" cy="50%"
                      innerRadius={60} outerRadius={100} paddingAngle={2}
                      label={({ nombre, pct }) => `${nombre} ${pct}%`}
                      labelLine={{ stroke: '#3f3f46' }}
                    >
                      {resumen.por_destino.slice(0, 6).map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                    </Pie>
                    <Tooltip content={<CustomTooltip />} />
                  </PieChart>
                </ResponsiveContainer>
              </div>
            </div>
          </div>

          {/* Top comunas bar */}
          <div style={{ background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)', borderRadius: 'var(--radius-lg)', padding: 20, marginBottom: 32 }}>
            <h3 style={{ fontSize: '0.85rem', color: '#000000', marginBottom: 16 }}>Top 15 Comunas por Mediana de Avalúo</h3>
            <div style={{ height: 320 }}>
              <ResponsiveContainer>
                <BarChart data={[...comunas].sort((a, b) => (b.mediana_avaluo || 0) - (a.mediana_avaluo || 0)).slice(0, 15)} margin={{ bottom: 60 }}>
                  <XAxis dataKey="nombre" angle={-45} textAnchor="end" tick={{ fontSize: 10, fill: '#a1a1aa' }} interval={0} />
                  <YAxis tickFormatter={formatCLP} tick={{ fontSize: 10, fill: '#71717a' }} width={55} />
                  <Tooltip content={<CustomTooltip />} />
                  <Bar dataKey="mediana_avaluo" name="Mediana Avalúo" radius={[4, 4, 0, 0]}>
                    {[...Array(15)].map((_, i) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </>
      )}

      {/* Table */}
      <h3 style={{ marginBottom: 12 }}>Detalle por Comuna</h3>
      <div style={{ marginBottom: 16, position: 'relative', maxWidth: 360 }}>
        <Search size={16} style={{ position: 'absolute', left: 12, top: '50%', transform: 'translateY(-50%)', color: 'var(--color-text-muted)' }} />
        <input value={search} onChange={e => setSearch(e.target.value)}
          placeholder="Filtrar comunas..." style={{ width: '100%', paddingLeft: 36 }} />
      </div>

      <div style={{ overflowX: 'auto' }}>
        <table>
          <thead>
            <tr>
              <th style={{ cursor: 'pointer' }} onClick={() => toggleSort('nombre')}>Comuna<SortIcon k="nombre" /></th>
              <th>Región</th>
              <th style={{ textAlign: 'right', cursor: 'pointer' }} onClick={() => toggleSort('total_predios')}>Predios<SortIcon k="total_predios" /></th>
              <th style={{ textAlign: 'right', cursor: 'pointer' }} onClick={() => toggleSort('mediana_avaluo')}>Mediana Avalúo<SortIcon k="mediana_avaluo" /></th>
              <th style={{ textAlign: 'right', cursor: 'pointer' }} onClick={() => toggleSort('avg_avaluo')}>Promedio<SortIcon k="avg_avaluo" /></th>
              <th style={{ textAlign: 'right', cursor: 'pointer' }} onClick={() => toggleSort('avg_superficie')}>Sup. Prom.<SortIcon k="avg_superficie" /></th>
              <th style={{ textAlign: 'right' }}>Hab.</th>
              <th style={{ textAlign: 'right' }}>Com.</th>
              <th style={{ textAlign: 'right', cursor: 'pointer' }} onClick={() => toggleSort('avg_contribucion')}>Contrib.<SortIcon k="avg_contribucion" /></th>
            </tr>
          </thead>
          <tbody>
            {filtered.map(c => (
              <tr key={c.comuna}>
                <td style={{ fontWeight: 500, color: 'var(--color-text-primary)' }}>{c.nombre || c.comuna}</td>
                <td style={{ fontSize: '0.8rem' }}>{c.region}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{formatNum(c.total_predios)}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', color: '#000000', fontWeight: 500 }}>{formatCLP(c.mediana_avaluo)}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{formatCLP(c.avg_avaluo)}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{c.avg_superficie ? `${c.avg_superficie.toLocaleString('es-CL')} m²` : '—'}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>{formatNum(c.habitacional)}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>{formatNum(c.comercial)}</td>
                <td style={{ textAlign: 'right', fontVariantNumeric: 'tabular-nums' }}>{formatCLP(c.avg_contribucion)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
