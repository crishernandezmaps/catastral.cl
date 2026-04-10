import { useState, useEffect } from 'react'
import { useParams, Link } from 'react-router-dom'
import { ArrowLeft, MapPin, Home, Ruler, DollarSign, Calendar, Layers, Building2, TrendingUp } from 'lucide-react'
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet'
import L from 'leaflet'
import { getPredio, getEvolucion, getPropertyPolygon } from '../services/api'
import PropertyPolygons from '../components/PropertyPolygons'
import EvolutionChart from '../components/EvolutionChart'
import Building3D from '../components/Building3D'
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'
import 'leaflet/dist/leaflet.css'

delete L.Icon.Default.prototype._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon-2x.png',
  iconUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-icon.png',
  shadowUrl: 'https://unpkg.com/leaflet@1.9.4/dist/images/marker-shadow.png',
})

const API = '/api'

const DESTINO_LABELS = {
  H: 'Habitacional', C: 'Comercial', I: 'Industrial',
  O: 'Oficina', E: 'Educación', S: 'Salud',
  A: 'Agrícola', F: 'Forestal', M: 'Minería',
}

const MATERIAL_LABELS = {
  A: 'Acero', B: 'Hormigón Armado', C: 'Albañilería',
  E: 'Madera', F: 'Adobe', G: 'Perfiles Metálicos',
  K: 'Prefabricado',
  GA: 'Galpón Acero', GB: 'Galpón Hormigón', GC: 'Galpón Albañilería',
  GE: 'Galpón Madera', GL: 'Galpón Madera Laminada', GF: 'Galpón Adobe',
  OA: 'Obra Acero', OB: 'Obra Hormigón', OE: 'Obra Madera',
  SA: 'Silo Acero', SB: 'Silo Hormigón', EA: 'Estanque Acero', EB: 'Estanque Hormigón',
  M: 'Marquesina', P: 'Pavimento', W: 'Piscina',
  TA: 'Techumbre Acero', TE: 'Techumbre Madera', TL: 'Techumbre Madera Laminada',
}

const CALIDAD_LABELS = {
  '1': 'Superior', '2': 'Media Superior', '3': 'Media', '4': 'Media Inferior', '5': 'Inferior',
}

function formatCLP(v) {
  if (!v && v !== 0) return '—'
  return '$' + Number(v).toLocaleString('es-CL')
}

function formatNum(v) {
  if (!v && v !== 0) return '—'
  return Number(v).toLocaleString('es-CL')
}

function Field({ label, value, accent }) {
  return (
    <div style={{ marginBottom: 14 }}>
      <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 2 }}>{label}</div>
      <div style={{ fontSize: '0.9rem', color: accent ? '#000000' : 'var(--color-text-primary)', fontWeight: accent ? 600 : 400 }}>{value ?? '—'}</div>
    </div>
  )
}

function Section({ title, icon: Icon, children, cols }) {
  return (
    <div style={{
      background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
      borderRadius: 'var(--radius-lg)', padding: 20, marginBottom: 16,
    }}>
      <h3 style={{ fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16, color: '#000000' }}>
        <Icon size={16} /> {title}
      </h3>
      <div style={{ display: 'grid', gridTemplateColumns: `repeat(auto-fill, minmax(${cols || 180}px, 1fr))`, gap: '0 24px' }}>
        {children}
      </div>
    </div>
  )
}

function ChartTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{ background: '#1f1f1f', border: '1px solid #27272a', borderRadius: 8, padding: '10px 14px', fontSize: '0.8rem' }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color }}>{p.name}: {formatCLP(p.value)}</div>
      ))}
    </div>
  )
}

function FitBounds({ geojson }) {
  const map = useMap()
  useEffect(() => {
    if (!geojson?.features?.length) return
    try {
      const layer = L.geoJSON(geojson)
      const bounds = layer.getBounds()
      if (bounds.isValid()) map.fitBounds(bounds, { padding: [30, 30], maxZoom: 18 })
    } catch {}
  }, [geojson, map])
  return null
}

export default function DetallePredio() {
  const { comuna, manzana, predio } = useParams()
  const [data, setData] = useState(null)
  const [evolucion, setEvolucion] = useState(null)
  const [edificio, setEdificio] = useState(null)
  const [edificio3d, setEdificio3d] = useState(null)
  const [polygon, setPolygon] = useState(null)
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    setLoading(true)
    Promise.all([
      getPredio(comuna, manzana, predio),
      getEvolucion(comuna, manzana, predio),
      fetch(`${API}/predios/${comuna}/${manzana}/${predio}/edificio`).then(r => r.json()),
      fetch(`${API}/predios/${comuna}/${manzana}/${predio}/edificio3d`).then(r => r.json()),
      getPropertyPolygon(comuna, manzana, predio).catch(() => null),
    ]).then(([p, e, b, b3d, poly]) => {
      setData(p)
      setEvolucion(e.evolucion)
      setEdificio(b)
      setEdificio3d(b3d)
      setPolygon(poly?.features?.length ? poly : null)
    }).catch(() => {}).finally(() => setLoading(false))
  }, [comuna, manzana, predio])

  if (loading) return <div className="container" style={{ padding: 80, textAlign: 'center', color: 'var(--color-text-muted)' }}>Cargando...</div>
  if (!data || data.error) return <div className="container" style={{ padding: 80, textAlign: 'center' }}>Predio no encontrado</div>

  const d = data
  const variacion = evolucion?.length >= 2
    ? ((evolucion[evolucion.length - 1].rc_avaluo_total - evolucion[0].rc_avaluo_total) / evolucion[0].rc_avaluo_total * 100).toFixed(1)
    : null

  // Decode materials
  const materiales = (d.materiales || '').split('|').map(m => MATERIAL_LABELS[m] || m).join(', ')
  const calidades = (d.calidades || '').split('|').map(c => CALIDAD_LABELS[c] || c).join(', ')

  return (
    <div className="container" style={{ paddingTop: 32, paddingBottom: 48, maxWidth: 960 }}>
      <Link to="/" style={{ display: 'inline-flex', alignItems: 'center', gap: 6, fontSize: '0.85rem', color: 'var(--color-text-muted)', marginBottom: 20 }}>
        <ArrowLeft size={16} /> Volver
      </Link>

      {/* Header */}
      <div style={{ marginBottom: 24 }}>
        <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', textTransform: 'uppercase', letterSpacing: '0.05em', marginBottom: 4 }}>Rol Catastral</div>
        <h1 style={{ fontSize: '2rem', marginBottom: 4 }}>
          <span style={{ color: '#000000' }}>{d.comuna}-{d.manzana}-{d.predio}</span>
        </h1>
        <div style={{ fontSize: '1rem', color: 'var(--color-text-secondary)' }}>
          {d.rc_direccion || 'Sin dirección'} · {d.comuna_nombre || ''}, {d.region || ''}
        </div>
      </div>

      {/* Mapa de ubicación */}
      {d.lat && d.lon && (
        <div style={{
          marginBottom: 20, borderRadius: 'var(--radius-lg)', overflow: 'hidden',
          border: '1px solid var(--color-border)', height: 220,
        }}>
          <MapContainer center={[d.lat, d.lon]} zoom={17} style={{ height: '100%', width: '100%' }}
            scrollWheelZoom={false} dragging={true} zoomControl={true}>
            <TileLayer
              attribution='Tiles &copy; Esri &mdash; Source: Esri, Maxar, Earthstar Geographics'
              url="https://server.arcgisonline.com/ArcGIS/rest/services/World_Imagery/MapServer/tile/{z}/{y}/{x}"
            />
            {polygon && <PropertyPolygons data={polygon} interactive={false} />}
            {polygon && <FitBounds geojson={polygon} />}
            <Marker position={[d.lat, d.lon]}>
              <Popup>
                <div style={{ fontSize: '0.8rem' }}>
                  <strong>{d.comuna}-{d.manzana}-{d.predio}</strong><br />
                  {d.rc_direccion}
                </div>
              </Popup>
            </Marker>
          </MapContainer>
        </div>
      )}

      {/* Summary cards */}
      <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(150px, 1fr))', gap: 12, marginBottom: 20 }}>
        <div className="stat-card" style={{ padding: 16 }}>
          <div className="stat-label">Avalúo Total</div>
          <div className="stat-value" style={{ fontSize: '1.3rem' }}>{formatCLP(d.rc_avaluo_total)}</div>
        </div>
        <div className="stat-card" style={{ padding: 16 }}>
          <div className="stat-label">Contribución Sem.</div>
          <div className="stat-value" style={{ fontSize: '1.3rem' }}>{formatCLP(d.dc_contribucion_semestral)}</div>
        </div>
        <div className="stat-card" style={{ padding: 16 }}>
          <div className="stat-label">Superficie</div>
          <div className="stat-value" style={{ fontSize: '1.3rem' }}>{d.superficie ? `${formatNum(d.superficie)} m²` : '—'}</div>
        </div>
        {variacion && (
          <div className="stat-card" style={{ padding: 16 }}>
            <div className="stat-label">Variación 2018→2025</div>
            <div className="stat-value" style={{ fontSize: '1.3rem', color: variacion > 0 ? '#22c55e' : '#ef4444' }}>
              {variacion > 0 ? '+' : ''}{variacion}%
            </div>
          </div>
        )}
      </div>

      {/* Main info sections */}
      <Section title="Identificación" icon={MapPin}>
        <Field label="Comuna" value={`${d.comuna_nombre} (${d.comuna})`} />
        <Field label="Manzana" value={d.manzana} />
        <Field label="Predio" value={d.predio} />
        <Field label="Periodo" value={d.periodo} />
        <Field label="Ubicación" value={d.rc_cod_ubicacion === 'U' ? 'Urbano' : d.rc_cod_ubicacion === 'R' ? 'Rural' : d.rc_cod_ubicacion} />
        <Field label="Destino" value={DESTINO_LABELS[d.dc_cod_destino] || d.dc_cod_destino} accent />
        <Field label="Serie" value={d.rc_serie === 'N' ? 'No Serie' : d.rc_serie} />
        <Field label="Aseo" value={d.rc_ind_aseo === 'A' ? 'Sí' : d.rc_ind_aseo === 'N' ? 'No' : d.rc_ind_aseo} />
      </Section>

      <Section title="Avalúos y Contribuciones" icon={DollarSign}>
        <Field label="Avalúo Total" value={formatCLP(d.rc_avaluo_total)} accent />
        <Field label="Avalúo Exento" value={formatCLP(d.rc_avaluo_exento)} />
        <Field label="Avalúo Fiscal" value={formatCLP(d.dc_avaluo_fiscal)} />
        <Field label="Contribución Semestral" value={formatCLP(d.dc_contribucion_semestral)} accent />
        <Field label="Cuota Trimestral" value={formatCLP(d.rc_cuota_trimestral)} />
        <Field label="Año Término Exención" value={d.rc_anio_term_exencion && d.rc_anio_term_exencion > 0 ? d.rc_anio_term_exencion : 'Sin exención'} />
      </Section>

      <Section title="Superficie" icon={Ruler}>
        <Field label="Superficie Efectiva" value={d.superficie ? `${formatNum(d.superficie)} m²` : '—'} accent />
        <Field label="Superficie Terreno" value={d.dc_sup_terreno ? `${formatNum(d.dc_sup_terreno)} m²` : 'N/A (unidad en edificio)'} />
        <Field label="Superficie Construida" value={d.sup_construida_total ? `${formatNum(d.sup_construida_total)} m²` : '—'} />
      </Section>

      <Section title="Construcción" icon={Home}>
        <Field label="Material" value={materiales || '—'} />
        <Field label="Calidad" value={calidades || '—'} />
        <Field label="Líneas de Construcción" value={d.n_lineas_construccion} />
        <Field label="Año Construcción" value={
          d.anio_construccion_min === d.anio_construccion_max
            ? (d.anio_construccion_min || '—')
            : `${d.anio_construccion_min} — ${d.anio_construccion_max}`
        } />
        <Field label="Pisos Máx" value={d.pisos_max} />
      </Section>

      {/* Building context */}
      {edificio?.es_edificio && (
        <div style={{
          background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
          borderRadius: 'var(--radius-lg)', padding: 20, marginBottom: 16,
        }}>
          <h3 style={{ fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16, color: '#000000' }}>
            <Building2 size={16} /> Edificio ({edificio.unidades} unidades)
          </h3>
          <p style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)', marginBottom: 16 }}>
            Este predio es parte de un edificio con bien común {edificio.bien_comun}
          </p>
          <div style={{ display: 'grid', gridTemplateColumns: 'repeat(auto-fit, minmax(140px, 1fr))', gap: 12, marginBottom: 16 }}>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Unidades</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700, color: '#000000' }}>{edificio.unidades}</div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)' }}>{edificio.habitacional} hab · {edificio.comercial} com</div>
            </div>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>M² Total Edificio</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{formatNum(edificio.m2_total)} m²</div>
            </div>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Avalúo Promedio</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{formatCLP(edificio.avg_avaluo)}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Rango M²</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{formatNum(edificio.min_m2)} — {formatNum(edificio.max_m2)}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Rango Avalúo</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 600 }}>{formatCLP(edificio.min_avaluo)}</div>
              <div style={{ fontSize: '0.9rem', fontWeight: 600 }}>{formatCLP(edificio.max_avaluo)}</div>
            </div>
            <div>
              <div style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', textTransform: 'uppercase' }}>Contrib. Promedio</div>
              <div style={{ fontSize: '1.2rem', fontWeight: 700 }}>{formatCLP(edificio.avg_contrib)}</div>
            </div>
          </div>

          {edificio.muestra?.length > 0 && (
            <>
              <div style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginBottom: 8 }}>Otras unidades del edificio:</div>
              <div style={{ display: 'flex', flexWrap: 'wrap', gap: 6 }}>
                {edificio.muestra.map(u => (
                  <Link key={u.predio} to={`/predio/${comuna}/${manzana}/${u.predio}`}
                    style={{
                      fontSize: '0.75rem', padding: '4px 10px',
                      background: u.predio === parseInt(predio) ? 'rgba(186,251,0,0.15)' : 'var(--color-bg-tertiary)',
                      border: '1px solid var(--color-border)', borderRadius: 'var(--radius-sm)',
                      color: u.predio === parseInt(predio) ? '#000000' : 'var(--color-text-secondary)',
                      textDecoration: 'none',
                    }}>
                    {u.direccion?.replace(/.*\d{3,}\s*/, '') || `Unidad ${u.predio}`} · {u.m2}m² · {formatCLP(u.avaluo)}
                  </Link>
                ))}
              </div>
            </>
          )}
        </div>
      )}

      {/* Relations */}
      {(d.dc_bc1_comuna || d.dc_padre_comuna) && !edificio?.es_edificio && (
        <Section title="Relaciones" icon={Layers}>
          {d.dc_bc1_comuna ? <Field label="Bien Común 1" value={`${d.dc_bc1_comuna}-${d.dc_bc1_manzana}-${d.dc_bc1_predio}`} /> : null}
          {d.dc_bc2_comuna ? <Field label="Bien Común 2" value={`${d.dc_bc2_comuna}-${d.dc_bc2_manzana}-${d.dc_bc2_predio}`} /> : null}
          {d.dc_padre_comuna ? <Field label="Predio Padre" value={`${d.dc_padre_comuna}-${d.dc_padre_manzana}-${d.dc_padre_predio}`} /> : null}
        </Section>
      )}

      {/* Evolution charts */}
      <div style={{
        background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-lg)', padding: 20, marginBottom: 16,
      }}>
        <h3 style={{ fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16, color: '#000000' }}>
          <TrendingUp size={16} /> Evolución de Avalúo (2018 — 2025)
          {variacion && <span style={{ fontSize: '0.75rem', color: variacion > 0 ? '#22c55e' : '#ef4444', marginLeft: 'auto' }}>
            {variacion > 0 ? '+' : ''}{variacion}%
          </span>}
        </h3>
        <EvolutionChart data={evolucion} />
      </div>

      {evolucion?.length > 0 && (
        <div style={{
          background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
          borderRadius: 'var(--radius-lg)', padding: 20,
        }}>
          <h3 style={{ fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 8, marginBottom: 16, color: '#3b82f6' }}>
            <Calendar size={16} /> Evolución de Contribuciones
          </h3>
          <div style={{ width: '100%', height: 250 }}>
            <ResponsiveContainer>
              <LineChart data={evolucion} margin={{ top: 10, right: 10, bottom: 0, left: 10 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
                <XAxis dataKey="periodo" tick={{ fontSize: 11, fill: '#71717a' }} />
                <YAxis tickFormatter={v => v >= 1e6 ? `$${(v/1e6).toFixed(0)}M` : v >= 1e3 ? `$${(v/1e3).toFixed(0)}K` : `$${v}`}
                  tick={{ fontSize: 11, fill: '#71717a' }} width={55} />
                <Tooltip content={<ChartTooltip />} />
                <Line type="monotone" dataKey="dc_contribucion_semestral" name="Contribución Sem."
                  stroke="#f59e0b" strokeWidth={2} dot={{ r: 3, fill: '#f59e0b' }} />
                <Line type="monotone" dataKey="rc_cuota_trimestral" name="Cuota Trim."
                  stroke="#8b5cf6" strokeWidth={2} dot={{ r: 3, fill: '#8b5cf6' }} />
              </LineChart>
            </ResponsiveContainer>
          </div>
        </div>
      )}

      {/* 3D Building — at the end */}
      {edificio3d?.es_edificio && (
        <Building3D data={edificio3d} currentPredio={parseInt(predio)} />
      )}
    </div>
  )
}
