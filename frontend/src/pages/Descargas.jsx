import { useState, useEffect } from 'react'
import { Download, Check } from 'lucide-react'

const API = '/api'

function formatSize(mb) {
  if (mb >= 1000) return `${(mb / 1000).toFixed(1)} GB`
  return `${Math.round(mb)} MB`
}

function formatNum(v) {
  return v?.toLocaleString('es-CL') || '—'
}

const DICT = [
  { col: 'periodo', tipo: 'TEXT', desc: 'Identificador del periodo (ej: 2025_2)' },
  { col: 'anio', tipo: 'INT', desc: 'Ano del semestre' },
  { col: 'semestre', tipo: 'INT', desc: 'Semestre (1 o 2)' },
  { col: 'comuna', tipo: 'INT', desc: 'Codigo SII de la comuna' },
  { col: 'manzana', tipo: 'INT', desc: 'Numero de manzana' },
  { col: 'predio', tipo: 'INT', desc: 'Numero de predio dentro de la manzana' },
  { col: 'rc_direccion', tipo: 'TEXT', desc: 'Direccion del predio segun rol catastral' },
  { col: 'rc_serie', tipo: 'TEXT', desc: 'Serie del rol (A, B, C...)' },
  { col: 'rc_ind_aseo', tipo: 'TEXT', desc: 'Indicador de aseo municipal' },
  { col: 'rc_cuota_trimestral', tipo: 'BIGINT', desc: 'Cuota trimestral de contribuciones (CLP)' },
  { col: 'rc_avaluo_total', tipo: 'BIGINT', desc: 'Avaluo fiscal total del predio (CLP)' },
  { col: 'rc_avaluo_exento', tipo: 'BIGINT', desc: 'Monto exento de contribuciones (CLP)' },
  { col: 'rc_anio_term_exencion', tipo: 'INT', desc: 'Ano de termino de la exencion' },
  { col: 'rc_cod_ubicacion', tipo: 'TEXT', desc: 'U = Urbano, R = Rural' },
  { col: 'rc_cod_destino', tipo: 'TEXT', desc: 'Codigo de destino: H=Habitacional, C=Comercial, A=Agricola, etc.' },
  { col: 'dc_direccion', tipo: 'TEXT', desc: 'Direccion segun detalle catastral' },
  { col: 'dc_avaluo_fiscal', tipo: 'BIGINT', desc: 'Avaluo fiscal segun detalle catastral (CLP)' },
  { col: 'dc_contribucion_semestral', tipo: 'BIGINT', desc: 'Contribucion semestral (CLP)' },
  { col: 'dc_cod_destino', tipo: 'TEXT', desc: 'Destino segun detalle catastral' },
  { col: 'dc_avaluo_exento', tipo: 'BIGINT', desc: 'Avaluo exento segun detalle catastral (CLP)' },
  { col: 'dc_sup_terreno', tipo: 'DECIMAL', desc: 'Superficie del terreno (m2)' },
  { col: 'dc_cod_ubicacion', tipo: 'TEXT', desc: 'Ubicacion segun detalle catastral' },
  { col: 'dc_bc1_comuna', tipo: 'INT', desc: 'Comuna del bien comun 1 (edificio)' },
  { col: 'dc_bc1_manzana', tipo: 'INT', desc: 'Manzana del bien comun 1' },
  { col: 'dc_bc1_predio', tipo: 'INT', desc: 'Predio del bien comun 1' },
  { col: 'dc_bc2_comuna', tipo: 'INT', desc: 'Comuna del bien comun 2' },
  { col: 'dc_bc2_manzana', tipo: 'INT', desc: 'Manzana del bien comun 2' },
  { col: 'dc_bc2_predio', tipo: 'INT', desc: 'Predio del bien comun 2' },
  { col: 'dc_padre_comuna', tipo: 'INT', desc: 'Comuna del predio padre (subdivision)' },
  { col: 'dc_padre_manzana', tipo: 'INT', desc: 'Manzana del predio padre' },
  { col: 'dc_padre_predio', tipo: 'INT', desc: 'Predio padre' },
  { col: 'n_lineas_construccion', tipo: 'INT', desc: 'Numero de lineas de construccion' },
  { col: 'sup_construida_total', tipo: 'DECIMAL', desc: 'Superficie total construida (m2)' },
  { col: 'anio_construccion_min', tipo: 'INT', desc: 'Ano de construccion mas antiguo' },
  { col: 'anio_construccion_max', tipo: 'INT', desc: 'Ano de construccion mas reciente' },
  { col: 'materiales', tipo: 'TEXT', desc: 'Codigos de materiales: A=Acero, B=Hormigon, C=Albanileria, E=Madera, K=Prefabricado' },
  { col: 'calidades', tipo: 'TEXT', desc: 'Calidad constructiva: 1=Superior, 2=Media-Sup, 3=Media, 4=Media-Inf, 5=Inferior' },
  { col: 'pisos_max', tipo: 'INT', desc: 'Numero maximo de pisos' },
  { col: 'serie', tipo: 'TEXT', desc: 'Serie del predio' },
]

export default function Descargas() {
  const [data, setData] = useState(null)
  const [downloading, setDownloading] = useState(null)
  const [showDict, setShowDict] = useState(false)

  const [loadError, setLoadError] = useState(false)

  useEffect(() => {
    let retries = 0
    function load() {
      fetch(`${API}/descargas`)
        .then(r => { if (!r.ok) throw new Error(); return r.json() })
        .then(setData)
        .catch(() => {
          if (retries < 3) { retries++; setTimeout(load, 3000) }
          else setLoadError(true)
        })
    }
    load()
  }, [])

  async function handleDownload(id) {
    setDownloading(id)
    try {
      const res = await fetch(`${API}/descargas/${id}/url`)
      const { url } = await res.json()
      window.open(url, '_blank')
    } catch {}
    setTimeout(() => setDownloading(null), 2000)
  }

  if (!data) return (
    <div className="container" style={{ padding: '32px 24px' }}>
      <h2 style={{ marginBottom: 8 }}>Descarga Masiva</h2>
      {loadError ? (
        <p style={{ color: '#999', fontSize: '0.9rem', marginTop: 16 }}>
          El servidor esta iniciando. <button onClick={() => window.location.reload()} style={{ textDecoration: 'underline', color: '#000' }}>Reintentar</button>
        </p>
      ) : (
        <div style={{ height: 2, background: 'var(--color-border)', borderRadius: 2, overflow: 'hidden', marginTop: 16 }}>
          <div style={{ height: '100%', width: '40%', background: '#000', borderRadius: 2, animation: 'progress-slide 1.2s ease-in-out infinite' }} />
        </div>
      )}
      <style>{`@keyframes progress-slide { 0% { margin-left: 0%; width: 30%; } 50% { margin-left: 35%; width: 50%; } 100% { margin-left: 100%; width: 30%; } }`}</style>
    </div>
  )

  return (
    <div className="container" style={{ paddingTop: 32, paddingBottom: 64 }}>
      <h2 style={{ marginBottom: 8 }}>Descarga Masiva</h2>
      <p style={{ color: 'var(--color-text-muted)', fontSize: '0.9rem', marginBottom: 28 }}>
        Datos catastrales publicos del SII. Cada archivo contiene todos los predios de Chile para un semestre.
        {' '}<strong style={{ color: 'var(--color-text-secondary)' }}>{data.total_archivos} archivos · {formatNum(data.total_registros)} registros · {data.total_size_gb} GB · {data.columnas} columnas</strong>
      </p>

      {/* File list — table style like Tienda */}
      <div style={{
        background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-lg)', overflow: 'hidden', marginBottom: 32, maxWidth: 700, margin: '0 auto 32px',
      }}>
        {/* Header */}
        <div style={{
          display: 'grid', gridTemplateColumns: '90px 180px 120px 80px 90px',
          padding: '12px 16px', borderBottom: '1px solid var(--color-border)',
          fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase',
          letterSpacing: '0.05em', color: 'var(--color-text-muted)',
        }}>
          <div>Periodo</div>
          <div>Semestre</div>
          <div style={{ textAlign: 'right' }}>Registros</div>
          <div style={{ textAlign: 'right' }}>Tamano</div>
          <div style={{ textAlign: 'right' }}></div>
        </div>

        {/* Rows */}
        {data.archivos.sort((a, b) => b.anio - a.anio || b.semestre - a.semestre).map(sem => (
          <div key={sem.id} style={{
            display: 'grid', gridTemplateColumns: '90px 180px 120px 80px 90px',
            padding: '10px 16px', alignItems: 'center',
            borderBottom: '1px solid rgba(39,39,42,0.5)',
            fontSize: '0.85rem', transition: 'background 150ms',
          }}
            onMouseOver={e => e.currentTarget.style.background = 'var(--color-bg-secondary)'}
            onMouseOut={e => e.currentTarget.style.background = 'transparent'}
          >
            <div style={{ fontWeight: 600, color: 'var(--color-accent-primary)', fontVariantNumeric: 'tabular-nums' }}>
              {sem.periodo}
            </div>
            <div style={{ color: 'var(--color-text-secondary)' }}>
              {sem.anio} — Semestre {sem.semestre}
            </div>
            <div style={{ textAlign: 'right', color: 'var(--color-text-secondary)', fontVariantNumeric: 'tabular-nums' }}>
              {formatNum(sem.registros)}
            </div>
            <div style={{ textAlign: 'right', color: 'var(--color-text-muted)', fontVariantNumeric: 'tabular-nums' }}>
              {formatSize(sem.size_mb)}
            </div>
            <div style={{ textAlign: 'right' }}>
              <button
                onClick={() => handleDownload(sem.id)}
                disabled={downloading === sem.id}
                className="btn-primary"
                style={{ padding: '6px 14px', fontSize: '0.8rem', display: 'inline-flex', alignItems: 'center', gap: 4 }}
              >
                {downloading === sem.id
                  ? <><Check size={14} /> Listo</>
                  : <><Download size={14} /> CSV</>
                }
              </button>
            </div>
          </div>
        ))}
      </div>

      {/* Dictionary toggle */}
      <div style={{
        background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-lg)', overflow: 'hidden',
      }}>
        <button onClick={() => setShowDict(!showDict)} style={{
          display: 'flex', alignItems: 'center', gap: 8, width: '100%',
          padding: '14px 16px', color: '#000000', fontSize: '0.9rem', fontWeight: 600,
        }}>
          Diccionario de Variables ({data.columnas} columnas)
          <span style={{ marginLeft: 'auto', color: 'var(--color-text-muted)', fontSize: '0.8rem' }}>{showDict ? '▲' : '▼'}</span>
        </button>

        {showDict && (
          <div style={{ borderTop: '1px solid var(--color-border)' }}>
            {/* Dict header */}
            <div style={{
              display: 'grid', gridTemplateColumns: '200px 80px 1fr',
              padding: '10px 16px', borderBottom: '1px solid var(--color-border)',
              fontSize: '0.7rem', fontWeight: 600, textTransform: 'uppercase',
              letterSpacing: '0.05em', color: 'var(--color-text-muted)',
            }}>
              <div>Variable</div>
              <div>Tipo</div>
              <div>Descripcion</div>
            </div>

            {/* Dict rows */}
            {DICT.map(({ col, tipo, desc }) => (
              <div key={col} style={{
                display: 'grid', gridTemplateColumns: '200px 80px 1fr',
                padding: '8px 16px', borderBottom: '1px solid rgba(39,39,42,0.3)',
                fontSize: '0.8rem',
              }}>
                <code style={{ color: 'var(--color-accent-primary)', fontSize: '0.78rem' }}>{col}</code>
                <span style={{ color: 'var(--color-text-muted)', fontSize: '0.7rem' }}>{tipo}</span>
                <span style={{ color: 'var(--color-text-secondary)' }}>{desc}</span>
              </div>
            ))}
          </div>
        )}
      </div>

      {/* Disclaimer */}
      <div style={{
        marginTop: 24, padding: '12px 16px',
        background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-md)', fontSize: '0.75rem', color: 'var(--color-text-muted)',
      }}>
        Los links de descarga expiran en 15 minutos. Datos publicos del Servicio de Impuestos Internos (SII) de Chile, procesados por <a href="https://tremen.tech" target="_blank" rel="noopener">Tremen SpA</a>.
      </div>
    </div>
  )
}
