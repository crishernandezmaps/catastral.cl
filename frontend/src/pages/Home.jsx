import { Link } from 'react-router-dom'
import { ShoppingCart, Download, ArrowRight } from 'lucide-react'
import LoginModal from '../components/LoginModal'

const FEATURES = [
  { to: '/tienda', icon: ShoppingCart, title: 'Tienda', desc: 'Descarga datos por comuna en CSV y GeoPackage. 343 comunas con poligonos vectorizados y datos enriquecidos.' },
  { to: '/descargas', icon: Download, title: 'Descargas Historicas', desc: '16 semestres de datos catastrales tabulares gratuitos (2018-2025). 22.8 GB en CSV.' },
]

export default function Home() {
  return (
    <>
      <LoginModal />

      {/* Hero */}
      <section style={{
        padding: '80px 0 60px',
        textAlign: 'center',
        borderBottom: '1px solid var(--color-border)',
      }}>
        <div className="container" style={{ maxWidth: 720 }}>
          <p style={{ fontSize: '0.8rem', color: '#999', textTransform: 'uppercase', letterSpacing: '0.1em', marginBottom: 16 }}>
            Datos Catastrales de Chile
          </p>
          <h1 style={{ fontSize: '3.5rem', marginBottom: 20 }}>
            Catastral.cl
          </h1>
          <p style={{ fontSize: '1.05rem', color: '#555', lineHeight: 1.7, marginBottom: 36 }}>
            Datos prediales del SII estructurados, georreferenciados y listos para analisis.
            Poligonos vectorizados, avaluos fiscales, superficies y destinos para los 9.5 millones de predios de Chile.
          </p>
          <div style={{ display: 'flex', gap: 12, justifyContent: 'center' }}>
            <Link to="/tienda" className="btn-primary" style={{ display: 'inline-flex', alignItems: 'center', gap: 8 }}>
              Ver catalogo <ArrowRight size={16} />
            </Link>
            <Link to="/metodologia" className="btn-ghost">
              Metodologia
            </Link>
          </div>
        </div>
      </section>

      <div className="container" style={{ padding: '64px 24px' }}>
        {/* Stats row */}
        <div style={{ display: 'flex', justifyContent: 'center', marginBottom: 64 }}>
          <div style={{ display: 'inline-flex', gap: 0 }} className="stats-grid">
            {[
              { value: '9.5M', label: 'Predios' },
              { value: '343', label: 'Comunas' },
              { value: '112', label: 'Variables' },
              { value: '9.1M', label: 'Poligonos' },
            ].map(s => (
              <div key={s.label} className="stat-card" style={{ textAlign: 'center', padding: '8px 28px' }}>
                <div className="stat-value">{s.value}</div>
                <div className="stat-label">{s.label}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Feature grid */}
        <div style={{ display: 'grid', gridTemplateColumns: 'repeat(2, 1fr)', gap: 20, maxWidth: 800, margin: '0 auto' }} className="features-grid">
          {FEATURES.map(({ to, icon: Icon, title, desc }) => (
            <Link key={to} to={to} style={{
              textDecoration: 'none',
              border: '1px solid var(--color-border)',
              borderRadius: 'var(--radius-lg)',
              padding: 'var(--space-8)',
              transition: 'all var(--transition-normal)',
              background: 'transparent',
            }}>
              <Icon size={22} color="#000" style={{ marginBottom: 14 }} />
              <h3 style={{ color: '#000', marginBottom: 8, fontSize: '1.15rem' }}>{title}</h3>
              <p style={{ color: '#777', fontSize: '0.85rem', lineHeight: 1.6 }}>{desc}</p>
            </Link>
          ))}
        </div>

        {/* Context block */}
        <div style={{ maxWidth: 700, margin: '64px auto 0', textAlign: 'center' }}>
          <p style={{ fontSize: '0.85rem', color: '#999', lineHeight: 1.8 }}>
            Los datos catastrales del SII son informacion publica (Ley 20.285), pero sus formatos de publicacion
            imponen barreras tecnicas que impiden el analisis masivo. Este proyecto estructura, georreferencia y
            distribuye estos datos para que cualquier actor — brokers, investigadores, municipios, periodistas —
            pueda usarlos sin barreras. Documentado en la tesis
            {' '}<em>"Datos publicos como catalizador de la industria"</em>{' '}
            (Universidad de Chile / MIT Sloan, 2026).
          </p>
        </div>
      </div>
    </>
  )
}
