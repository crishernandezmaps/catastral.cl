import { useState } from 'react'
import { Share2, Check, Loader2, ExternalLink, Copy, Link2 } from 'lucide-react'
import { confirmShare, trustShare } from '../services/api'
import { useAuth } from '../context/AuthContext'

const SHARE_URL = 'https://catastral.cl'
const LINKEDIN_SHARE = `https://www.linkedin.com/sharing/share-offsite/?url=${encodeURIComponent(SHARE_URL)}`

const SUGGESTED_POST = `Encontre algo que quiero compartir: Catastral.cl

Es una plataforma para explorar los 9.4 millones de predios de Chile, con poligonos vectorizados, avaluos del SII y series historicas desde 2018. Esta disponible gratis para cualquiera que la use.

Me parecio util para quienes trabajan en analisis territorial, inmobiliario o urbano. Tambien para entender que hay detras de una direccion o rol catastral.

Esta detras @crishernandezco con Tremen (@tremen-tech), vale la pena conocer lo que hacen.

https://catastral.cl`

function extractUsername(url) {
  const clean = url.split('?')[0]
  const m = clean.match(/linkedin\.com\/posts\/([^_/\s]+)_/i)
  return m ? m[1].toLowerCase() : null
}

export default function ShareToDownload({ onSuccess }) {
  const { markAsShared } = useAuth()
  const [step, setStep] = useState('idle')
  const [postUrl, setPostUrl] = useState('')
  const [error, setError] = useState(null)
  const [copied, setCopied] = useState(false)

  const handleShare = () => {
    window.open(LINKEDIN_SHARE, '_blank', 'width=600,height=500')
    setStep('sharing')
  }

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(SUGGESTED_POST)
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    } catch {}
  }

  const handleVerify = async () => {
    setError(null)
    setStep('verifying')
    try {
      const detectedUsername = extractUsername(postUrl)
      const res = await confirmShare(postUrl.trim(), detectedUsername)
      markAsShared(res.username || 'shared')
      setStep('done')
      onSuccess?.()
    } catch (err) {
      setError(err.data?.detail || err.message || 'Error al verificar')
      setStep('sharing')
    }
  }

  if (step === 'done') {
    return (
      <div style={{
        padding: '24px 32px', textAlign: 'center',
        border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-lg)',
      }}>
        <Check size={28} color="#000" style={{ marginBottom: 8 }} />
        <div style={{ fontSize: '1.05rem', fontWeight: 500 }}>Acceso desbloqueado</div>
        <div style={{ fontSize: '0.85rem', color: '#777', marginTop: 4 }}>
          Ahora puedes descargar todas las capas. Gracias por compartir.
        </div>
      </div>
    )
  }

  return (
    <div style={{
      padding: '28px 32px',
      border: '1px solid var(--color-border)',
      borderRadius: 'var(--radius-lg)',
    }}>
      <div style={{ display: 'flex', alignItems: 'center', gap: 12, marginBottom: 16 }}>
        <Share2 size={24} color="#000" />
        <h3 style={{ fontSize: '1.15rem', margin: 0 }}>Comparte en LinkedIn para desbloquear</h3>
      </div>

      <p style={{ fontSize: '0.85rem', color: '#777', marginBottom: 16 }}>
        Comparte Catastral.cl en LinkedIn y obtiene acceso permanente a todas las capas de datos
        (CSV y GeoPackage) de las 342 comunas de Chile. Sin registro, sin email.
      </p>

          <div style={{
            background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
            borderRadius: 'var(--radius-md)', padding: 14, marginBottom: 16,
          }}>
            <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 8 }}>
              <span style={{ fontSize: '0.75rem', fontWeight: 600, color: '#999', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
                Texto sugerido
              </span>
              <button onClick={handleCopy} style={{
                background: 'transparent', border: '1px solid var(--color-border)',
                borderRadius: 'var(--radius-full)', padding: '4px 12px',
                fontSize: '0.75rem', color: copied ? '#000' : '#999',
                cursor: 'pointer', display: 'inline-flex', alignItems: 'center', gap: 4,
              }}>
                {copied ? <><Check size={12} /> Copiado</> : <><Copy size={12} /> Copiar</>}
              </button>
            </div>
            <pre style={{
              fontSize: '0.78rem', color: '#555', lineHeight: 1.5,
              whiteSpace: 'pre-wrap', fontFamily: 'inherit', margin: 0,
            }}>{SUGGESTED_POST}</pre>
            <div style={{ fontSize: '0.7rem', color: '#999', marginTop: 10, fontStyle: 'italic' }}>
              Convierte @crishernandezco y @tremen-tech en menciones reales en LinkedIn escribiendo @ y seleccionando.
            </div>
          </div>

          {step === 'idle' && (
            <button onClick={handleShare} className="btn-primary" style={{
              width: '100%', justifyContent: 'center',
              display: 'inline-flex', alignItems: 'center', gap: 8,
            }}>
              <ExternalLink size={16} /> Abrir LinkedIn y publicar
            </button>
          )}

          {(step === 'sharing' || step === 'verifying') && (
            <div>
              <label style={{ display: 'block', fontSize: '0.8rem', fontWeight: 500, marginBottom: 8, color: '#555' }}>
                <Link2 size={14} style={{ display: 'inline', verticalAlign: 'middle', marginRight: 4 }} />
                Pega el URL de tu post de LinkedIn:
              </label>
              <input
                type="text"
                placeholder="https://www.linkedin.com/posts/..."
                value={postUrl}
                onChange={e => { setPostUrl(e.target.value); setError(null) }}
                disabled={step === 'verifying'}
                style={{
                  width: '100%', padding: '10px 14px', marginBottom: 10,
                  fontFamily: 'monospace',
                }}
              />

              {error && <div style={{ fontSize: '0.8rem', color: 'var(--color-error)', marginBottom: 10 }}>{error}</div>}
              <div style={{ display: 'flex', gap: 8 }}>
                <button
                  onClick={handleVerify}
                  disabled={!postUrl.trim() || step === 'verifying'}
                  className="btn-primary"
                  style={{
                    flex: 1, justifyContent: 'center',
                    display: 'inline-flex', alignItems: 'center', gap: 8,
                    opacity: (!postUrl.trim() || step === 'verifying') ? 0.4 : 1,
                    cursor: (postUrl.trim() && step !== 'verifying') ? 'pointer' : 'not-allowed',
                  }}
                >
                  {step === 'verifying' ? (
                    <><Loader2 size={16} style={{ animation: 'spin 1s linear infinite' }} /> Verificando...</>
                  ) : (
                    <><Check size={16} /> Verificar y desbloquear</>
                  )}
                </button>
                <button
                  onClick={() => { setStep('idle'); setPostUrl(''); setError(null) }}
                  disabled={step === 'verifying'}
                  className="btn-ghost"
                >Cancelar</button>
              </div>
            </div>
          )}

          <div style={{ marginTop: 16, paddingTop: 14, borderTop: '1px solid var(--color-border)', display: 'flex', alignItems: 'center', gap: 8 }}>
            <input
              type="checkbox"
              id="already-shared"
              onChange={async (e) => { if (e.target.checked) { try { await trustShare() } catch {} markAsShared('returning'); onSuccess?.() } }}
              style={{ accentColor: '#000', width: 16, height: 16, cursor: 'pointer' }}
            />
            <label htmlFor="already-shared" style={{ fontSize: '0.82rem', color: '#999', cursor: 'pointer' }}>
              Ya comparti en LinkedIn antes
            </label>
          </div>
    </div>
  )
}
