import { useState } from 'react'
import { X } from 'lucide-react'
import { useAuth } from '../context/AuthContext'
import { requestOtpCode, verifyOtpCode } from '../services/api'

export default function LoginModal() {
  const { loginModalOpen, closeLoginModal, login } = useAuth()
  const [step, setStep] = useState('email') // email | code
  const [email, setEmail] = useState('')
  const [code, setCode] = useState('')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')

  if (!loginModalOpen) return null

  const handleRequestCode = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      await requestOtpCode(email)
      setStep('code')
    } catch (err) {
      setError(err.message || 'Error al enviar codigo')
    }
    setLoading(false)
  }

  const handleVerifyCode = async (e) => {
    e.preventDefault()
    setLoading(true)
    setError('')
    try {
      const data = await verifyOtpCode(email, code)
      login(data.token, data.user.email, data.user.role)
      closeLoginModal()
      setStep('email')
      setEmail('')
      setCode('')
    } catch (err) {
      setError(err.message || 'Codigo invalido')
    }
    setLoading(false)
  }

  return (
    <div style={{
      position: 'fixed', inset: 0, zIndex: 10000,
      display: 'flex', alignItems: 'center', justifyContent: 'center',
      background: 'rgba(0,0,0,0.7)', backdropFilter: 'blur(4px)',
    }} onClick={closeLoginModal}>
      <div style={{
        background: 'var(--color-bg-secondary)',
        border: '1px solid var(--color-border)',
        borderRadius: 'var(--radius-lg)',
        padding: 'var(--space-8)',
        width: 380, maxWidth: '90vw',
      }} onClick={e => e.stopPropagation()}>
        <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: 'var(--space-6)' }}>
          <h3>Ingresar</h3>
          <button onClick={closeLoginModal} style={{ color: 'var(--color-text-muted)' }}>
            <X size={18} />
          </button>
        </div>

        {error && (
          <div style={{
            background: 'rgba(239,68,68,0.1)', color: 'var(--color-error)',
            padding: 'var(--space-3)', borderRadius: 'var(--radius-md)',
            fontSize: '0.85rem', marginBottom: 'var(--space-4)',
          }}>
            {error}
          </div>
        )}

        {step === 'email' ? (
          <form onSubmit={handleRequestCode}>
            <label style={{ fontSize: '0.85rem', color: 'var(--color-text-secondary)', display: 'block', marginBottom: 'var(--space-2)' }}>
              Email
            </label>
            <input
              type="email" value={email} onChange={e => setEmail(e.target.value)}
              placeholder="tu@email.com" required autoFocus
              style={{ width: '100%', marginBottom: 'var(--space-4)', padding: '10px 12px' }}
            />
            <button type="submit" className="btn-primary" disabled={loading}
              style={{ width: '100%', padding: '10px', fontSize: '0.9rem' }}>
              {loading ? 'Enviando...' : 'Enviar codigo'}
            </button>
          </form>
        ) : (
          <form onSubmit={handleVerifyCode}>
            <p style={{ fontSize: '0.85rem', color: 'var(--color-text-secondary)', marginBottom: 'var(--space-4)' }}>
              Ingresa el codigo de 6 digitos enviado a <strong style={{ color: '#fff' }}>{email}</strong>
            </p>
            <input
              type="text" value={code} onChange={e => setCode(e.target.value)}
              placeholder="123456" required autoFocus maxLength={6}
              style={{
                width: '100%', marginBottom: 'var(--space-4)', padding: '10px 12px',
                textAlign: 'center', fontSize: '1.5rem', letterSpacing: '8px', fontWeight: 700,
              }}
            />
            <button type="submit" className="btn-primary" disabled={loading}
              style={{ width: '100%', padding: '10px', fontSize: '0.9rem' }}>
              {loading ? 'Verificando...' : 'Verificar'}
            </button>
            <button type="button" onClick={() => { setStep('email'); setError('') }}
              style={{ width: '100%', marginTop: 'var(--space-3)', fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
              Cambiar email
            </button>
          </form>
        )}
      </div>
    </div>
  )
}
