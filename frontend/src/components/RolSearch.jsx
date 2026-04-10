import { useState, useEffect, useMemo } from 'react'
import { useNavigate } from 'react-router-dom'
import { Search, ChevronDown, Loader2 } from 'lucide-react'
import { getComunas } from '../services/api'

export default function RolSearch() {
  const navigate = useNavigate()
  const [comunas, setComunas] = useState([])
  const [loadingComunas, setLoadingComunas] = useState(true)
  const [region, setRegion] = useState('')
  const [comuna, setComuna] = useState('')
  const [manzana, setManzana] = useState('')
  const [predio, setPredio] = useState('')

  useEffect(() => {
    getComunas()
      .then(setComunas)
      .catch(() => {})
      .finally(() => setLoadingComunas(false))
  }, [])

  const regiones = useMemo(() => {
    const set = new Set(comunas.map(c => c.region))
    return [...set].sort((a, b) => a.localeCompare(b, 'es'))
  }, [comunas])

  const comunasFiltradas = useMemo(() => {
    if (!region) return []
    return comunas.filter(c => c.region === region).sort((a, b) => a.nombre.localeCompare(b.nombre, 'es'))
  }, [comunas, region])

  function handleRegionChange(e) {
    setRegion(e.target.value)
    setComuna('')
  }

  function handleSubmit(e) {
    e.preventDefault()
    if (!comuna || !manzana || !predio) return
    navigate(`/predio/${comuna}/${manzana}/${predio}`)
  }

  const canSubmit = comuna && manzana && predio

  return (
    <form onSubmit={handleSubmit} style={{ maxWidth: 600, margin: '0 auto' }}>
      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr', gap: 12, marginBottom: 12 }}>
        {/* Region */}
        <div style={{ position: 'relative' }}>
          <label style={labelStyle}>Región</label>
          <div style={{ position: 'relative' }}>
            <select
              value={region}
              onChange={handleRegionChange}
              disabled={loadingComunas}
              style={selectStyle}
            >
              <option value="">Selecciona región</option>
              {regiones.map(r => (
                <option key={r} value={r}>{r}</option>
              ))}
            </select>
            <ChevronDown size={14} style={chevronStyle} />
          </div>
        </div>

        {/* Comuna */}
        <div style={{ position: 'relative' }}>
          <label style={labelStyle}>Comuna</label>
          <div style={{ position: 'relative' }}>
            <select
              value={comuna}
              onChange={e => setComuna(e.target.value)}
              disabled={!region}
              style={selectStyle}
            >
              <option value="">{region ? 'Selecciona comuna' : 'Primero elige región'}</option>
              {comunasFiltradas.map(c => (
                <option key={c.codigo} value={c.codigo}>{c.nombre}</option>
              ))}
            </select>
            <ChevronDown size={14} style={chevronStyle} />
          </div>
        </div>
      </div>

      <div style={{ display: 'grid', gridTemplateColumns: '1fr 1fr auto', gap: 12, alignItems: 'end' }}>
        {/* Manzana */}
        <div>
          <label style={labelStyle}>Manzana</label>
          <input
            type="number"
            min="1"
            value={manzana}
            onChange={e => setManzana(e.target.value)}
            placeholder="Ej: 12"
            disabled={!comuna}
            style={inputStyle}
          />
        </div>

        {/* Predio */}
        <div>
          <label style={labelStyle}>Predio</label>
          <input
            type="number"
            min="1"
            value={predio}
            onChange={e => setPredio(e.target.value)}
            placeholder="Ej: 45"
            disabled={!comuna}
            style={inputStyle}
          />
        </div>

        {/* Submit */}
        <button
          type="submit"
          disabled={!canSubmit}
          className="btn-primary"
          style={{
            display: 'flex', alignItems: 'center', gap: 8,
            padding: '10px 20px', height: 42,
            opacity: canSubmit ? 1 : 0.4,
            cursor: canSubmit ? 'pointer' : 'not-allowed',
          }}
        >
          <Search size={16} />
          Buscar
        </button>
      </div>

      {comuna && (
        <div style={{
          marginTop: 12, fontSize: '0.8rem', color: 'var(--color-text-muted)',
          display: 'flex', alignItems: 'center', gap: 6,
        }}>
          Rol: <span style={{ color: '#000000', fontWeight: 600, fontVariantNumeric: 'tabular-nums' }}>
            {comuna}-{manzana || '??'}-{predio || '??'}
          </span>
        </div>
      )}
    </form>
  )
}

const labelStyle = {
  display: 'block',
  fontSize: '0.75rem',
  fontWeight: 600,
  color: 'var(--color-text-muted)',
  textTransform: 'uppercase',
  letterSpacing: '0.05em',
  marginBottom: 6,
}

const selectStyle = {
  width: '100%',
  appearance: 'none',
  paddingRight: 32,
  height: 42,
}

const inputStyle = {
  width: '100%',
  height: 42,
}

const chevronStyle = {
  position: 'absolute',
  right: 10,
  top: '50%',
  transform: 'translateY(-50%)',
  color: 'var(--color-text-muted)',
  pointerEvents: 'none',
}
