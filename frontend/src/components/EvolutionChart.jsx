import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer, CartesianGrid } from 'recharts'

function formatCLP(value) {
  if (value >= 1e9) return `$${(value / 1e9).toFixed(1)}B`
  if (value >= 1e6) return `$${(value / 1e6).toFixed(0)}M`
  if (value >= 1e3) return `$${(value / 1e3).toFixed(0)}K`
  return `$${value}`
}

function CustomTooltip({ active, payload, label }) {
  if (!active || !payload?.length) return null
  return (
    <div style={{
      background: '#1f1f1f', border: '1px solid #27272a',
      borderRadius: 8, padding: '10px 14px', fontSize: '0.8rem',
    }}>
      <div style={{ fontWeight: 600, marginBottom: 4 }}>{label}</div>
      {payload.map((p, i) => (
        <div key={i} style={{ color: p.color, display: 'flex', gap: 8, justifyContent: 'space-between' }}>
          <span>{p.name}</span>
          <span style={{ fontWeight: 600 }}>{formatCLP(p.value)}</span>
        </div>
      ))}
    </div>
  )
}

export default function EvolutionChart({ data }) {
  if (!data?.length) return <p style={{ color: 'var(--color-text-muted)', fontSize: '0.85rem' }}>Sin datos históricos</p>

  return (
    <div style={{ width: '100%', height: 320 }}>
      <ResponsiveContainer>
        <LineChart data={data} margin={{ top: 10, right: 10, bottom: 0, left: 10 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#27272a" />
          <XAxis dataKey="periodo" tick={{ fontSize: 11, fill: '#71717a' }} />
          <YAxis tickFormatter={formatCLP} tick={{ fontSize: 11, fill: '#71717a' }} width={60} />
          <Tooltip content={<CustomTooltip />} />
          <Line type="monotone" dataKey="rc_avaluo_total" name="Avalúo Total"
            stroke="#000000" strokeWidth={2} dot={{ r: 3, fill: '#000000' }} />
          <Line type="monotone" dataKey="dc_avaluo_fiscal" name="Avalúo Fiscal"
            stroke="#3b82f6" strokeWidth={2} dot={{ r: 3, fill: '#3b82f6' }} />
        </LineChart>
      </ResponsiveContainer>
    </div>
  )
}
