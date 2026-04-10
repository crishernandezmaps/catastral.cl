import { useEffect, useRef, useState } from 'react'

const COLORS = {
  departamento: '#000000',
  oficina: '#3b82f6',
  local: '#f59e0b',
  casa: '#22c55e',
  bodega: '#71717a',
  estacionamiento: '#52525b',
  otro: '#a855f7',
  highlight: '#ff3366',
}

const DESTINO_LABEL = {
  departamento: 'Deptos',
  oficina: 'Oficinas',
  local: 'Locales',
  casa: 'Casas',
  bodega: 'Bodegas',
  estacionamiento: 'Estac.',
  otro: 'Otros',
}

// Isometric projection helpers
function isoX(x, y) { return (x - y) * Math.cos(Math.PI / 6) }
function isoY(x, y, z) { return (x + y) * Math.sin(Math.PI / 6) - z }

export default function Building3D({ data, currentPredio }) {
  const canvasRef = useRef(null)
  const [hoveredFloor, setHoveredFloor] = useState(null)
  const [tooltip, setTooltip] = useState(null)

  useEffect(() => {
    if (!data?.es_edificio || !canvasRef.current) return
    draw()
  }, [data, hoveredFloor])

  function draw() {
    const canvas = canvasRef.current
    const ctx = canvas.getContext('2d')
    const dpr = window.devicePixelRatio || 1
    const W = canvas.clientWidth
    const H = canvas.clientHeight
    canvas.width = W * dpr
    canvas.height = H * dpr
    ctx.scale(dpr, dpr)
    ctx.clearRect(0, 0, W, H)

    const { pisos, pisos_max, footprint_m2, unidades } = data
    // En Chile los pisos empiezan desde 1 (no hay piso 0).
    // Reasignar piso 0 → piso 1: merge con piso 1 si existe, o renumerar.
    const rawFloors = (pisos || []).filter(p => p.piso >= 0)
    const floors = []
    const floor0 = rawFloors.find(p => p.piso === 0)
    for (const f of rawFloors) {
      if (f.piso === 0) continue
      if (f.piso === 1 && floor0) {
        floors.push({ ...f, unidades: f.unidades + floor0.unidades, m2: f.m2 + floor0.m2, tipos: { ...floor0.tipos, ...Object.fromEntries(Object.entries(f.tipos || {}).map(([k, v]) => [k, (floor0.tipos?.[k] || 0) + v])) } })
      } else {
        floors.push(f)
      }
    }
    if (floor0 && !rawFloors.find(p => p.piso === 1)) {
      floors.push({ ...floor0, piso: 1 })
    }
    floors.sort((a, b) => a.piso - b.piso)

    // Building dimensions (scaled)
    const maxSide = Math.min(W * 0.22, 150)
    const bw = maxSide
    const bd = maxSide * 0.6
    const floorH = Math.min(40, (H * 0.6) / Math.max(pisos_max, 1))

    // Center the building visually
    const isoRight = isoX(bw, 0)
    const isoLeft = isoX(0, bd)
    const isoCenterX = (isoLeft + isoRight) / 2
    const cx = W / 2 - isoCenterX
    const baseY = H - 80

    // Find which floor the current predio is on (piso 0 → piso 1 en Chile)
    const currentUnit = unidades?.find(u => u.predio === currentPredio)
    const currentFloor = currentUnit ? Math.max(currentUnit.piso, 1) : -999

    function drawFace(path, color) {
      ctx.beginPath()
      path.forEach(([px, py], i) => i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py))
      ctx.closePath()
      ctx.fillStyle = color
      ctx.fill()
      ctx.strokeStyle = 'rgba(0,0,0,0.3)'
      ctx.lineWidth = 0.5
      ctx.stroke()
    }

    function drawTexture(path, material, face) {
      ctx.save()
      ctx.beginPath()
      path.forEach(([px, py], i) => i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py))
      ctx.closePath()
      ctx.clip()

      const bounds = path.reduce((b, [px, py]) => ({
        minX: Math.min(b.minX, px), maxX: Math.max(b.maxX, px),
        minY: Math.min(b.minY, py), maxY: Math.max(b.maxY, py),
      }), { minX: Infinity, maxX: -Infinity, minY: Infinity, maxY: -Infinity })

      ctx.strokeStyle = 'rgba(255,255,255,0.07)'
      ctx.lineWidth = 0.5

      if (material === 'B') {
        const step = face === 'top' ? 6 : 5
        for (let py = bounds.minY; py < bounds.maxY; py += step) {
          ctx.beginPath()
          ctx.moveTo(bounds.minX, py)
          ctx.lineTo(bounds.maxX, py)
          ctx.stroke()
        }
      } else if (material === 'C' || material === 'K') {
        const bh = 5, bw2 = 10
        let row = 0
        for (let py = bounds.minY; py < bounds.maxY; py += bh) {
          const offset = (row % 2) * (bw2 / 2)
          for (let px = bounds.minX + offset; px < bounds.maxX; px += bw2) {
            ctx.strokeRect(px, py, bw2, bh)
          }
          row++
        }
      } else if (material === 'E') {
        // Madera: vertical wood grain
        const step = 8
        ctx.strokeStyle = 'rgba(255,255,255,0.05)'
        for (let px = bounds.minX; px < bounds.maxX; px += step) {
          ctx.beginPath()
          ctx.moveTo(px, bounds.minY)
          ctx.lineTo(px + 2, bounds.maxY)
          ctx.stroke()
        }
      } else if (material === 'A' || material === 'G') {
        const step = 8
        ctx.strokeStyle = 'rgba(255,255,255,0.06)'
        for (let i = bounds.minX - (bounds.maxY - bounds.minY); i < bounds.maxX; i += step) {
          ctx.beginPath()
          ctx.moveTo(i, bounds.maxY)
          ctx.lineTo(i + (bounds.maxY - bounds.minY), bounds.minY)
          ctx.stroke()
        }
      } else if (material === 'F') {
        // Adobe: rough horizontal + vertical blocks
        const bh = 8, bw2 = 14
        let row = 0
        ctx.strokeStyle = 'rgba(255,255,255,0.04)'
        for (let py = bounds.minY; py < bounds.maxY; py += bh) {
          const offset = (row % 2) * (bw2 / 2)
          for (let px = bounds.minX + offset; px < bounds.maxX; px += bw2) {
            ctx.strokeRect(px, py, bw2, bh)
          }
          row++
        }
      }

      ctx.restore()
    }

    function drawBlock(x, y, w, d, h, color, alpha = 1, isHighlight = false, material = '') {
      const top = [
        [isoX(0, 0), isoY(0, 0, h)],
        [isoX(w, 0), isoY(w, 0, h)],
        [isoX(w, d), isoY(w, d, h)],
        [isoX(0, d), isoY(0, d, h)],
      ]
      const right = [
        [isoX(w, 0), isoY(w, 0, h)],
        [isoX(w, d), isoY(w, d, h)],
        [isoX(w, d), isoY(w, d, 0)],
        [isoX(w, 0), isoY(w, 0, 0)],
      ]
      const left = [
        [isoX(0, 0), isoY(0, 0, h)],
        [isoX(0, d), isoY(0, d, h)],
        [isoX(0, d), isoY(0, d, 0)],
        [isoX(0, 0), isoY(0, 0, 0)],
      ]

      ctx.save()
      ctx.translate(x, y)
      ctx.globalAlpha = alpha

      drawFace(left, darken(color, 0.4))
      if (material) drawTexture(left, material, 'left')

      drawFace(right, darken(color, 0.2))
      if (material) drawTexture(right, material, 'right')

      drawFace(top, color)
      if (material) drawTexture(top, material, 'top')

      if (isHighlight) {
        ctx.shadowColor = '#ff3366'
        ctx.shadowBlur = 15
        ctx.beginPath()
        top.forEach(([px, py], i) => i === 0 ? ctx.moveTo(px, py) : ctx.lineTo(px, py))
        ctx.closePath()
        ctx.strokeStyle = '#ff3366'
        ctx.lineWidth = 2
        ctx.stroke()
        ctx.shadowBlur = 0
      }

      ctx.restore()
    }

    // Draw floors
    floors.forEach((floor, i) => {
      const fz = i * floorH
      const bx = cx
      const by = baseY - fz

      const tipos = floor.tipos || {}
      const mainType = Object.entries(tipos).sort((a, b) => b[1] - a[1])[0]?.[0] || 'departamento'
      let color = COLORS[mainType] || COLORS.departamento

      const isHovered = hoveredFloor === floor.piso
      const isCurrent = floor.piso === currentFloor
      const alpha = hoveredFloor !== null && !isHovered && !isCurrent ? 0.4 : 1

      if (isCurrent) color = COLORS.highlight

      const floorUnits = (data.unidades || []).filter(u => Math.max(u.piso, 1) === floor.piso)
      const mats = floorUnits.map(u => u.material).filter(Boolean)
      const floorMaterial = mats.length > 0
        ? [...new Set(mats)].sort((a, b) => mats.filter(m => m === b).length - mats.filter(m => m === a).length)[0]
        : data.material_dominante || ''

      drawBlock(bx, by, bw, bd, floorH - 2, color, alpha, isCurrent, floorMaterial)

      // Floor label
      ctx.save()
      ctx.globalAlpha = alpha
      ctx.fillStyle = isCurrent ? '#ff3366' : '#a1a1aa'
      ctx.font = `${isCurrent ? 'bold ' : ''}11px DM Sans, system-ui`
      ctx.textAlign = 'right'
      const labelY = by + isoY(0, 0, (floorH - 2) / 2) + 4
      ctx.fillText(`P${floor.piso}`, cx + isoX(-8, 0), labelY)

      ctx.textAlign = 'left'
      ctx.fillStyle = isCurrent ? '#ff3366' : '#71717a'
      ctx.font = '10px DM Sans, system-ui'
      const rightX = cx + isoX(bw + 8, 0)
      ctx.fillText(`${floor.unidades} un. · ${Math.round(floor.m2)} m²`, rightX, labelY)
      ctx.restore()
    })

    // Building label on top
    const topY = baseY - (floors.length) * floorH
    const buildingUnits = (data.unidades || []).length
    ctx.fillStyle = '#000000'
    ctx.font = 'bold 13px DM Sans, system-ui'
    ctx.textAlign = 'center'
    ctx.fillText(`${buildingUnits} unidades · ${pisos_max} pisos`, cx + isoX(bw / 2, bd / 2), topY - 20)
    ctx.fillStyle = '#71717a'
    ctx.font = '11px DM Sans, system-ui'
    const buildingM2 = (data.unidades || []).reduce((s, u) => s + u.m2, 0)
    ctx.fillText(`${Math.round(buildingM2).toLocaleString('es-CL')} m² construidos`, cx + isoX(bw / 2, bd / 2), topY - 5)

    // Current unit indicator
    if (currentUnit) {
      ctx.fillStyle = '#ff3366'
      ctx.font = 'bold 11px DM Sans, system-ui'
      ctx.textAlign = 'center'
      const currentFloorIdx = floors.findIndex(f => f.piso === currentFloor)
      const arrowY = baseY - (currentFloorIdx >= 0 ? currentFloorIdx : 0) * floorH + isoY(0, 0, (floorH - 2) / 2) + 4
      ctx.fillText('◄ Tu unidad', cx + isoX(bw + 8, 0) + 110, arrowY)
    }
  }

  function handleMouseMove(e) {
    if (!data?.es_edificio) return
    const rect = canvasRef.current.getBoundingClientRect()
    const y = e.clientY - rect.top
    const floorH = Math.min(40, (rect.height * 0.6) / Math.max(data.pisos_max, 1))
    const baseY = rect.height - 80
    const floors = (data.pisos || []).filter(p => p.piso >= 0)

    let found = null
    floors.forEach((floor, i) => {
      const fy = baseY - i * floorH - floorH
      if (y >= fy && y < fy + floorH) found = floor
    })

    if (found) {
      setHoveredFloor(found.piso)
      setTooltip(found)
    } else {
      setHoveredFloor(null)
      setTooltip(null)
    }
  }

  if (!data?.es_edificio) return null

  const tiposPresentes = new Set(data.unidades?.map(u => u.tipo) || [])
  const materialPresente = data.material_dominante
  const MATERIAL_NAMES = { A: 'Acero', B: 'Hormigón Armado', C: 'Albañilería', E: 'Madera', F: 'Adobe', G: 'Perfiles Metálicos', K: 'Prefabricado' }
  const anexoResumen = data.anexo_resumen || {}
  const hasAnexos = Object.keys(anexoResumen).length > 0

  return (
    <div style={{
      background: 'var(--color-bg-secondary)', border: '1px solid var(--color-border)',
      borderRadius: 'var(--radius-lg)', padding: 20, marginBottom: 16,
    }}>
      <h3 style={{ fontSize: '0.9rem', display: 'flex', alignItems: 'center', gap: 8, marginBottom: 4, color: '#000000' }}>
        <Building3DIcon /> Visualización del Edificio
        <span style={{ fontSize: '0.7rem', color: 'var(--color-text-muted)', fontWeight: 400, marginLeft: 'auto',
          padding: '2px 8px', background: 'rgba(186,251,0,0.1)', borderRadius: 4 }}>EXPERIMENTAL</span>
      </h3>
      <p style={{ fontSize: '0.75rem', color: 'var(--color-text-muted)', marginBottom: 16 }}>
        Volumen estimado basado en pisos, unidades y m² construidos del SII
      </p>

      <canvas
        ref={canvasRef}
        onMouseMove={handleMouseMove}
        onMouseLeave={() => { setHoveredFloor(null); setTooltip(null) }}
        style={{ width: '100%', height: Math.max(380, data.pisos_max * 55 + 200), cursor: 'crosshair' }}
      />

      {/* Tooltip */}
      {tooltip && (
        <div style={{
          marginTop: 8, padding: '8px 14px',
          background: 'var(--color-bg-tertiary)', border: '1px solid var(--color-border)',
          borderRadius: 'var(--radius-md)', fontSize: '0.8rem',
        }}>
          <strong style={{ color: '#000000' }}>Piso {tooltip.piso}</strong>
          <span style={{ color: 'var(--color-text-muted)', marginLeft: 12 }}>
            {tooltip.unidades} unidades · {Math.round(tooltip.m2)} m²
          </span>
          {tooltip.tipos && Object.entries(tooltip.tipos).map(([tipo, count]) => (
            <span key={tipo} style={{ marginLeft: 12, color: COLORS[tipo] || '#a1a1aa' }}>
              {count} {DESTINO_LABEL[tipo] || tipo}
            </span>
          ))}
        </div>
      )}

      {/* Anexos: estacionamientos y bodegas */}
      {hasAnexos && (
        <div style={{
          marginTop: 16, display: 'grid',
          gridTemplateColumns: `repeat(${Object.keys(anexoResumen).length}, 1fr)`,
          gap: 12,
        }}>
          {Object.entries(anexoResumen).map(([tipo, info]) => (
            <div key={tipo} style={{
              padding: '12px 16px',
              background: 'var(--color-bg-tertiary)',
              border: '1px solid var(--color-border)',
              borderRadius: 'var(--radius-md)',
            }}>
              <div style={{
                display: 'flex', alignItems: 'center', gap: 6, marginBottom: 8,
                fontSize: '0.75rem', fontWeight: 600, textTransform: 'uppercase',
                letterSpacing: '0.05em', color: COLORS[tipo] || '#a1a1aa',
              }}>
                {tipo === 'estacionamiento' ? <ParkingIcon /> : <BoxIcon />}
                {DESTINO_LABEL[tipo] || tipo}
              </div>
              <div style={{ display: 'flex', gap: 16, fontSize: '0.8rem' }}>
                <div>
                  <div style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: '1.1rem', fontVariantNumeric: 'tabular-nums' }}>
                    {info.count}
                  </div>
                  <div style={{ color: 'var(--color-text-muted)', fontSize: '0.7rem' }}>unidades</div>
                </div>
                <div>
                  <div style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: '1.1rem', fontVariantNumeric: 'tabular-nums' }}>
                    {Math.round(info.m2).toLocaleString('es-CL')} m²
                  </div>
                  <div style={{ color: 'var(--color-text-muted)', fontSize: '0.7rem' }}>superficie</div>
                </div>
                <div>
                  <div style={{ color: 'var(--color-text-primary)', fontWeight: 600, fontSize: '1.1rem', fontVariantNumeric: 'tabular-nums' }}>
                    ${(info.avg_avaluo / 1e6).toFixed(1)}M
                  </div>
                  <div style={{ color: 'var(--color-text-muted)', fontSize: '0.7rem' }}>avalúo prom.</div>
                </div>
              </div>
            </div>
          ))}
        </div>
      )}

      {/* Legend */}
      <div style={{ display: 'flex', gap: 16, marginTop: 12, flexWrap: 'wrap' }}>
        {[...tiposPresentes].map(tipo => (
          <div key={tipo} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.75rem', color: 'var(--color-text-muted)' }}>
            <div style={{ width: 10, height: 10, borderRadius: 2, background: COLORS[tipo] || '#71717a' }} />
            {DESTINO_LABEL[tipo] || tipo}
          </div>
        ))}
        {hasAnexos && Object.keys(anexoResumen).map(tipo => (
          <div key={`annex-${tipo}`} style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.75rem', color: 'var(--color-text-muted)' }}>
            <div style={{ width: 10, height: 10, borderRadius: 2, background: COLORS[tipo] || '#71717a' }} />
            {DESTINO_LABEL[tipo] || tipo} (subterráneo)
          </div>
        ))}
        <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.75rem', color: '#ff3366' }}>
          <div style={{ width: 10, height: 10, borderRadius: 2, background: '#ff3366' }} />
          Tu unidad
        </div>
        {materialPresente && (
          <div style={{ display: 'flex', alignItems: 'center', gap: 6, fontSize: '0.75rem', color: 'var(--color-text-muted)', marginLeft: 'auto' }}>
            Materialidad: <strong style={{ color: 'var(--color-text-primary)' }}>{MATERIAL_NAMES[materialPresente] || materialPresente}</strong>
          </div>
        )}
      </div>
    </div>
  )
}

function Building3DIcon() {
  return (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#000000" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="4" y="2" width="16" height="20" rx="2" /><path d="M9 22v-4h6v4" /><path d="M8 6h.01M16 6h.01M12 6h.01M8 10h.01M16 10h.01M12 10h.01M8 14h.01M16 14h.01M12 14h.01" />
    </svg>
  )
}

function ParkingIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <rect x="3" y="3" width="18" height="18" rx="2" /><path d="M9 17V7h4a3 3 0 0 1 0 6H9" />
    </svg>
  )
}

function BoxIcon() {
  return (
    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 8a2 2 0 0 0-1-1.73l-7-4a2 2 0 0 0-2 0l-7 4A2 2 0 0 0 3 8v8a2 2 0 0 0 1 1.73l7 4a2 2 0 0 0 2 0l7-4A2 2 0 0 0 21 16Z" />
      <path d="m3.3 7 8.7 5 8.7-5M12 22V12" />
    </svg>
  )
}

function darken(hex, amount) {
  const r = parseInt(hex.slice(1, 3), 16)
  const g = parseInt(hex.slice(3, 5), 16)
  const b = parseInt(hex.slice(5, 7), 16)
  return `rgb(${Math.round(r * (1 - amount))},${Math.round(g * (1 - amount))},${Math.round(b * (1 - amount))})`
}
