import { useMemo } from 'react'
import { GeoJSON, Tooltip } from 'react-leaflet'
import { useNavigate } from 'react-router-dom'

const DESTINO_COLORS = {
  H: '#000000', C: '#f59e0b', O: '#3b82f6', I: '#a855f7',
  L: '#71717a', Z: '#52525b', E: '#22c55e', S: '#ec4899',
}

export default function PropertyPolygons({ data, interactive = true }) {
  const navigate = useNavigate()

  // Force re-render when data changes by using feature count as key
  const key = useMemo(() => {
    if (!data || !data.features) return 'empty'
    return data.features.length + '-' + (data.features[0]?.properties?.v || '')
  }, [data])

  if (!data || !data.features || data.features.length === 0) return null

  const style = (feature) => {
    const destino = feature.properties?.txt_cod_destino || ''
    const color = DESTINO_COLORS[destino] || '#000000'
    return {
      color,
      weight: 2,
      fillColor: color,
      fillOpacity: 0.35,
      opacity: 0.9,
    }
  }

  const onEachFeature = (feature, layer) => {
    const props = feature.properties || {}
    const v = props.v || ''
    const parts = v.split('|')
    const rol = parts.length === 3 ? `${parts[0]}-${parts[1]}-${parts[2]}` : v
    const dir = props.txt_direccion || ''

    layer.bindTooltip(
      `<strong>${rol}</strong>${dir ? '<br/>' + dir : ''}`,
      { direction: 'top', className: 'predio-tooltip', sticky: true }
    )

    if (interactive && parts.length === 3) {
      layer.on('click', () => navigate(`/predio/${parts[0]}/${parts[1]}/${parts[2]}`))
      layer.setStyle({ cursor: 'pointer' })
    }
  }

  return (
    <GeoJSON
      key={key}
      data={data}
      style={style}
      onEachFeature={onEachFeature}
    />
  )
}
