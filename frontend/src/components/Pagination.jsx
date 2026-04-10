import { ChevronLeft, ChevronRight } from 'lucide-react'

export default function Pagination({ page, pages, total, onPage }) {
  if (pages <= 1) return null

  return (
    <div style={{
      display: 'flex', alignItems: 'center', justifyContent: 'space-between',
      padding: 'var(--space-4) 0',
    }}>
      <span style={{ fontSize: '0.8rem', color: 'var(--color-text-muted)' }}>
        {total.toLocaleString('es-CL')} resultados — Pagina {page} de {pages.toLocaleString('es-CL')}
      </span>
      <div style={{ display: 'flex', gap: 4 }}>
        <button className="btn-ghost" disabled={page <= 1}
          onClick={() => onPage(page - 1)}
          style={{ opacity: page <= 1 ? 0.3 : 1 }}>
          <ChevronLeft size={16} />
        </button>
        <button className="btn-ghost" disabled={page >= pages}
          onClick={() => onPage(page + 1)}
          style={{ opacity: page >= pages ? 0.3 : 1 }}>
          <ChevronRight size={16} />
        </button>
      </div>
    </div>
  )
}
