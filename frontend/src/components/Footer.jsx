export default function Footer() {
  return (
    <footer style={{
      padding: '24px 0', textAlign: 'center',
      borderTop: '1px solid var(--color-border)',
      fontSize: '0.75rem', color: 'var(--color-text-muted)',
    }}>
      <div className="container">
        Datos publicos del Servicio de Impuestos Internos (SII) de Chile.
        <br />
        {new Date().getFullYear()} — <a href="https://tremen.tech" target="_blank" rel="noopener">Tremen SpA</a>
      </div>
    </footer>
  )
}
