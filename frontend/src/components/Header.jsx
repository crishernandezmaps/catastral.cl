import { Link, useLocation } from 'react-router-dom'
import { Download, ShoppingCart, BookOpen, User, LogOut, Shield } from 'lucide-react'
import { useAuth } from '../context/AuthContext'

const NAV_PUBLIC = [
  { to: '/tienda', label: 'Tienda', icon: ShoppingCart },
  { to: '/descargas', label: 'Descargas', icon: Download },
  { to: '/metodologia', label: 'Metodologia', icon: BookOpen },
]

export default function Header() {
  const { pathname } = useLocation()
  const { user, logout, isAdmin } = useAuth()

  return (
    <header className="glass-panel" style={{
      position: 'sticky', top: 0, zIndex: 100,
    }}>
      <div className="container" style={{
        display: 'flex', alignItems: 'center', justifyContent: 'space-between',
        height: 56,
      }}>
        <Link to="/" style={{ display: 'flex', alignItems: 'center', gap: 8, textDecoration: 'none' }}>
          <img src="/tremen.svg" alt="Catastral.cl" width={22} height={22} />
          <span style={{ fontWeight: 600, fontSize: '0.95rem', color: '#000', letterSpacing: '0.04em' }}>
            CATASTRAL.CL
          </span>
        </Link>

        <nav style={{ display: 'flex', gap: 2, alignItems: 'center' }}>
          {NAV_PUBLIC.map(({ to, label, icon: Icon }) => {
            const active = pathname === to || (to !== '/' && pathname.startsWith(to))
            return (
              <Link key={to} to={to} style={{
                display: 'flex', alignItems: 'center', gap: 6,
                padding: '6px 14px', borderRadius: 9999,
                fontSize: '0.82rem', fontWeight: 500,
                color: active ? '#000' : '#999',
                background: active ? '#f0f0f0' : 'transparent',
                textDecoration: 'none',
                transition: 'all 150ms',
              }}>
                <Icon size={15} />
                {label}
              </Link>
            )
          })}

          {user && (
            <>
              <div style={{ width: 1, height: 20, background: '#e0e0e0', margin: '0 6px' }} />
              <div style={{ display: 'flex', alignItems: 'center', gap: 2 }}>
                {isAdmin && (
                  <Link to="/admin" style={{
                    display: 'flex', alignItems: 'center', gap: 4,
                    padding: '6px 10px', borderRadius: 9999,
                    fontSize: '0.8rem', color: '#000',
                    textDecoration: 'none',
                  }}>
                    <Shield size={14} />
                  </Link>
                )}
                <span style={{ fontSize: '0.8rem', color: '#999', padding: '0 6px' }}>
                  {user.email.split('@')[0]}
                </span>
                <button onClick={logout} style={{
                  display: 'flex', alignItems: 'center',
                  padding: '6px 8px', borderRadius: 9999,
                  color: '#999', fontSize: '0.8rem',
                }}>
                  <LogOut size={14} />
                </button>
              </div>
            </>
          )}
        </nav>
      </div>
    </header>
  )
}
