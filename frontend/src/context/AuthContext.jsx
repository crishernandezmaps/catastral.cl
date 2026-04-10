import { createContext, useContext, useState, useEffect, useCallback } from 'react'
import { apiLogout, getShareStatus } from '../services/api'

const AuthContext = createContext(null)

export const AuthProvider = ({ children }) => {
  const [user, setUser] = useState(() => {
    const email = localStorage.getItem('tremen_email')
    const role = localStorage.getItem('tremen_role')
    return email ? { email, role: role || 'user' } : null
  })

  const [loginModalOpen, setLoginModalOpen] = useState(false)
  const [hasShared, setHasShared] = useState(false)
  const [linkedinUsername, setLinkedinUsername] = useState(null)

  // Check share status on mount (cookie-based, no login required)
  useEffect(() => {
    getShareStatus()
      .then(r => { setHasShared(r.shared); setLinkedinUsername(r.username || null) })
      .catch(() => setHasShared(false))
  }, [])

  const markAsShared = useCallback((username) => {
    setHasShared(true)
    if (username) setLinkedinUsername(username)
  }, [])

  const login = useCallback((token, email, role) => {
    localStorage.setItem('tremen_email', email)
    localStorage.setItem('tremen_role', role || 'user')
    localStorage.removeItem('tremen_token')
    setUser({ email, role: role || 'user' })
  }, [])

  const logout = useCallback(async () => {
    try {
      await apiLogout()
    } catch {
      // Continue logout even if API call fails
    }
    localStorage.removeItem('tremen_email')
    localStorage.removeItem('tremen_role')
    localStorage.removeItem('tremen_token')
    setUser(null)
  }, [])

  const openLoginModal = useCallback(() => setLoginModalOpen(true), [])
  const closeLoginModal = useCallback(() => setLoginModalOpen(false), [])

  useEffect(() => {
    const handleStorage = () => {
      const email = localStorage.getItem('tremen_email')
      const role = localStorage.getItem('tremen_role')
      setUser(email ? { email, role: role || 'user' } : null)
    }
    window.addEventListener('storage', handleStorage)
    return () => window.removeEventListener('storage', handleStorage)
  }, [])

  return (
    <AuthContext.Provider value={{
      user, login, logout,
      loginModalOpen, openLoginModal, closeLoginModal,
      isAdmin: user?.role === 'admin',
      hasShared, markAsShared, linkedinUsername,
    }}>
      {children}
    </AuthContext.Provider>
  )
}

export const useAuth = () => {
  const ctx = useContext(AuthContext)
  if (!ctx) throw new Error('useAuth must be used within AuthProvider')
  return ctx
}
