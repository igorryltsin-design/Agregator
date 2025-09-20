import React, { createContext, useCallback, useContext, useEffect, useMemo, useState } from 'react'

type AuthUser = {
  id: number
  username: string
  role: 'admin' | 'user'
  full_name?: string | null
  aiword_access?: boolean
  can_upload?: boolean
  can_import?: boolean
  created_at?: string | null
  updated_at?: string | null
}

type AuthContextValue = {
  user: AuthUser | null
  loading: boolean
  login: (username: string, password: string) => Promise<{ ok: boolean; error?: string }>
  logout: () => Promise<void>
  refresh: () => Promise<void>
}

const AuthContext = createContext<AuthContextValue | undefined>(undefined)

export function useAuth(): AuthContextValue {
  const ctx = useContext(AuthContext)
  if (!ctx) {
    throw new Error('useAuth должен вызываться внутри AuthProvider')
  }
  return ctx
}

const AuthProvider: React.FC<{ children: React.ReactNode }> = ({ children }) => {
  const [user, setUser] = useState<AuthUser | null>(null)
  const [loading, setLoading] = useState(true)

  const fetchMe = useCallback(async () => {
    try {
      const r = await fetch('/api/auth/me', { credentials: 'same-origin' })
      if (!r.ok) {
        setUser(null)
        return
      }
      const data = await r.json().catch(() => ({}))
      if (data?.ok && data.user) {
        setUser(data.user)
      } else {
        setUser(null)
      }
    } catch {
      setUser(null)
    }
  }, [])

  useEffect(() => {
    (async () => {
      await fetchMe()
      setLoading(false)
    })()
  }, [fetchMe])

  const login = useCallback(async (username: string, password: string) => {
    try {
      const r = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password }),
        credentials: 'same-origin',
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && data.user) {
        setUser(data.user)
        setLoading(false)
        return { ok: true }
      }
      setUser(null)
      setLoading(false)
      return { ok: false, error: data?.error || 'Ошибка входа' }
    } catch {
      setUser(null)
      setLoading(false)
      return { ok: false, error: 'Ошибка подключения к серверу' }
    }
  }, [])

  const logout = useCallback(async () => {
    try {
      await fetch('/api/auth/logout', { method: 'POST', credentials: 'same-origin' })
    } catch {
      // игнорируем
    }
    setUser(null)
  }, [])

  const refresh = useCallback(async () => {
    await fetchMe()
  }, [fetchMe])

  const value = useMemo<AuthContextValue>(() => ({ user, loading, login, logout, refresh }), [user, loading, login, logout, refresh])

  return (
    <AuthContext.Provider value={value}>
      {children}
    </AuthContext.Provider>
  )
}

export default AuthProvider
