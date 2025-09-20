import React, { useEffect, useState } from 'react'
import { useLocation, useNavigate } from 'react-router-dom'
import { useAuth } from '../ui/Auth'

export default function LoginPage(){
  const { login, user, loading } = useAuth()
  const [username, setUsername] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const navigate = useNavigate()
  const location = useLocation()
  const from = (location.state as { from?: string } | undefined)?.from || '/'

  useEffect(() => {
    if (!loading && user) {
      navigate(from, { replace: true })
    }
  }, [loading, user, navigate, from])

  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    setBusy(true)
    setError('')
    const res = await login(username.trim(), password)
    setBusy(false)
    if (res.ok) {
      navigate(from, { replace: true })
    } else {
      setError(res.error || 'Не удалось войти')
    }
  }

  return (
    <div className="login-screen">
      <div className="login-abstract">
        <div className="orb orb-1"></div>
        <div className="orb orb-2"></div>
        <div className="orb orb-3"></div>
        <div className="orb orb-4"></div>
      </div>
      <div className="login-card card p-4">
        <div className="fw-semibold fs-4 mb-3 text-center">Agregator — вход</div>
        <div className="text-muted text-center mb-4" style={{ fontSize: 14 }}>
          Управляйте коллекциями, задачами и LLM-сервисами в едином окне.
        </div>
        <form className="d-grid gap-3" onSubmit={submit}>
          <div>
            <label className="form-label">Логин</label>
            <input className="form-control" value={username} onChange={e=>setUsername(e.target.value)} autoFocus autoComplete="username" />
          </div>
          <div>
            <label className="form-label">Пароль</label>
            <input className="form-control" type="password" value={password} onChange={e=>setPassword(e.target.value)} autoComplete="current-password" />
          </div>
          {error && <div className="alert alert-danger py-2 m-0">{error}</div>}
          <button className="btn btn-primary" type="submit" disabled={busy}>{busy ? 'Вход…' : 'Войти'}</button>
        </form>
        <div className="text-muted mt-3" style={{ fontSize: 13 }}>
          Доступ предоставляется администраторами. После входа будут доступны функции согласно вашей роли.
        </div>
      </div>
    </div>
  )
}
