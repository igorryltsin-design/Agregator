import React, { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

export default function ProfilePage(){
  const { user, logout, refresh } = useAuth()
  const toasts = useToasts()
  const navigate = useNavigate()
  const [current, setCurrent] = useState('')
  const [p1, setP1] = useState('')
  const [p2, setP2] = useState('')
  const [busy, setBusy] = useState(false)
  const [error, setError] = useState('')
  const [fullName, setFullName] = useState(user?.full_name || '')
  const [profileBusy, setProfileBusy] = useState(false)

  if (!user) {
    return <div className="card p-3">Загрузка профиля…</div>
  }

  useEffect(() => {
    setFullName(user.full_name || '')
  }, [user.full_name])

  const changePassword = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    setError('')
    if (!p1 || p1.length < 6) {
      setError('Новый пароль должен содержать минимум 6 символов')
      return
    }
    if (p1 !== p2) {
      setError('Новый пароль и подтверждение не совпадают')
      return
    }
    setBusy(true)
    try {
      const r = await fetch('/api/auth/password', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ current_password: current, new_password: p1 }),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('Пароль обновлён', 'success')
        setCurrent(''); setP1(''); setP2('')
      } else {
        setError(data?.error || 'Не удалось обновить пароль')
      }
    } catch {
      setError('Ошибка соединения с сервером')
    } finally {
      setBusy(false)
    }
  }

  const saveProfile = async () => {
    setProfileBusy(true)
    try {
      const r = await fetch('/api/profile', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ full_name: fullName.trim() })
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('Профиль обновлён', 'success')
        await refresh()
      } else {
        toasts.push(data?.error || 'Не удалось обновить профиль', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения с сервером', 'error')
    } finally {
      setProfileBusy(false)
    }
  }

  const handleLogout = async () => {
    await logout()
    navigate('/login', { replace: true })
  }

  const created = user.created_at ? new Date(user.created_at).toLocaleString() : '—'

  return (
    <div className="card p-3">
      <div className="fw-semibold fs-5 mb-3">Профиль</div>
      <div className="row g-3">
        <div className="col-md-5">
          <div className="card p-3 h-100">
            <div className="fw-semibold mb-2">Учётная запись</div>
            <div className="mb-1"><span className="text-muted">Логин:</span> {user.username}</div>
            <div className="mb-1"><span className="text-muted">Роль:</span> {user.role === 'admin' ? 'Администратор' : 'Пользователь'}</div>
            <div className="mb-3"><span className="text-muted">Создан:</span> {created}</div>
            <div className="mb-3">
              <label className="form-label">ФИО</label>
              <input className="form-control" value={fullName} onChange={e=>setFullName(e.target.value)} placeholder="Фамилия Имя Отчество" />
            </div>
            <div className="d-flex flex-wrap gap-2">
              <button className="btn btn-primary" type="button" onClick={saveProfile} disabled={profileBusy}>{profileBusy ? 'Сохранение…' : 'Сохранить профиль'}</button>
              <button className="btn btn-outline-secondary" type="button" onClick={()=>setFullName(user.full_name || '')} disabled={profileBusy}>Сбросить</button>
              <button className="btn btn-outline-secondary ms-auto" type="button" onClick={handleLogout}>Выйти</button>
            </div>
          </div>
        </div>
        <div className="col-md-7">
          <div className="card p-3 h-100">
            <div className="fw-semibold mb-2">Смена пароля</div>
            <form className="d-grid gap-3" onSubmit={changePassword}>
              <div>
                <label className="form-label">Текущий пароль</label>
                <input className="form-control" type="password" value={current} onChange={e=>setCurrent(e.target.value)} autoComplete="current-password" />
              </div>
              <div>
                <label className="form-label">Новый пароль</label>
                <input className="form-control" type="password" value={p1} onChange={e=>setP1(e.target.value)} autoComplete="new-password" />
              </div>
              <div>
                <label className="form-label">Подтверждение</label>
                <input className="form-control" type="password" value={p2} onChange={e=>setP2(e.target.value)} autoComplete="new-password" />
              </div>
              {error && <div className="alert alert-danger py-2 m-0">{error}</div>}
              <div className="d-flex gap-2">
                <button className="btn btn-primary" type="submit" disabled={busy}>{busy ? 'Сохранение…' : 'Обновить пароль'}</button>
                <button className="btn btn-outline-secondary" type="button" onClick={()=>{ setCurrent(''); setP1(''); setP2(''); setError('') }}>Сбросить</button>
              </div>
            </form>
          </div>
        </div>
      </div>
    </div>
  )
}
