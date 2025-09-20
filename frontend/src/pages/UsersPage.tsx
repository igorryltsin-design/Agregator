import React, { useEffect, useState } from 'react'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

type UserRow = {
  id: number
  username: string
  role: 'admin' | 'user'
  full_name?: string | null
  created_at?: string | null
}

export default function UsersPage(){
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'
  const toasts = useToasts()
  const [rows, setRows] = useState<UserRow[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState('')
  const [form, setForm] = useState({ username: '', password: '', role: 'user' as 'admin' | 'user' })
  const [saving, setSaving] = useState(false)

  const load = async () => {
    if (!isAdmin) return
    setLoading(true)
    setError('')
    try {
      const r = await fetch('/api/admin/users')
      if (r.status === 403) {
        setError('Недостаточно прав для просмотра пользователей')
        setRows([])
        return
      }
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && Array.isArray(data.users)) {
        setRows(data.users)
      } else {
        setError(data?.error || 'Не удалось получить список пользователей')
        setRows([])
      }
    } catch {
      setError('Ошибка соединения с сервером')
      setRows([])
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [isAdmin])

  if (!isAdmin) {
    return <div className="card p-3">Недостаточно прав.</div>
  }

  const createUser = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!form.username.trim() || !form.password.trim()) {
      setError('Укажите логин и пароль')
      return
    }
    setSaving(true)
    setError('')
    try {
      const r = await fetch('/api/admin/users', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username: form.username.trim(), password: form.password, role: form.role }),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && data.user) {
        setRows(prev => [...prev, data.user])
        setForm({ username: '', password: '', role: 'user' })
        toasts.push('Пользователь создан', 'success')
      } else {
        setError(data?.error || 'Не удалось создать пользователя')
      }
    } catch {
      setError('Ошибка соединения при создании пользователя')
    } finally {
      setSaving(false)
    }
  }

  const changeRole = async (id: number, role: 'admin' | 'user') => {
    try {
      const r = await fetch(`/api/admin/users/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role }),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && data.user) {
        setRows(prev => prev.map(u => u.id === id ? data.user : u))
        toasts.push('Роль обновлена', 'success')
      } else {
        toasts.push(data?.error || 'Не удалось изменить роль', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при изменении роли', 'error')
    }
  }

  const resetPassword = async (id: number, username: string) => {
    const pw = prompt(`Введите новый пароль для ${username}`)
    if (!pw) return
    if (pw.length < 6) {
      alert('Пароль должен содержать минимум 6 символов')
      return
    }
    try {
      const r = await fetch(`/api/admin/users/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ password: pw }),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('Пароль обновлён', 'success')
      } else {
        toasts.push(data?.error || 'Не удалось обновить пароль', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при обновлении пароля', 'error')
    }
  }

  const removeUser = async (id: number, username: string) => {
    if (!confirm(`Удалить пользователя ${username}?`)) return
    try {
      const r = await fetch(`/api/admin/users/${id}`, { method: 'DELETE' })
      if (r.ok) {
        setRows(prev => prev.filter(u => u.id !== id))
        toasts.push('Пользователь удалён', 'success')
      } else {
        const data = await r.json().catch(() => ({}))
        toasts.push(data?.error || 'Не удалось удалить пользователя', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при удалении пользователя', 'error')
    }
  }

  return (
    <div className="d-grid gap-3">
      <div className="card p-3">
        <div className="fw-semibold mb-2">Создать пользователя</div>
        <form className="row g-3 align-items-end" onSubmit={createUser}>
          <div className="col-md-4">
            <label className="form-label">Логин</label>
            <input className="form-control" value={form.username} onChange={e=>setForm({...form, username:e.target.value})} autoComplete="off" />
          </div>
          <div className="col-md-4">
            <label className="form-label">Пароль</label>
            <input className="form-control" type="password" value={form.password} onChange={e=>setForm({...form, password:e.target.value})} autoComplete="new-password" />
          </div>
          <div className="col-md-2">
            <label className="form-label">Роль</label>
            <select className="form-select" value={form.role} onChange={e=>setForm({...form, role: e.target.value as 'admin' | 'user'})}>
              <option value="user">Пользователь</option>
              <option value="admin">Администратор</option>
            </select>
          </div>
          <div className="col-md-2">
            <button className="btn btn-primary w-100" type="submit" disabled={saving}>{saving ? 'Создание…' : 'Создать'}</button>
          </div>
          {error && <div className="col-12"><div className="alert alert-danger py-2 m-0">{error}</div></div>}
        </form>
      </div>
      <div className="card p-3">
        <div className="fw-semibold mb-2">Список пользователей</div>
        {loading ? (
          <div>Загрузка…</div>
        ) : (
          <div className="table-responsive">
            <table className="table table-sm align-middle">
              <thead>
                <tr>
                  <th>Логин</th>
                  <th>ФИО</th>
                  <th>Роль</th>
                  <th>Создан</th>
                  <th style={{ width: 220 }}>Действия</th>
                </tr>
              </thead>
              <tbody>
                {rows.map(u => (
                  <tr key={u.id}>
                    <td>{u.username}</td>
                    <td>{u.full_name || '—'}</td>
                    <td>
                      <select className="form-select form-select-sm" value={u.role} onChange={e=>changeRole(u.id, e.target.value as 'admin' | 'user')} disabled={u.id === user.id}>
                        <option value="user">Пользователь</option>
                        <option value="admin">Администратор</option>
                      </select>
                    </td>
                    <td>{u.created_at ? new Date(u.created_at).toLocaleString() : '—'}</td>
                    <td>
                      <div className="btn-group btn-group-sm">
                        <button className="btn btn-outline-secondary" onClick={()=>resetPassword(u.id, u.username)}>Пароль</button>
                        <button className="btn btn-outline-danger" onClick={()=>removeUser(u.id, u.username)} disabled={u.id === user.id}>Удалить</button>
                      </div>
                    </td>
                  </tr>
                ))}
                {rows.length === 0 && (
                  <tr><td colSpan={4} className="text-center text-muted py-3">Пользователи не найдены</td></tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  )
}
