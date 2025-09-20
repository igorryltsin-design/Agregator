import React, { useEffect, useMemo, useState } from 'react'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

type Member = {
  user_id: number
  username?: string | null
  role: string
}

type CollectionRow = {
  id: number
  name: string
  owner_id?: number | null
  owner_username?: string | null
  is_private?: boolean
  count?: number
  members?: Member[]
}

const ROLE_LABELS: Record<string, string> = {
  viewer: 'Читатель',
  editor: 'Редактор',
  owner: 'Владелец',
}

export default function AdminCollectionsPage() {
  const { user } = useAuth()
  const toasts = useToasts()
  const isAdmin = user?.role === 'admin'
  const [collections, setCollections] = useState<CollectionRow[]>([])
  const [selectedId, setSelectedId] = useState<number | null>(null)
  const [members, setMembers] = useState<Member[]>([])
  const [loading, setLoading] = useState(false)
  const [rescanLoading, setRescanLoading] = useState(false)
  const [memberForm, setMemberForm] = useState({ user_id: '', role: 'viewer' })

  const loadCollections = async () => {
    setLoading(true)
    try {
      if (isAdmin) {
        const r = await fetch('/api/admin/collections')
        const data = await r.json().catch(() => ({}))
        if (r.ok && data?.ok) {
          setCollections(Array.isArray(data.collections) ? data.collections : [])
        } else {
          toasts.push(data?.error || 'Не удалось получить коллекции', 'error')
        }
      } else {
        const r = await fetch('/api/collections')
        const data = await r.json().catch(() => [])
        if (Array.isArray(data)) {
          setCollections(data)
        } else {
          toasts.push('Не удалось получить коллекции', 'error')
        }
      }
    } catch {
      toasts.push('Ошибка соединения при загрузке коллекций', 'error')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { loadCollections() }, [isAdmin])

  useEffect(() => {
    if (!selectedId && collections.length) {
      setSelectedId(collections[0].id)
    }
  }, [collections, selectedId])

  useEffect(() => {
    if (!selectedId) {
      setMembers([])
      return
    }
    const loadMembers = async () => {
      try {
        const r = await fetch(`/api/collections/${selectedId}/members`)
        const data = await r.json().catch(() => ({}))
        if (r.ok && data?.ok) {
          setMembers(Array.isArray(data.members) ? data.members : [])
        } else {
          setMembers([])
          if (data?.error) toasts.push(data.error, 'error')
        }
      } catch {
        setMembers([])
      }
    }
    loadMembers()
  }, [selectedId, toasts])

  const selectedCollection = useMemo(() => collections.find(c => c.id === selectedId) || null, [collections, selectedId])

  const rescanCollection = async () => {
    if (!selectedId) return
    if (!confirm('Пересканировать выбранную коллекцию?')) return
    setRescanLoading(true)
    try {
      const fd = new FormData()
      fd.set('extract_text', 'on')
      const r = await fetch(`/scan/collection/${selectedId}`, { method: 'POST', body: fd })
      const data = await r.json().catch(() => ({}))
      if (r.status === 409 || data?.status === 'busy') {
        toasts.push('Сканирование уже запущено', 'error')
      } else if (r.status === 404 || data?.status === 'not_found') {
        toasts.push('Коллекция не найдена', 'error')
      } else if (r.ok && data?.status === 'empty') {
        const msg = data?.missing ? `В коллекции нет файлов для сканирования (отсутствует ${data.missing}).` : 'В коллекции нет файлов для сканирования.'
        toasts.push(msg, 'info')
      } else if (r.ok && data?.status === 'started') {
        const msg = data?.missing ? `Сканирование запущено (${data.files} файлов, пропущено ${data.missing}).` : `Сканирование запущено (${data.files} файлов).`
        toasts.push(msg, 'success')
        try { window.dispatchEvent(new Event('scan-open')) } catch {}
      } else {
        toasts.push('Не удалось запустить сканирование коллекции', 'error')
      }
    } catch {
      toasts.push('Ошибка при запуске сканирования коллекции', 'error')
    } finally {
      setRescanLoading(false)
    }
  }

  const addMember = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (!selectedId) return
    const payload = {
      user_id: memberForm.user_id.trim(),
      role: memberForm.role,
    }
    try {
      const r = await fetch(`/api/collections/${selectedId}/members`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        setMemberForm({ user_id: '', role: 'viewer' })
        setMembers(Array.isArray(data.members) ? data.members : [])
        toasts.push('Участник добавлен', 'success')
      } else {
        toasts.push(data?.error || 'Не удалось добавить участника', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при добавлении участника', 'error')
    }
  }

  const updateRole = async (userId: number, role: string) => {
    if (!selectedId) return
    try {
      const r = await fetch(`/api/collections/${selectedId}/members/${userId}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ role }),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('Роль обновлена', 'success')
        setMembers(prev => prev.map(m => (m.user_id === userId ? { ...m, role } : m)))
      } else {
        toasts.push(data?.error || 'Не удалось обновить роль', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при обновлении роли', 'error')
    }
  }

  const removeMember = async (userId: number) => {
    if (!selectedId) return
    if (!confirm('Удалить участника из коллекции?')) return
    try {
      const r = await fetch(`/api/collections/${selectedId}/members/${userId}`, { method: 'DELETE' })
      if (r.ok) {
        toasts.push('Участник удалён', 'success')
        setMembers(prev => prev.filter(m => m.user_id !== userId))
      } else {
        const data = await r.json().catch(() => ({}))
        toasts.push(data?.error || 'Не удалось удалить участника', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при удалении участника', 'error')
    }
  }

  if (!collections.length && !loading) {
    return (
      <div className="card p-3">
        <div className="fw-semibold mb-2">Коллекции</div>
        <div className="text-muted">Нет доступных коллекций.</div>
      </div>
    )
  }

  return (
    <div className="row g-3">
      <div className="col-lg-5">
        <div className="card p-3 h-100">
          <div className="d-flex justify-content-between align-items-center mb-2">
            <div className="fw-semibold">Коллекции</div>
            <button className="btn btn-outline-secondary btn-sm" onClick={loadCollections} disabled={loading}>{loading ? '…' : 'Обновить'}</button>
          </div>
          <div className="list-group" style={{ maxHeight: 420, overflow: 'auto' }}>
            {collections.map(col => (
              <button
                key={col.id}
                type="button"
                className={`list-group-item list-group-item-action ${selectedId === col.id ? 'active' : ''}`}
                onClick={() => setSelectedId(col.id)}
              >
                <div className="d-flex justify-content-between align-items-center">
                  <div>
                    <div className="fw-semibold">{col.name}</div>
                    <div style={{ fontSize: 12 }} className="text-muted">
                      {col.owner_username ? `Владелец: ${col.owner_username}` : 'Владелец не задан'} · {col.is_private ? 'Приватная' : 'Общая'} · {col.count ?? 0} файлов
                    </div>
                  </div>
                </div>
              </button>
            ))}
          </div>
        </div>
      </div>
      <div className="col-lg-7">
        <div className="card p-3 h-100">
          <div className="fw-semibold mb-2">Участники</div>
          {selectedCollection ? (
            <>
              <div className="d-flex justify-content-between align-items-center mb-3">
                <div>
                  <div className="fw-semibold">{selectedCollection.name}</div>
                  <div className="text-muted" style={{ fontSize: 12 }}>
                    {selectedCollection.owner_username ? `Владелец: ${selectedCollection.owner_username}` : 'Владелец не задан'} · {selectedCollection.is_private ? 'Приватная' : 'Общая'}
                  </div>
                </div>
                {isAdmin && (
                  <button
                    type="button"
                    className="btn btn-outline-secondary btn-sm"
                    onClick={rescanCollection}
                    disabled={rescanLoading}
                  >
                    {rescanLoading ? '...' : 'Пересканировать'}
                  </button>
                )}
              </div>
              <form className="row g-2 align-items-end mb-3" onSubmit={addMember}>
                <div className="col-md-4">
                  <label className="form-label">ID пользователя</label>
                  <input className="form-control" value={memberForm.user_id} onChange={e=>setMemberForm({...memberForm, user_id:e.target.value})} required />
                </div>
                <div className="col-md-4">
                  <label className="form-label">Роль</label>
                  <select className="form-select" value={memberForm.role} onChange={e=>setMemberForm({...memberForm, role:e.target.value})}>
                    <option value="viewer">{ROLE_LABELS.viewer}</option>
                    <option value="editor">{ROLE_LABELS.editor}</option>
                    <option value="owner" disabled={!isAdmin}>{ROLE_LABELS.owner}</option>
                  </select>
                </div>
                <div className="col-md-2">
                  <button className="btn btn-primary w-100" type="submit">Добавить</button>
                </div>
              </form>
              <div className="table-responsive" style={{ maxHeight: 300, overflow: 'auto' }}>
                <table className="table table-sm">
                  <thead>
                    <tr>
                      <th>ID</th>
                      <th>Имя</th>
                      <th>Роль</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {members.map(m => (
                      <tr key={m.user_id}>
                        <td>{m.user_id}</td>
                        <td>{m.username ?? '—'}</td>
                        <td>
                          <select className="form-select form-select-sm" value={m.role} onChange={e=>updateRole(m.user_id, e.target.value)}>
                            <option value="viewer">{ROLE_LABELS.viewer}</option>
                            <option value="editor">{ROLE_LABELS.editor}</option>
                            <option value="owner" disabled={!isAdmin}>{ROLE_LABELS.owner}</option>
                          </select>
                        </td>
                        <td>
                          <button className="btn btn-outline-danger btn-sm" onClick={()=>removeMember(m.user_id)}>Удалить</button>
                        </td>
                      </tr>
                    ))}
                    {members.length === 0 && (
                      <tr><td colSpan={4} className="text-center text-muted py-3">Участники не найдены</td></tr>
                    )}
                  </tbody>
                </table>
              </div>
            </>
          ) : (
            <div className="text-muted">Выберите коллекцию, чтобы просмотреть участников.</div>
          )}
        </div>
      </div>
    </div>
  )
}
