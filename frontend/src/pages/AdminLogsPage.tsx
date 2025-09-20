import React, { useCallback, useEffect, useState } from 'react'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

type ActionLog = {
  id: number
  user_id: number | null
  username?: string | null
  full_name?: string | null
  action: string
  entity?: string | null
  entity_id?: number | null
  detail?: string | null
  created_at?: string | null
}

export default function AdminLogsPage() {
  const { user } = useAuth()
  const toasts = useToasts()
  const isAdmin = user?.role === 'admin'
  const [logs, setLogs] = useState<ActionLog[]>([])
  const [loading, setLoading] = useState(false)
  const [userFilter, setUserFilter] = useState('')
  const [actionFilter, setActionFilter] = useState('')
  const [deleteBefore, setDeleteBefore] = useState('')
  const [deleting, setDeleting] = useState(false)

  const load = useCallback(async () => {
    if (!isAdmin) return
    setLoading(true)
    try {
      const params = new URLSearchParams()
      if (userFilter.trim()) params.set('user_id', userFilter.trim())
      if (actionFilter.trim()) params.set('action', actionFilter.trim())
      const r = await fetch(`/api/admin/actions?${params.toString()}`)
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        setLogs(Array.isArray(data.actions) ? data.actions : [])
      } else {
        toasts.push(data?.error || 'Не удалось получить журнал действий', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при получении журнала', 'error')
    } finally {
      setLoading(false)
    }
  }, [isAdmin, userFilter, actionFilter, toasts])

  const handleDelete = useCallback(async () => {
    if (!isAdmin || !deleteBefore.trim()) {
      return
    }
    setDeleting(true)
    try {
      const body = { before: deleteBefore.trim() }
      const r = await fetch('/api/admin/actions', {
        method: 'DELETE',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(body),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push(`Удалено записей: ${data.deleted ?? 0}`, 'success')
        setDeleteBefore('')
        await load()
      } else {
        toasts.push(data?.error || 'Не удалось удалить записи', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при удалении', 'error')
    } finally {
      setDeleting(false)
    }
  }, [deleteBefore, isAdmin, load, toasts])

  useEffect(() => { load() }, [load])

  const flattenDetail = useCallback((value: any, prefix = '', acc: string[] = []) => {
    if (value === null || value === undefined) {
      acc.push(`${prefix}: null`)
      return acc
    }
    if (typeof value === 'object') {
      if (Array.isArray(value)) {
        if (!value.length) {
          const label = prefix || 'value'
          acc.push(`${label}: []`)
        } else {
          value.forEach((item, idx) => {
            const nextPrefix = prefix ? `${prefix}[${idx}]` : `[${idx}]`
            flattenDetail(item, nextPrefix, acc)
          })
        }
      } else {
        const entries = Object.entries(value)
        if (!entries.length) {
          const label = prefix || 'value'
          acc.push(`${label}: {}`)
        } else {
          entries.forEach(([k, v]) => {
            const nextPrefix = prefix ? `${prefix}.${k}` : k
            flattenDetail(v, nextPrefix, acc)
          })
        }
      }
      return acc
    }
    const label = prefix || 'value'
    acc.push(`${label}: ${String(value)}`)
    return acc
  }, [])

  const formatDetail = useCallback((detail: string | null) => {
    if (!detail) return '—'
    try {
      const parsed = JSON.parse(detail)
      const lines = flattenDetail(parsed)
      return lines.join('\n') || '—'
    } catch {
      return detail.replace(/[{}]/g, '')
    }
  }, [flattenDetail])

  if (!isAdmin) return <div className="card p-3">Недостаточно прав.</div>

  return (
    <div className="card p-3">
      <div className="d-flex flex-wrap gap-2 align-items-end mb-3">
        <div className="flex-grow-1" style={{ maxWidth: 200 }}>
          <label className="form-label">Пользователь ID</label>
          <input className="form-control form-control-sm" value={userFilter} onChange={e=>setUserFilter(e.target.value)} placeholder="пример: 2" />
        </div>
        <div className="flex-grow-1" style={{ maxWidth: 220 }}>
          <label className="form-label">Действие</label>
          <input className="form-control form-control-sm" value={actionFilter} onChange={e=>setActionFilter(e.target.value)} placeholder="например user_update" />
        </div>
        <div className="d-flex align-items-end gap-2">
          <button className="btn btn-outline-secondary" onClick={load} disabled={loading}>{loading ? 'Загрузка…' : 'Обновить'}</button>
          <button className="btn btn-outline-secondary" onClick={()=>{ setUserFilter(''); setActionFilter(''); }} disabled={loading}>Сбросить</button>
        </div>
        <div className="d-flex align-items-end gap-2" style={{ maxWidth: 280 }}>
          <div className="flex-grow-1">
            <label className="form-label">Удалить записи до даты</label>
            <input
              type="date"
              className="form-control form-control-sm"
              value={deleteBefore}
              onChange={e => setDeleteBefore(e.target.value)}
            />
          </div>
          <button
            className="btn btn-outline-danger"
            onClick={handleDelete}
            disabled={deleting || !deleteBefore.trim()}
          >
            {deleting ? 'Удаление…' : 'Удалить'}
          </button>
        </div>
      </div>
      <div className="table-responsive">
        <table className="table table-sm align-middle">
          <thead>
            <tr>
              <th>ID</th>
              <th>Пользователь</th>
              <th>Действие</th>
              <th>Объект</th>
              <th>Детали</th>
              <th>Время</th>
            </tr>
          </thead>
          <tbody>
            {logs.map(log => (
              <tr key={log.id}>
                <td>{log.id}</td>
                <td>
                  {log.username ? (
                    <span>{log.username}{log.full_name ? ` (${log.full_name})` : ''}</span>
                  ) : (log.user_id ?? '—')}
                </td>
                <td><span className="badge bg-secondary">{log.action}</span></td>
                <td>{log.entity ? `${log.entity}${log.entity_id ? `#${log.entity_id}` : ''}` : '—'}</td>
                <td>
                  <pre style={{ fontSize: 12, margin: 0, whiteSpace: 'pre-wrap' }}>{formatDetail(log.detail)}</pre>
                </td>
                <td>{log.created_at ? new Date(log.created_at).toLocaleString() : '—'}</td>
              </tr>
            ))}
            {logs.length === 0 && (
              <tr><td colSpan={6} className="text-center text-muted py-3">Нет записей</td></tr>
            )}
          </tbody>
        </table>
      </div>
    </div>
  )
}
