import React, { useEffect, useMemo, useState } from 'react'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

type LlmEndpoint = {
  id: number
  name: string
  base_url: string
  model: string
  weight: number
  purpose?: string | null
  purposes?: string[]
  created_at?: string | null
}

type LlmPurposeOption = { id: string; label: string }

type CreateFormState = {
  name: string
  base_url: string
  model: string
  weight: string
  api_key: string
  purposes: string[]
}

const defaultForm: CreateFormState = {
  name: '',
  base_url: '',
  model: '',
  weight: '1',
  api_key: '',
  purposes: ['default'],
}

export default function AdminLlmPage() {
  const { user } = useAuth()
  const toasts = useToasts()
  const isAdmin = user?.role === 'admin'
  const [list, setList] = useState<LlmEndpoint[]>([])
  const [loading, setLoading] = useState(false)
  const [form, setForm] = useState<CreateFormState>(defaultForm)
  const [purposeOptions, setPurposeOptions] = useState<LlmPurposeOption[]>([])

  const normalizedPurposeOptions = useMemo(() => {
    const options = [...purposeOptions]
    if (!options.some(o => o.id === 'default')) {
      options.unshift({ id: 'default', label: 'По умолчанию' })
    }
    return options
  }, [purposeOptions])

  const purposeLabel = useMemo(() => {
    const map = new Map<string, string>()
    normalizedPurposeOptions.forEach(opt => map.set(opt.id, opt.label))
    return map
  }, [normalizedPurposeOptions])

  const load = async () => {
    if (!isAdmin) return
    setLoading(true)
    try {
      const r = await fetch('/api/admin/llm-endpoints')
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        setList(Array.isArray(data.items) ? data.items : [])
        const catalog = Array.isArray(data.purposes_catalog) ? data.purposes_catalog : []
        setPurposeOptions(catalog)
      } else {
        toasts.push(data?.error || 'Не удалось получить список LLM', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при загрузке LLM', 'error')
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [isAdmin])

  if (!isAdmin) return <div className="card p-3">Недостаточно прав.</div>

  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    try {
      const payload = {
        name: form.name.trim(),
        base_url: form.base_url.trim(),
        model: form.model.trim(),
        weight: parseFloat(form.weight || '1') || 1,
        api_key: form.api_key.trim(),
        purposes: form.purposes.length ? form.purposes : ['default'],
      }
      const r = await fetch('/api/admin/llm-endpoints', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('LLM добавлена', 'success')
        setForm(defaultForm)
        load()
      } else {
        toasts.push(data?.error || 'Не удалось добавить LLM', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при добавлении LLM', 'error')
    }
  }

  const remove = async (id: number) => {
    if (!confirm('Удалить LLM-эндпоинт?')) return
    try {
      const r = await fetch(`/api/admin/llm-endpoints/${id}`, { method: 'DELETE' })
      if (r.ok) {
        toasts.push('Удалено', 'success')
        setList(prev => prev.filter(x => x.id !== id))
      } else {
        const data = await r.json().catch(() => ({}))
        toasts.push(data?.error || 'Не удалось удалить', 'error')
      }
    } catch {
      toasts.push('Ошибка соединения при удалении LLM', 'error')
    }
  }

  const updateEndpoint = async (id: number, patch: Record<string, unknown>, successMessage = 'Обновлено') => {
    try {
      const r = await fetch(`/api/admin/llm-endpoints/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(patch),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && data.item) {
        toasts.push(successMessage, 'success')
        setList(prev => prev.map(item => item.id === id ? { ...item, ...data.item } : item))
        return true
      }
      toasts.push(data?.error || 'Не удалось обновить LLM', 'error')
    } catch {
      toasts.push('Ошибка соединения при обновлении LLM', 'error')
    }
    return false
  }

  return (
    <div className="d-grid gap-3">
      <div className="card p-3">
        <div className="fw-semibold mb-2">Добавить новый эндпоинт</div>
        <form className="row g-3" onSubmit={submit}>
          <div className="col-md-3">
            <label className="form-label">Название</label>
            <input className="form-control" value={form.name} onChange={e => setForm(prev => ({ ...prev, name: e.target.value }))} required />
          </div>
          <div className="col-md-4">
            <label className="form-label">Base URL</label>
            <input className="form-control" value={form.base_url} onChange={e => setForm(prev => ({ ...prev, base_url: e.target.value }))} required placeholder="http://localhost:1234/v1" />
          </div>
          <div className="col-md-3">
            <label className="form-label">Модель</label>
            <input className="form-control" value={form.model} onChange={e => setForm(prev => ({ ...prev, model: e.target.value }))} required placeholder="google/gemma" />
          </div>
          <div className="col-md-2">
            <label className="form-label">Вес</label>
            <input className="form-control" type="number" step="0.1" value={form.weight} onChange={e => setForm(prev => ({ ...prev, weight: e.target.value }))} />
          </div>
          <div className="col-md-4">
            <label className="form-label">Типы задач</label>
            <select
              className="form-select"
              multiple
              value={form.purposes}
              onChange={e => {
                const values = Array.from(e.target.selectedOptions).map(opt => opt.value)
                setForm(prev => ({ ...prev, purposes: values.length ? values : ['default'] }))
              }}
              size={Math.min(Math.max(normalizedPurposeOptions.length, 4), 8)}
            >
              {normalizedPurposeOptions.map(opt => (
                <option value={opt.id} key={opt.id}>{opt.label}</option>
              ))}
            </select>
            <div className="form-text">По умолчанию используются эндпоинты с назначением «По умолчанию».</div>
          </div>
          <div className="col-md-4">
            <label className="form-label">API ключ (опционально)</label>
            <input className="form-control" value={form.api_key} onChange={e => setForm(prev => ({ ...prev, api_key: e.target.value }))} />
          </div>
          <div className="col-12">
            <button className="btn btn-primary" type="submit" disabled={loading}>Добавить</button>
          </div>
        </form>
      </div>
      <div className="card p-3">
        <div className="d-flex justify-content-between align-items-center mb-2">
          <div className="fw-semibold">Сохранённые эндпоинты</div>
          <button className="btn btn-outline-secondary btn-sm" onClick={load} disabled={loading}>{loading ? 'Обновление…' : 'Обновить'}</button>
        </div>
        <div className="table-responsive">
          <table className="table table-sm align-middle">
            <thead>
              <tr>
                <th>ID</th>
                <th>Название</th>
                <th>URL</th>
                <th>Модель</th>
                <th style={{ width: 110 }}>Вес</th>
                <th style={{ minWidth: 220 }}>Назначения</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {list.map(item => {
                const selected = item.purposes && item.purposes.length ? item.purposes : (item.purpose ? item.purpose.split(',').map(x => x.trim()).filter(Boolean) : ['default'])
                const chips = selected.map(id => purposeLabel.get(id) || id)
                return (
                  <tr key={item.id}>
                    <td>{item.id}</td>
                    <td>{item.name}</td>
                    <td style={{ maxWidth: 260 }}>{item.base_url}</td>
                    <td>{item.model}</td>
                    <td>
                      <input
                        className="form-control form-control-sm"
                        type="number"
                        step="0.1"
                        defaultValue={item.weight}
                        onBlur={e => updateEndpoint(item.id, { weight: parseFloat(e.target.value || '1') || 1 })}
                      />
                    </td>
                    <td>
                      <select
                        className="form-select form-select-sm"
                        multiple
                        value={selected}
                        onChange={e => {
                          const values = Array.from(e.target.selectedOptions).map(opt => opt.value)
                          updateEndpoint(item.id, { purposes: values.length ? values : ['default'] })
                        }}
                        size={Math.min(Math.max(normalizedPurposeOptions.length, 4), 6)}
                      >
                        {normalizedPurposeOptions.map(opt => (
                          <option value={opt.id} key={opt.id}>{opt.label}</option>
                        ))}
                      </select>
                      <div className="text-muted" style={{ fontSize: 12 }}>{chips.join(', ') || 'По умолчанию'}</div>
                    </td>
                    <td>
                      <button className="btn btn-outline-danger btn-sm" onClick={() => remove(item.id)}>Удалить</button>
                    </td>
                  </tr>
                )
              })}
              {list.length === 0 && (
                <tr><td colSpan={7} className="text-center text-muted py-3">Список пуст</td></tr>
              )}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  )
}
