import React, { useCallback, useEffect, useMemo, useState } from 'react'
import { Link } from 'react-router-dom'
import { useAuth } from '../ui/Auth'
import { useToasts } from '../ui/Toasts'

type Collection = {
  id: number
  name: string
  slug?: string
  searchable: boolean
  graphable: boolean
  is_private?: boolean
  count?: number
}
type LlmEndpointInfo = {
  id: number
  name: string
  base_url: string
  model: string
  weight: number
  purpose?: string | null
  purposes?: string[]
}
type LlmPurposeOption = { id: string; label: string }
type AiwordAccessUser = { user_id: number; username?: string | null; full_name?: string | null }
type UserSuggestion = { id: number; username: string; full_name?: string | null }
type Settings = {
  scan_root: string
  extract_text: boolean
  lm_base: string
  lm_model: string
  lm_key: string
  transcribe_enabled: boolean
  transcribe_backend: string
  transcribe_model: string
  transcribe_language: string
  summarize_audio: boolean
  audio_keywords_llm: boolean
  vision_images: boolean
  kw_to_tags: boolean
  type_detect_flow: string
  type_llm_override: boolean
  import_subdir: string
  move_on_rename: boolean
  collections_in_dirs?: boolean
  collection_type_subdirs?: boolean
  ocr_langs: string
  pdf_ocr_pages: number
  ocr_first_page_dissertation?: boolean
  prompts?: Record<string, string>
  ai_rerank_llm?: boolean
  collections?: Collection[]
  llm_endpoints?: LlmEndpointInfo[]
  llm_purposes?: LlmPurposeOption[]
  aiword_users?: AiwordAccessUser[]
  default_use_llm?: boolean
  default_prune?: boolean
  type_dirs?: Record<string, string>
}

const defaultPrompts: Record<string, string> = {
  metadata_system: '',
  summarize_audio_system: '',
  keywords_system: '',
  vision_system: '',
}

const boolVal = (value: any, fallback: boolean) => (value === undefined ? fallback : !!value)

const normalizeSettings = (raw: any): Settings => {
  const prompts = { ...defaultPrompts, ...(raw?.prompts || {}) }
  const collectionsInDirs = boolVal(raw?.collections_in_dirs, false)
  const collections = Array.isArray(raw?.collections)
    ? raw.collections.map((col: any) => ({
        id: Number(col?.id ?? 0),
        name: String(col?.name ?? ''),
        slug: col?.slug ? String(col.slug) : undefined,
        searchable: boolVal(col?.searchable, true),
        graphable: boolVal(col?.graphable, true),
        is_private: boolVal(col?.is_private, false),
        count: Number.isFinite(col?.count) ? Number(col.count) : 0,
      }))
    : []

  return {
    scan_root: String(raw?.scan_root || ''),
    extract_text: boolVal(raw?.extract_text, true),
    lm_base: String(raw?.lm_base || ''),
    lm_model: String(raw?.lm_model || ''),
    lm_key: String(raw?.lm_key || ''),
    transcribe_enabled: boolVal(raw?.transcribe_enabled, true),
    transcribe_backend: String(raw?.transcribe_backend || 'faster-whisper'),
    transcribe_model: String(raw?.transcribe_model || ''),
    transcribe_language: String(raw?.transcribe_language || 'ru'),
    summarize_audio: boolVal(raw?.summarize_audio, true),
    audio_keywords_llm: boolVal(raw?.audio_keywords_llm, true),
    vision_images: boolVal(raw?.vision_images, false),
    kw_to_tags: boolVal(raw?.kw_to_tags, true),
    type_detect_flow: String(raw?.type_detect_flow || 'extension,filename,heuristics,llm'),
    type_llm_override: boolVal(raw?.type_llm_override, true),
    import_subdir: String(raw?.import_subdir || 'import'),
    move_on_rename: boolVal(raw?.move_on_rename, true),
    collections_in_dirs: collectionsInDirs,
    collection_type_subdirs: collectionsInDirs ? boolVal(raw?.collection_type_subdirs, false) : false,
    ocr_langs: String(raw?.ocr_langs || 'rus+eng'),
    pdf_ocr_pages: Number.isFinite(raw?.pdf_ocr_pages) ? Number(raw.pdf_ocr_pages) : 5,
    ocr_first_page_dissertation: boolVal(raw?.ocr_first_page_dissertation, true),
    prompts,
    ai_rerank_llm: boolVal(raw?.ai_rerank_llm, false),
    collections,
    llm_endpoints: Array.isArray(raw?.llm_endpoints) ? raw.llm_endpoints : [],
    llm_purposes: Array.isArray(raw?.llm_purposes) ? raw.llm_purposes : [],
    aiword_users: Array.isArray(raw?.aiword_users) ? raw.aiword_users : [],
    default_use_llm: boolVal(raw?.default_use_llm, true),
    default_prune: boolVal(raw?.default_prune, true),
    type_dirs: raw?.type_dirs || {},
  }
}

export default function SettingsPage() {
  const { user } = useAuth()
  const toasts = useToasts()
  const isAdmin = user?.role === 'admin'
  const [s, setS] = useState<Settings | null>(null)
  const [saving, setSaving] = useState(false)
  const [reindexUseLLM, setReindexUseLLM] = useState(true)
  const [reindexPrune, setReindexPrune] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const [aiwordQuery, setAiwordQuery] = useState('')
  const [aiwordOptions, setAiwordOptions] = useState<UserSuggestion[]>([])
  const [aiwordLoading, setAiwordLoading] = useState(false)
  const [llmWeights, setLlmWeights] = useState<Record<number, string>>({})
  const [deleteCollectionId, setDeleteCollectionId] = useState<number | null>(null)

  useEffect(() => {
    if (!isAdmin) return
    let cancelled = false
    ;(async () => {
      try {
        setError(null)
        const r = await fetch('/api/settings')
        if (cancelled) return
        if (r.status === 403) {
          setError('Недостаточно прав для просмотра настроек')
          setS(null)
          return
        }
        const data = await r.json().catch(() => ({}))
        const normalized = normalizeSettings(data)
        setS(normalized)
        setReindexUseLLM(boolVal(normalized.default_use_llm, true))
        setReindexPrune(boolVal(normalized.default_prune, true))
      } catch {
        if (!cancelled) {
          setError('Не удалось загрузить настройки')
        }
      }
    })()
    return () => { cancelled = true }
  }, [isAdmin])

  useEffect(() => {
    if (!s?.llm_endpoints) {
      setLlmWeights({})
      return
    }
    const map: Record<number, string> = {}
    s.llm_endpoints.forEach(ep => { map[ep.id] = String(ep.weight ?? 1) })
    setLlmWeights(map)
  }, [s?.llm_endpoints])

  const assignedAiwordIds = useMemo(() => new Set((s?.aiword_users || []).map(u => u.user_id)), [s?.aiword_users])
  const llmPurposes = useMemo<LlmPurposeOption[]>(() => {
    if (Array.isArray(s?.llm_purposes)) {
      const list = [...s.llm_purposes]
      if (!list.some(p => p.id === 'default')) {
        list.unshift({ id: 'default', label: 'По умолчанию' })
      }
      return list
    }
    return [{ id: 'default', label: 'По умолчанию' }]
  }, [s?.llm_purposes])
  const llmPurposeLabels = useMemo(() => {
    const map = new Map<string, string>()
    llmPurposes.forEach(p => map.set(p.id, p.label))
    return map
  }, [llmPurposes])
  const transcribeBackendOptions = useMemo(() => {
    const options = ['faster-whisper']
    const current = (s?.transcribe_backend || '').trim()
    if (current && !options.includes(current)) {
      options.push(current)
    }
    return options
  }, [s?.transcribe_backend])
  const transcribeModelOptions = useMemo(() => {
    const defaults = ['tiny', 'base', 'small', 'medium', 'large-v2', 'large-v3', 'distil-small.en', 'distil-medium.en', 'distil-large-v2']
    const set = new Set(defaults)
    const current = (s?.transcribe_model || '').trim()
    if (current) {
      set.add(current)
    }
    return Array.from(set)
  }, [s?.transcribe_model])

  useEffect(() => {
    if (!isAdmin) return
    if (!aiwordQuery.trim()) {
      setAiwordOptions([])
      setAiwordLoading(false)
      return
    }
    const controller = new AbortController()
    const handle = window.setTimeout(async () => {
      setAiwordLoading(true)
      try {
        const r = await fetch(`/api/admin/users/search?q=${encodeURIComponent(aiwordQuery)}&limit=10`, { signal: controller.signal })
        const data = await r.json().catch(() => ({}))
        if (!controller.signal.aborted && r.ok && data?.ok && Array.isArray(data.users)) {
          const suggestions: UserSuggestion[] = data.users.filter((u: any) => !assignedAiwordIds.has(u.id)).map((u: any) => ({
            id: u.id,
            username: u.username,
            full_name: u.full_name,
          }))
          setAiwordOptions(suggestions)
        } else if (!controller.signal.aborted) {
          setAiwordOptions([])
        }
      } catch (err) {
        if (!(err instanceof DOMException && err.name === 'AbortError')) {
          if (!controller.signal.aborted) {
            setAiwordOptions([])
          }
        }
      } finally {
        if (!controller.signal.aborted) {
          setAiwordLoading(false)
        }
      }
    }, 250)
    return () => {
      controller.abort()
      window.clearTimeout(handle)
    }
  }, [aiwordQuery, assignedAiwordIds, isAdmin])

  const patchLlmEndpoint = useCallback(async (id: number, payload: Record<string, unknown>): Promise<LlmEndpointInfo | null> => {
    try {
      const r = await fetch(`/api/admin/llm-endpoints/${id}`, {
        method: 'PATCH',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok && data.item) {
        toasts.push('LLM обновлена', 'success')
        return data.item as LlmEndpointInfo
      }
      toasts.push(data?.error || 'Не удалось обновить LLM', 'error')
    } catch {
      toasts.push('Ошибка соединения при обновлении LLM', 'error')
    }
    return null
  }, [toasts])

  const commitLlmWeight = useCallback(async (endpointId: number) => {
    if (!s) return
    const input = llmWeights[endpointId]
    const weight = parseFloat(input)
    const current = (s.llm_endpoints || []).find(ep => ep.id === endpointId)?.weight ?? 1
    if (!Number.isFinite(weight) || weight <= 0) {
      toasts.push('Вес должен быть положительным числом', 'error')
      setLlmWeights(prev => ({ ...prev, [endpointId]: String(current) }))
      return
    }
    const updated = await patchLlmEndpoint(endpointId, { weight })
    if (updated) {
      setS(prev => {
        if (!prev) return prev
        const nextEndpoints = (prev.llm_endpoints || []).map(ep => ep.id === endpointId ? { ...ep, ...updated } : ep)
        return { ...prev, llm_endpoints: nextEndpoints }
      })
      setLlmWeights(prev => ({ ...prev, [endpointId]: String(updated.weight ?? weight) }))
    } else {
      setLlmWeights(prev => ({ ...prev, [endpointId]: String(current) }))
    }
  }, [llmWeights, patchLlmEndpoint, s, toasts])

  const commitLlmPurposes = useCallback(async (endpointId: number, values: string[]) => {
    if (!s) return
    const selected = values.length ? values : ['default']
    const before = (s.llm_endpoints || []).find(ep => ep.id === endpointId)?.purposes || ['default']
    setS(prev => {
      if (!prev) return prev
      const nextEndpoints = (prev.llm_endpoints || []).map(ep => ep.id === endpointId ? { ...ep, purposes: selected, purpose: selected.join(',') } : ep)
      return { ...prev, llm_endpoints: nextEndpoints }
    })
    const updated = await patchLlmEndpoint(endpointId, { purposes: selected })
    if (updated) {
      setS(prev => {
        if (!prev) return prev
        const nextEndpoints = (prev.llm_endpoints || []).map(ep => ep.id === endpointId ? { ...ep, ...updated } : ep)
        return { ...prev, llm_endpoints: nextEndpoints }
      })
    } else {
      setS(prev => {
        if (!prev) return prev
        const nextEndpoints = (prev.llm_endpoints || []).map(ep => ep.id === endpointId ? { ...ep, purposes: before, purpose: before.join(',') } : ep)
        return { ...prev, llm_endpoints: nextEndpoints }
      })
    }
  }, [patchLlmEndpoint, s])

  const addAiwordUser = useCallback((entry: UserSuggestion) => {
    if (!s || assignedAiwordIds.has(entry.id)) return
    const nextEntry: AiwordAccessUser = { user_id: entry.id, username: entry.username, full_name: entry.full_name }
    setS(prev => prev ? { ...prev, aiword_users: [ ...(prev.aiword_users || []), nextEntry ] } : prev)
    setAiwordQuery('')
    setAiwordOptions([])
  }, [assignedAiwordIds, s])

  const removeAiwordUser = useCallback((userId: number) => {
    setS(prev => {
      if (!prev) return prev
      return { ...prev, aiword_users: (prev.aiword_users || []).filter(u => u.user_id !== userId) }
    })
  }, [])

  const save = async () => {
    if (!s) return
    setSaving(true)
    try {
      const { llm_endpoints, llm_purposes, aiword_users, ...rest } = s
      const payload: any = { ...rest, aiword_users: (aiword_users || []).map(u => u.user_id) }
      payload.prompts = { ...defaultPrompts, ...(payload.prompts || {}) }
      payload.collections_in_dirs = !!payload.collections_in_dirs
      payload.collection_type_subdirs = !!payload.collection_type_subdirs
      payload.default_use_llm = reindexUseLLM
      payload.default_prune = reindexPrune
      payload.collections = (payload.collections || []).map((col: Collection) => ({
        id: col.id,
        searchable: !!col.searchable,
        graphable: !!col.graphable,
      }))
      const r = await fetch('/api/settings', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload) })
      if (r.ok) {
        toasts.push('Настройки сохранены', 'success')
      } else {
        toasts.push(r.status === 403 ? 'Недостаточно прав' : 'Ошибка сохранения настроек', 'error')
      }
    } catch {
      toasts.push('Ошибка сохранения настроек', 'error')
    } finally {
      setSaving(false)
    }
  }

  const reindex = async () => {
    try {
      const fd = new FormData()
      if (s?.extract_text) fd.set('extract_text', 'on')
      fd.set('use_llm', reindexUseLLM ? 'on' : 'off')
      if (reindexPrune) fd.set('prune', 'on')
      await fetch('/scan/start', { method: 'POST', body: fd })
      try { window.dispatchEvent(new Event('scan-open')) } catch {}
    } catch {
      alert('Не удалось запустить сканирование')
    }
  }

  const deleteCollection = async (collectionId: number) => {
    const target = (s?.collections || []).find(c => c.id === collectionId)
    if (!target) return
    if (!confirm(`Удалить коллекцию «${target.name}» и все её файлы?`)) return
    setDeleteCollectionId(collectionId)
    try {
      const r = await fetch(`/api/collections/${collectionId}`, { method: 'DELETE' })
      const data = await r.json().catch(() => ({}))
      if (r.ok && data?.ok) {
        toasts.push('Коллекция удалена', 'success')
        setS(prev => prev ? { ...prev, collections: (prev.collections || []).filter(c => c.id !== collectionId) } : prev)
      } else {
        toasts.push(data?.error || 'Не удалось удалить коллекцию', 'error')
      }
    } catch {
      toasts.push('Ошибка при удалении коллекции', 'error')
    } finally {
      setDeleteCollectionId(null)
    }
  }

  const backupDb = async () => {
    try {
      const r = await fetch('/admin/backup-db', { method: 'POST' })
      if (!r.ok) { alert('Ошибка резервного копирования'); return }
      const blob = await r.blob()
      const a = document.createElement('a')
      a.href = URL.createObjectURL(blob)
      a.download = 'catalogue_backup.db'
      a.click()
    } catch {
      alert('Ошибка резервного копирования')
    }
  }

  const clearDb = async () => {
    if (!confirm('Удалить ВСЕ записи? Операция необратима.')) return
    try {
      await fetch('/admin/clear-db', { method: 'POST' })
      alert('База очищена. Перезапустите сканирование.')
    } catch {
      alert('Ошибка очистки базы')
    }
  }

  const importDb = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    const fd = new FormData(e.currentTarget)
    try {
      const r = await fetch('/admin/import-db', { method: 'POST', body: fd })
      alert(r.ok ? 'Импорт завершён. Перезапустите приложение.' : 'Ошибка импорта')
    } catch {
      alert('Ошибка импорта')
    }
  }

  if (!isAdmin) return <div className="card p-3">Недостаточно прав.</div>
  if (error) return <div className="card p-3">{error}</div>
  if (!s) return <div className="card p-3">Загрузка настроек…</div>

  const llmEndpoints = s.llm_endpoints || []
  const aiwordUsers = s.aiword_users || []

  return (
    <div className="d-grid gap-3">
      <div className="card p-3">
        <div className="fw-semibold mb-3 d-flex flex-wrap justify-content-between gap-2 align-items-center">
          <span>Настройки</span>
          <div className="d-flex flex-wrap align-items-center gap-2">
            <button className="btn btn-primary" onClick={save} disabled={saving}>{saving ? 'Сохранение…' : 'Сохранить'}</button>
            <div className="form-check form-switch m-0">
              <input
                className="form-check-input"
                type="checkbox"
                id="reidx_llm"
                checked={reindexUseLLM}
                onChange={e => {
                  setReindexUseLLM(e.target.checked)
                  setS(prev => prev ? { ...prev, default_use_llm: e.target.checked } : prev)
                }}
              />
              <label className="form-check-label" htmlFor="reidx_llm">LLM при переиндексации</label>
            </div>
            <div className="form-check form-switch m-0">
              <input
                className="form-check-input"
                type="checkbox"
                id="reidx_prune"
                checked={reindexPrune}
                onChange={e => {
                  setReindexPrune(e.target.checked)
                  setS(prev => prev ? { ...prev, default_prune: e.target.checked } : prev)
                }}
              />
              <label className="form-check-label" htmlFor="reidx_prune">Удалять отсутствующие файлы</label>
            </div>
            <button className="btn btn-outline-secondary" onClick={reindex}>Переиндексировать библиотеку</button>
          </div>
        </div>
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-2">Индексатор и OCR</div>
        <div className="row g-3">
          <div className="col-md-8">
            <label className="form-label">Корневая папка</label>
            <input className="form-control" placeholder="/path/to/library" value={s.scan_root} onChange={e => setS({ ...s, scan_root: e.target.value })} />
          </div>
          <div className="col-md-4 d-flex align-items-end">
            <div className="form-check form-switch">
              <input className="form-check-input" type="checkbox" id="ext" checked={s.extract_text} onChange={e => setS({ ...s, extract_text: e.target.checked })} />
              <label className="form-check-label" htmlFor="ext">Извлекать текст</label>
            </div>
          </div>
          <div className="col-md-4">
            <label className="form-label">OCR языки</label>
            <input className="form-control" placeholder="rus+eng" value={s.ocr_langs} onChange={e => setS({ ...s, ocr_langs: e.target.value })} />
            <div className="form-text">Напр.: rus+eng</div>
          </div>
          <div className="col-md-4">
            <label className="form-label">PDF OCR: кол-во страниц</label>
            <input className="form-control" type="number" min={0} max={20} value={s.pdf_ocr_pages} onChange={e => setS({ ...s, pdf_ocr_pages: parseInt(e.target.value || '0', 10) })} />
          </div>
          <div className="col-md-4">
            <div className="d-flex flex-column gap-2">
              <div className="form-check form-switch">
                <input className="form-check-input" type="checkbox" id="ocr1st" checked={!!s.ocr_first_page_dissertation} onChange={e => setS({ ...s, ocr_first_page_dissertation: e.target.checked })} />
                <label className="form-check-label" htmlFor="ocr1st">OCR 1‑й стр. для диссертаций</label>
              </div>
              <div className="form-check form-switch">
                <input className="form-check-input" type="checkbox" id="move" checked={s.move_on_rename} onChange={e => setS({ ...s, move_on_rename: e.target.checked })} />
                <label className="form-check-label" htmlFor="move">Перемещать при переименовании</label>
              </div>
              <div className="form-check form-switch">
                <input
                  className="form-check-input"
                  type="checkbox"
                  id="collectionsDirs"
                  checked={!!s.collections_in_dirs}
                  onChange={e => setS(prev => prev ? { ...prev, collections_in_dirs: e.target.checked, collection_type_subdirs: e.target.checked ? prev.collection_type_subdirs : false } : prev)}
                />
                <label className="form-check-label" htmlFor="collectionsDirs">Коллекции в отдельных папках</label>
              </div>
              <div className="form-check form-switch ms-3">
                <input
                  className="form-check-input"
                  type="checkbox"
                  id="collectionsType"
                  checked={!!s.collection_type_subdirs}
                  disabled={!s.collections_in_dirs}
                  onChange={e => setS(prev => prev ? { ...prev, collection_type_subdirs: e.target.checked } : prev)}
                />
                <label className="form-check-label" htmlFor="collectionsType">Подпапки по типам внутри коллекций</label>
              </div>
            </div>
          </div>
        </div>
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-2">LLM и типизация</div>
        <div className="row g-3">
          <div className="col-md-5">
            <label className="form-label">LM API Base</label>
            <input className="form-control" placeholder="http://localhost:1234/v1" value={s.lm_base} onChange={e => setS({ ...s, lm_base: e.target.value })} />
          </div>
          <div className="col-md-4">
            <label className="form-label">LM Модель</label>
            <input className="form-control" placeholder="gpt-4o-mini" value={s.lm_model} onChange={e => setS({ ...s, lm_model: e.target.value })} />
          </div>
          <div className="col-md-3">
            <label className="form-label">LM API Key</label>
            <input className="form-control" placeholder="sk-..." value={s.lm_key} onChange={e => setS({ ...s, lm_key: e.target.value })} />
          </div>
          <div className="col-md-3 d-flex align-items-end">
            <div className="form-check form-switch">
              <input className="form-check-input" type="checkbox" id="vision" checked={s.vision_images} onChange={e => setS({ ...s, vision_images: e.target.checked })} />
              <label className="form-check-label" htmlFor="vision">LLM для изображений (vision)</label>
            </div>
          </div>
          <div className="col-md-9">
            <label className="form-label">Порядок определения типа</label>
            <input className="form-control" placeholder="extension,filename,heuristics" value={s.type_detect_flow} onChange={e => setS({ ...s, type_detect_flow: e.target.value })} />
            <div className="form-check form-switch mt-2">
              <input className="form-check-input" type="checkbox" id="tlo" checked={s.type_llm_override} onChange={e => setS({ ...s, type_llm_override: e.target.checked })} />
              <label className="form-check-label" htmlFor="tlo">LLM может переопределять тип</label>
            </div>
          </div>
          <div className="col-12">
            <div className="form-check form-switch">
              <input className="form-check-input" type="checkbox" id="ktt" checked={s.kw_to_tags} onChange={e => setS({ ...s, kw_to_tags: e.target.checked })} />
              <label className="form-check-label" htmlFor="ktt">Ключевые слова → теги</label>
            </div>
          </div>
          <div className="col-md-3 d-flex align-items-center">
            <div className="form-check form-switch">
              <input className="form-check-input" type="checkbox" id="rerank" checked={!!s.ai_rerank_llm} onChange={e => setS({ ...s, ai_rerank_llm: e.target.checked })} />
              <label className="form-check-label" htmlFor="rerank">LLM‑реранжирование поиска</label>
            </div>
          </div>
        </div>
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-2">Транскрибация и аудио</div>
        <div className="row g-3">
          <div className="col-md-3 d-flex align-items-center">
            <div className="form-check form-switch m-0">
              <input className="form-check-input" type="checkbox" id="transcribe_enabled" checked={s.transcribe_enabled} onChange={e => setS({ ...s, transcribe_enabled: e.target.checked })} />
              <label className="form-check-label" htmlFor="transcribe_enabled">Включить транскрибацию</label>
            </div>
          </div>
          <div className="col-md-3">
            <label className="form-label" htmlFor="transcribe_backend">Бэкенд</label>
            <select
              id="transcribe_backend"
              className="form-select"
              value={s.transcribe_backend}
              onChange={e => setS({ ...s, transcribe_backend: e.target.value })}
            >
              {transcribeBackendOptions.map(option => (
                <option key={option} value={option}>{option}</option>
              ))}
            </select>
            <div className="form-text">Поддерживается faster-whisper.</div>
          </div>
          <div className="col-md-3">
            <label className="form-label" htmlFor="transcribe_model">Модель</label>
            <input
              id="transcribe_model"
              className="form-control"
              placeholder="small"
              list="transcribe-models"
              value={s.transcribe_model}
              onChange={e => setS({ ...s, transcribe_model: e.target.value })}
            />
            <div className="form-text">Можно указать алиас, repo id или путь к модели.</div>
          </div>
          <div className="col-md-3">
            <label className="form-label" htmlFor="transcribe_language">Язык распознавания</label>
            <input
              id="transcribe_language"
              className="form-control"
              placeholder="ru"
              value={s.transcribe_language}
              onChange={e => setS({ ...s, transcribe_language: e.target.value })}
            />
            <div className="form-text">Напр.: ru, en, auto.</div>
          </div>
          <div className="col-md-3 d-flex align-items-center">
            <div className="form-check form-switch m-0">
              <input className="form-check-input" type="checkbox" id="summ_audio" checked={s.summarize_audio} onChange={e => setS({ ...s, summarize_audio: e.target.checked })} />
              <label className="form-check-label" htmlFor="summ_audio">Суммаризировать аудио</label>
            </div>
          </div>
          <div className="col-md-3 d-flex align-items-center">
            <div className="form-check form-switch m-0">
              <input className="form-check-input" type="checkbox" id="audio_kw" checked={s.audio_keywords_llm} onChange={e => setS({ ...s, audio_keywords_llm: e.target.checked })} />
              <label className="form-check-label" htmlFor="audio_kw">Ключевые слова из аудио</label>
            </div>
          </div>
        </div>
        <datalist id="transcribe-models">
          {transcribeModelOptions.map(option => (
            <option key={option} value={option} />
          ))}
        </datalist>
      </div>

      <div className="card p-3">
        <div className="d-flex flex-wrap justify-content-between align-items-start gap-2 mb-3">
          <div>
            <div className="fw-semibold">Назначение LLM эндпоинтов</div>
            <div className="text-muted" style={{ fontSize: 13 }}>Выберите задачи для каждого эндпоинта. Один тип может обслуживаться несколькими LLM — запросы будут распределяться по очереди.</div>
          </div>
          <Link className="btn btn-sm btn-outline-secondary" to="/admin/llm">Управление эндпоинтами</Link>
        </div>
        <div className="d-grid gap-3">
          {llmEndpoints.length === 0 && (
            <div className="text-muted">Нет сохранённых эндпоинтов. Добавьте их на вкладке «LLM».</div>
          )}
          {llmEndpoints.map(ep => {
            const selected = ep.purposes && ep.purposes.length ? ep.purposes : ['default']
            const chips = selected.map(id => llmPurposeLabels.get(id) || id)
            return (
              <div key={ep.id} className="border rounded-3 p-3" style={{ borderColor: 'var(--border)', background: 'var(--surface)' }}>
                <div className="d-flex flex-wrap justify-content-between align-items-start gap-3">
                  <div>
                    <div className="fw-semibold">{ep.name}</div>
                    <div className="text-muted" style={{ fontSize: 13 }}>{ep.model} • {ep.base_url}</div>
                  </div>
                  <div className="d-flex align-items-center gap-2">
                    <label className="form-label m-0" htmlFor={`llm-weight-${ep.id}`} style={{ fontSize: 13 }}>Вес</label>
                    <input
                      id={`llm-weight-${ep.id}`}
                      className="form-control form-control-sm"
                      style={{ width: 90 }}
                      value={llmWeights[ep.id] ?? ''}
                      onChange={e => setLlmWeights(prev => ({ ...prev, [ep.id]: e.target.value }))}
                      onBlur={() => commitLlmWeight(ep.id)}
                      inputMode="decimal"
                    />
                  </div>
                </div>
                <div className="mt-3">
                  <label className="form-label" htmlFor={`llm-purposes-${ep.id}`}>Типы задач</label>
                  <select
                    id={`llm-purposes-${ep.id}`}
                    className="form-select"
                    multiple
                    value={selected}
                    onChange={e => {
                      const values = Array.from(e.target.selectedOptions).map(o => o.value)
                      commitLlmPurposes(ep.id, values)
                    }}
                    size={Math.min(Math.max(llmPurposes.length, 4), 8)}
                  >
                    {llmPurposes.map(option => (
                      <option key={option.id} value={option.id}>{option.label}</option>
                    ))}
                  </select>
                  <div className="form-text">Назначено: {chips.length ? chips.join(', ') : 'По умолчанию'}</div>
                </div>
              </div>
            )
          })}
        </div>
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-1">Доступ к AIWord</div>
        <div className="text-muted mb-3" style={{ fontSize: 13 }}>Администраторы всегда имеют доступ. Назначьте дополнительных пользователей, начав вводить их имя или логин.</div>
        <div className="d-flex flex-wrap align-items-start gap-2">
          <div className="position-relative" style={{ flex: '1 1 280px', maxWidth: 360 }}>
            <input
              className="form-control"
              placeholder="Начните вводить имя пользователя"
              value={aiwordQuery}
              onChange={e => setAiwordQuery(e.target.value)}
            />
            {(aiwordOptions.length > 0 || aiwordLoading) && (
              <div className="border rounded-3 mt-1" style={{ position: 'absolute', insetInlineStart: 0, top: '100%', width: '100%', background: 'var(--surface)', zIndex: 1500, boxShadow: 'var(--card-shadow)' }}>
                {aiwordLoading && <div className="px-3 py-2 text-muted" style={{ fontSize: 13 }}>Поиск…</div>}
                {!aiwordLoading && aiwordOptions.map((opt, idx) => (
                  <button
                    key={opt.id}
                    type="button"
                    className="btn w-100 text-start"
                    style={{ border: 'none', borderBottom: idx === aiwordOptions.length - 1 ? 'none' : '1px solid var(--border)', borderRadius: 0 }}
                    onClick={() => addAiwordUser(opt)}
                  >
                    <div className="fw-semibold" style={{ fontSize: 13 }}>{opt.full_name || '—'}</div>
                    <div className="text-muted" style={{ fontSize: 12 }}>@{opt.username}</div>
                  </button>
                ))}
                {!aiwordLoading && aiwordOptions.length === 0 && <div className="px-3 py-2 text-muted" style={{ fontSize: 13 }}>Пользователи не найдены</div>}
              </div>
            )}
          </div>
        </div>
        <div className="d-flex flex-wrap gap-2 mt-3">
          {aiwordUsers.map(u => {
            const label = u.full_name?.trim() ? u.full_name : u.username
            return (
              <span key={u.user_id} className="badge bg-primary-subtle text-primary d-flex align-items-center gap-2" style={{ fontSize: 13 }}>
                <span>{label}</span>
                <button type="button" className="btn btn-sm btn-outline-primary" onClick={() => removeAiwordUser(u.user_id)} aria-label="Удалить доступ">×</button>
              </span>
            )
          })}
          {aiwordUsers.length === 0 && <span className="text-muted" style={{ fontSize: 13 }}>Доступ ещё не назначен ни одному пользователю.</span>}
        </div>
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-2">Коллекции</div>
        <div className="table-responsive">
          <table className="table table-sm align-middle">
            <thead>
              <tr>
                <th>Название</th>
                <th>Файлов</th>
                <th>Поиск</th>
                <th>Граф</th>
                <th></th>
              </tr>
            </thead>
            <tbody>
              {(s.collections || []).map((c, idx) => (
                <tr key={c.id}>
                  <td>
                    <div className="fw-semibold">{c.name}</div>
                    <div className="text-muted" style={{ fontSize: 12 }}>
                      {c.slug ? `slug: ${c.slug}` : ''}
                    </div>
                  </td>
                  <td className="text-secondary">{c.count ?? 0}</td>
                  <td>
                    <input
                      className="form-check-input"
                      type="checkbox"
                      checked={c.searchable}
                      onChange={e => {
                        const next = [...(s.collections || [])]
                        next[idx] = { ...c, searchable: e.target.checked }
                        setS({ ...s, collections: next })
                      }}
                    />
                  </td>
                  <td>
                    <input
                      className="form-check-input"
                      type="checkbox"
                      checked={c.graphable}
                      onChange={e => {
                        const next = [...(s.collections || [])]
                        next[idx] = { ...c, graphable: e.target.checked }
                        setS({ ...s, collections: next })
                      }}
                    />
                  </td>
                  <td className="text-end">
                    <button
                      className="btn btn-sm btn-outline-danger"
                      onClick={() => deleteCollection(c.id)}
                      disabled={deleteCollectionId === c.id}
                    >
                      {deleteCollectionId === c.id ? '...' : 'Удалить'}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        <CollectionsEditor
          collections={s.collections || []}
          onSaved={async () => {
            const r = await fetch('/api/settings')
            const j = await r.json()
            const normalized = normalizeSettings(j)
            setS(normalized)
            setReindexUseLLM(boolVal(normalized.default_use_llm, true))
            setReindexPrune(boolVal(normalized.default_prune, true))
          }}
        />
      </div>

      <div className="card p-3">
        <div className="fw-semibold mb-2">Управление базой данных</div>
        <div className="d-flex flex-wrap gap-2 align-items-center">
          <button className="btn btn-outline-primary" onClick={backupDb}>Резервная копия</button>
          <form onSubmit={importDb} className="d-flex gap-2 align-items-center">
            <input className="form-control" type="file" name="dbfile" accept=".db" required />
            <button className="btn btn-outline-secondary" type="submit">Импорт .db</button>
          </form>
          <button className="btn btn-outline-danger" onClick={clearDb}>Очистить базу</button>
        </div>
      </div>

      <div className="card p-3">
        <details>
          <summary className="fw-semibold">Промпты LLM</summary>
          <div className="row g-2 mt-2">
            <div className="col-md-6">
              <label className="form-label">metadata_system</label>
              <textarea className="form-control" rows={4} value={s.prompts?.metadata_system || ''} onChange={e => setS({ ...s, prompts: { ...s.prompts, metadata_system: e.target.value } })} />
            </div>
            <div className="col-md-6">
              <label className="form-label">summarize_audio_system</label>
              <textarea className="form-control" rows={4} value={s.prompts?.summarize_audio_system || ''} onChange={e => setS({ ...s, prompts: { ...s.prompts, summarize_audio_system: e.target.value } })} />
            </div>
            <div className="col-md-6">
              <label className="form-label">keywords_system</label>
              <textarea className="form-control" rows={4} value={s.prompts?.keywords_system || ''} onChange={e => setS({ ...s, prompts: { ...s.prompts, keywords_system: e.target.value } })} />
            </div>
            <div className="col-md-6">
              <label className="form-label">vision_system</label>
              <textarea className="form-control" rows={4} value={s.prompts?.vision_system || ''} onChange={e => setS({ ...s, prompts: { ...s.prompts, vision_system: e.target.value } })} />
            </div>
          </div>
        </details>
      </div>
    </div>
  )
}

function CollectionsEditor({ collections, onSaved }: { collections: Collection[]; onSaved: () => void }) {
  const [newName, setNewName] = useState('')
  const [saving, setSaving] = useState(false)
  const save = async () => {
    setSaving(true)
    try {
      const fd = new FormData()
      for (const c of collections) {
        if (c.searchable) fd.set(`search_${c.id}`, 'on')
        if (c.graphable) fd.set(`graph_${c.id}`, 'on')
      }
      if (newName.trim()) fd.set('new_name', newName.trim())
      const r = await fetch('/settings/collections', { method: 'POST', body: fd })
      if (!r.ok) alert('Ошибка сохранения коллекций')
      setNewName('')
      onSaved()
    } catch {
      alert('Ошибка сети при сохранении коллекций')
    } finally {
      setSaving(false)
    }
  }
  return (
    <div className="d-flex flex-wrap align-items-center gap-2">
      <input className="form-control" placeholder="Новая коллекция" value={newName} onChange={e => setNewName(e.target.value)} style={{ maxWidth: 320 }} />
      <button className="btn btn-outline-primary" onClick={save} disabled={saving}>{saving ? 'Сохранение…' : 'Сохранить'}</button>
    </div>
  )
}
