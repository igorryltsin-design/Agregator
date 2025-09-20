import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Link, Outlet, useLocation, useNavigate, useSearchParams } from 'react-router-dom'
import { useToasts } from '../ui/Toasts'
import { useAuth } from '../ui/Auth'

export default function App() {
  const [sp, setSp] = useSearchParams()
  const nav = useNavigate()
  const location = useLocation()
  const { user, loading, logout } = useAuth()
  const q = sp.get('q') || ''
  const [theme, setTheme] = useState<string>(()=>{
    const saved = localStorage.getItem('theme')
    if (saved) return saved
    const prefersDark = window.matchMedia && window.matchMedia('(prefers-color-scheme: dark)').matches
    return prefersDark ? 'dark' : 'light'
  })
  const searchRef = useRef<HTMLInputElement|null>(null)
  const [adminMenuOpen, setAdminMenuOpen] = useState(false)
  const [scanOpen, setScanOpen] = useState(false)
  const [scanMin, setScanMin] = useState(false)
  const [scanRunning, setScanRunning] = useState(false)
  const [scanStat, setScanStat] = useState<any>(null)
  const toasts = useToasts()
  const isAdmin = user?.role === 'admin'
  const canUseAiword = !!user?.aiword_access
  const canImport = !!user?.can_import || isAdmin
  const adminMenuRef = useRef<HTMLDivElement | null>(null)

  useEffect(() => {
    if (!loading && !user) {
      const from = location.pathname + location.search
      nav('/login', { replace: true, state: { from } })
    }
  }, [loading, user, location.pathname, location.search, nav])

  useEffect(() => {
    if (!loading && user) {
      document.documentElement.setAttribute('data-theme', theme)
      localStorage.setItem('theme', theme)
    }
  }, [theme, loading, user])

  useEffect(() => {
    if (!adminMenuOpen) return
    const onClickOutside = (event: MouseEvent) => {
      if (!adminMenuRef.current?.contains(event.target as Node)) {
        setAdminMenuOpen(false)
      }
    }
    const onKeyClose = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setAdminMenuOpen(false)
      }
    }
    document.addEventListener('mousedown', onClickOutside)
    document.addEventListener('keydown', onKeyClose)
    return () => {
      document.removeEventListener('mousedown', onClickOutside)
      document.removeEventListener('keydown', onKeyClose)
    }
  }, [adminMenuOpen])

  useEffect(() => {
    setAdminMenuOpen(false)
  }, [location.pathname, location.search])

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (e.key === '/' && !e.metaKey && !e.ctrlKey && !e.altKey) { e.preventDefault(); searchRef.current?.focus() }
      if (e.key === 'Escape') {
        if (searchRef.current === document.activeElement) searchRef.current?.blur()
        setAdminMenuOpen(false)
      }
    }
    const onScanOpen = () => setScanOpen(true)
    window.addEventListener('keydown', onKey)
    window.addEventListener('scan-open', onScanOpen as any)
    return () => {
      window.removeEventListener('keydown', onKey)
      window.removeEventListener('scan-open', onScanOpen as any)
    }
  }, [])

  useEffect(() => {
    if (!scanOpen) return
    let timer: ReturnType<typeof setTimeout> | null = null
    const poll = async () => {
      try {
        const r = await fetch('/scan/status')
        if (!r.ok) throw new Error('status')
        const j = await r.json()
        setScanRunning(!!j.running)
        setScanStat(j)
      } catch {
        // ignore
      } finally {
        timer = setTimeout(poll, 1500)
      }
    }
    poll()
    return () => { if (timer) clearTimeout(timer) }
  }, [scanOpen])

  const startScan = async () => {
    if (!isAdmin) {
      toasts.push('Недостаточно прав для запуска сканирования', 'error')
      return
    }
    try {
      await fetch('/scan/start', { method: 'POST' })
      setScanOpen(true)
      setScanMin(false)
      toasts.push('Сканирование запущено', 'success')
    } catch {
      toasts.push('Не удалось запустить сканирование', 'error')
    }
  }

  const cancelScan = async () => {
    if (!isAdmin) {
      toasts.push('Недостаточно прав', 'error')
      return
    }
    try {
      await fetch('/scan/cancel', { method: 'POST' })
      toasts.push('Сканирование остановлено', 'success')
    } catch {
      toasts.push('Ошибка отмены сканирования', 'error')
    }
  }

  const handleLogout = async () => {
    setAdminMenuOpen(false)
    await logout()
    nav('/login', { replace: true })
  }

  const adminMenuItems = useMemo(() => ([
    { to: 'settings', label: 'Настройки' },
    { to: 'users', label: 'Пользователи' },
    { to: 'admin/tasks', label: 'Задачи' },
    { to: 'admin/logs', label: 'Логи' },
    { to: 'admin/llm', label: 'LLM' },
    { to: 'admin/collections', label: 'Коллекции' },
  ]), [])

  if (loading) {
    return (
      <div className="d-flex align-items-center justify-content-center" style={{ minHeight: '100vh' }}>
        Загрузка интерфейса…
      </div>
    )
  }

  if (!user) {
    return null
  }

  return (
    <div>
      <nav className="navbar navbar-expand px-3 py-2 mb-3">
        <div className="d-flex align-items-center gap-2 w-100">
          <Link className="navbar-brand m-0" to="/" style={{ color: 'var(--text)' }}>
            <span style={{display:'inline-block', lineHeight:1.05}}>
              <div style={{fontWeight:600}}>Agregator</div>
              <div style={{fontSize:12}}>made by Ryltsin I.A.</div>
            </span>
          </Link>
          <input
            className="form-control"
            style={{ width: 600, maxWidth: '60%', flex: '0 1 auto' }}
            placeholder="Поиск…"
            value={q}
            ref={searchRef}
            onChange={e => { sp.set('q', e.target.value); sp.delete('commit'); setSp(sp, { replace: true }) }}
            onKeyDown={e => { if (e.key === 'Enter') { sp.set('commit', String(Date.now())); setSp(sp, { replace: true }) } }}
          />
          <div className="ms-auto d-flex align-items-center gap-2 flex-wrap justify-content-end" style={{ rowGap: '0.3rem' }}>
            <button className="btn btn-outline-secondary" onClick={()=> setTheme(t => t==='dark'?'light':'dark')} aria-label="Переключить тему">{theme==='dark'?'🌙':'☀️'}</button>
            <Link className="btn btn-outline-secondary" to="graph">Граф</Link>
            <Link className="btn btn-outline-secondary" to="stats">Статистика</Link>
            {canImport && <Link className="btn btn-outline-secondary" to="ingest">Импорт</Link>}
            {isAdmin && (
              <div className="position-relative" ref={adminMenuRef}>
                <button className="btn btn-outline-secondary" type="button" onClick={() => setAdminMenuOpen(v => !v)} aria-expanded={adminMenuOpen}>
                  Администрирование ▾
                </button>
                {adminMenuOpen && (
                  <div className="border rounded-3" style={{ position: 'absolute', top: 'calc(100% + 4px)', right: 0, minWidth: 200, background: 'var(--surface)', boxShadow: 'var(--card-shadow)', zIndex: 1500 }}>
                    {adminMenuItems.map(item => (
                      <Link
                        key={item.to}
                        className="d-block px-3 py-2 text-decoration-none"
                        style={{ color: 'var(--text)' }}
                        to={item.to}
                        onClick={() => setAdminMenuOpen(false)}
                      >
                        {item.label}
                      </Link>
                    ))}
                  </div>
                )}
              </div>
            )}
            <Link className="btn btn-outline-secondary" to="profile">Профиль</Link>
            <span className="badge bg-secondary text-uppercase" style={{ letterSpacing: 0.3 }}>{user.role === 'admin' ? 'Админ' : 'Пользователь'}</span>
            <button className="btn btn-outline-secondary" onClick={handleLogout}>Выход</button>
            {canUseAiword && (
              <a className="btn btn-primary" href="/aiword" target="_blank" rel="noopener">AIWord</a>
            )}
          </div>
        </div>
      </nav>
      <div className="container-fluid">
        <Outlet />
      </div>

      {scanOpen && (
        <div role="dialog" aria-modal="false" style={{position:'fixed', right:16, bottom:16, width: scanMin? 320 : 420, background:'var(--surface)', border:'1px solid var(--border)', borderRadius:12, boxShadow:'var(--card-shadow)', overflow:'hidden', zIndex:1600}}>
          <div className="d-flex align-items-center justify-content-between" style={{padding:'8px 12px', borderBottom:'1px solid var(--border)'}}>
            <div className="d-grid">
              <div>{scanRunning ? 'Сканирование…' : 'Сканер остановлен'}</div>
              {scanStat && (
                <div className="muted" style={{fontSize:12}}>
                  {(() => {
                    const p = Number(scanStat.processed||0)
                    const t = Number(scanStat.total||0)
                    const a = Number(scanStat.added||0)
                    const u = Number(scanStat.updated||0)
                    const cur = scanStat.current || ''
                    const eta = Number(scanStat.eta_seconds||0)
                    const pct = t>0 ? Math.min(100, Math.round(p*100/t)) : 0
                    const fmt = (s: number) => {
                      const hh = Math.floor(s/3600)
                      const mm = Math.floor((s%3600)/60)
                      const ss = Math.floor(s%60)
                      return `${hh.toString().padStart(2,'0')}:${mm.toString().padStart(2,'0')}:${ss.toString().padStart(2,'0')}`
                    }
                    return `Прогресс: ${p}/${t} (${pct}%), добавлено ${a}, обновлено ${u}${cur? `, текущий: ${cur}` : ''}${eta? `, ETA: ${fmt(eta)}` : ''}`
                  })()}
                </div>
              )}
            </div>
            <div className="btn-group">
              <button className="btn btn-sm btn-outline-secondary" onClick={()=>setScanMin(v=>!v)}>{scanMin? 'Развернуть':'Свернуть'}</button>
              <button className="btn btn-sm btn-outline-secondary" onClick={()=>setScanOpen(false)}>Закрыть</button>
            </div>
          </div>
          {scanStat && !scanMin && (
            <div style={{padding:'12px'}}>
              <div style={{height:6, background:'var(--border)', borderRadius:6, overflow:'hidden', marginBottom:8}}>
                {(() => {
                  const p = Number(scanStat.processed||0)
                  const t = Number(scanStat.total||0)
                  const pct = t>0 ? Math.min(100, Math.round(p*100/t)) : 0
                  return <div style={{width:`${pct}%`, height:'100%', background:'var(--accent)'}} />
                })()}
              </div>
              {scanStat?.current && <div className="muted" style={{fontSize:12}}>Текущий файл: {scanStat.current}</div>}
            </div>
          )}
          {!scanMin && isAdmin && (
            <div className="d-flex gap-2" style={{padding:'0 12px 12px 12px'}}>
              <button className="btn btn-sm btn-outline-secondary" disabled={scanRunning} onClick={startScan}>Запустить</button>
              <button className="btn btn-sm btn-outline-secondary" disabled={!scanRunning} onClick={cancelScan}>Остановить</button>
            </div>
          )}
        </div>
      )}

    </div>
  )
}
