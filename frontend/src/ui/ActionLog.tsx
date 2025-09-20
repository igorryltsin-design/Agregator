import React, { createContext, useCallback, useContext, useMemo, useState } from 'react'

type LogItem = { id: number; level: 'info'|'error'|'success'; text: string; t: number }
type Ctx = { push: (text: string, level?: LogItem['level']) => void }

const ActionLogCtx = createContext<Ctx>({ push: () => {} })
export function useActionLog(){ return useContext(ActionLogCtx) }

export default function ActionLogProvider({ children }: { children: React.ReactNode }){
  const [items, setItems] = useState<LogItem[]>([])
  const [open, setOpen] = useState(false)
  const push = useCallback((text: string, level: LogItem['level']='info') => {
    const id = Date.now() + Math.floor(Math.random()*1000)
    const t = Date.now()
    setItems(arr => [...arr.slice(-500), { id, level, text, t }])
    setOpen(true)
  }, [])
  const ctx = useMemo(() => ({ push }), [push])
  return (
    <ActionLogCtx.Provider value={ctx}>
      {children}
      <div style={{position:'fixed', left:16, bottom:16, zIndex:1550}}>
        <div className="card" style={{width: open? 420 : 'auto', maxWidth: '90vw'}}>
          <div className="d-flex align-items-center justify-content-between" style={{padding:'6px 10px', borderBottom:'1px solid var(--border)'}}>
            <div className="fw-semibold">Лог действий</div>
            <div className="btn-group">
              <button className="btn btn-sm btn-outline-secondary" onClick={()=>setOpen(o=>!o)}>{open? 'Свернуть':'Развернуть'}</button>
              <button className="btn btn-sm btn-outline-secondary" onClick={()=>setItems([])}>Очистить</button>
            </div>
          </div>
          {open && (
            <div style={{padding:10, maxHeight: 260, overflow:'auto', fontFamily:'ui-monospace, SFMono-Regular, Menlo, monospace', fontSize:12}}>
              {items.map(it => (
                <div key={it.id}>
                  <span className="muted">{new Date(it.t).toLocaleTimeString()} </span>
                  <span>[{it.level.toUpperCase()}]</span>
                  <span> {it.text}</span>
                </div>
              ))}
            </div>
          )}
        </div>
      </div>
    </ActionLogCtx.Provider>
  )
}

