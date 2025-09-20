import React, { createContext, useCallback, useContext, useMemo, useState } from 'react'

type Toast = { id: number; kind?: 'success'|'error'|'info'; text: string }
type Ctx = { push: (text: string, kind?: Toast['kind']) => void }
const ToastCtx = createContext<Ctx>({ push: () => {} })

export function useToasts(){ return useContext(ToastCtx) }

export default function ToastProvider({ children }: { children: React.ReactNode }){
  const [items, setItems] = useState<Toast[]>([])
  const push = useCallback((text: string, kind: Toast['kind']='info') => {
    const id = Date.now() + Math.floor(Math.random()*1000)
    setItems(arr => [...arr, { id, kind, text }])
    setTimeout(() => setItems(arr => arr.filter(x => x.id !== id)), 4000)
  }, [])
  const ctx = useMemo(() => ({ push }), [push])
  return (
    <ToastCtx.Provider value={ctx}>
      {children}
      <div style={{position:'fixed', right:16, bottom:16, display:'grid', gap:8, zIndex:2000}}>
        {items.map(t => (
          <div key={t.id} role="status" style={{padding:'8px 12px', border:'1px solid var(--border)', background:'var(--surface)', color:'var(--text)', borderRadius:8, minWidth:240, boxShadow:'var(--card-shadow)'}}>
            <strong style={{marginRight:8}}>{t.kind==='error'?'Ошибка': t.kind==='success'?'Готово':'Сообщение'}</strong>
            <span>{t.text}</span>
          </div>
        ))}
      </div>
    </ToastCtx.Provider>
  )
}

