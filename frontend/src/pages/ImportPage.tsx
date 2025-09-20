import React, { useEffect, useState } from 'react'
import { useAuth } from '../ui/Auth'

type Collection = { id: number; name: string }

export default function ImportPage(){
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState('')
  const [extract, setExtract] = useState(true)
  const [useLLM, setUseLLM] = useState(false)
  const [prune, setPrune] = useState(true)
  const [collections, setCollections] = useState<Collection[]>([])
  const [collectionId, setCollectionId] = useState<string>('')
  const [newCollection, setNewCollection] = useState<string>('')
  const [isPrivate, setIsPrivate] = useState(!isAdmin)

  useEffect(()=>{
    (async()=>{
      try{
        const r = await fetch('/import')
        if (!r.ok) return
        const data = await r.json().catch(()=>({}))
        const cols: Collection[] = Array.isArray(data.collections) ? data.collections : []
        setCollections(cols)
        if (cols[0]) setCollectionId(String(cols[0].id))
      }catch{}
    })()
  }, [])
  const submit = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); setBusy(true); setMsg('')
    try{
      const fd = new FormData(e.currentTarget)
      if (extract) fd.set('extract_text','on')
      if (useLLM) fd.set('use_llm','on')
      if (prune) fd.set('prune','on')
      if (collectionId) fd.set('collection_id', collectionId)
      if (newCollection.trim()) {
        fd.set('new_collection', newCollection.trim())
        if (isPrivate || !isAdmin) fd.set('private','on')
      }
      if (!isAdmin) {
        fd.delete('start_scan')
      }
      const r = await fetch('/import', { method:'POST', body: fd })
      setMsg(r.ok ? 'Файлы загружены. Сканирование запустится при выборе опции.' : 'Ошибка импорта')
    } finally { setBusy(false) }
  }

  return (
    <div className="card p-3">
      <div className="fw-semibold mb-2">Импорт файлов/папок</div>
      <form onSubmit={submit}>
        <input className="form-control" type="file" name="files" multiple {...{ webkitdirectory: '' as any, directory: '' as any }} />
        <div className="row g-2 mt-2">
          <div className="col-md-6">
            <label className="form-label">Коллекция</label>
            <select className="form-select" value={collectionId} onChange={e=>setCollectionId(e.target.value)}>
              {collections.map(c=> <option key={c.id} value={c.id}>{c.name}</option>)}
            </select>
          </div>
          <div className="col-md-6">
            <label className="form-label">Новая коллекция</label>
            <input className="form-control" placeholder="или новая…" value={newCollection} onChange={e=>setNewCollection(e.target.value)} />
          </div>
        </div>
        <div className="row g-2 mt-2">
          {newCollection.trim().length > 0 && (
            <div className="col-auto form-check form-switch">
              <input className="form-check-input" type="checkbox" id="priv" checked={isPrivate || !isAdmin} disabled={!isAdmin} onChange={e=>setIsPrivate(e.target.checked)} />
              <label className="form-check-label" htmlFor="priv">Приватная коллекция</label>
            </div>
          )}
        </div>
        {isAdmin && (
          <div className="form-check form-switch mt-3">
            <input className="form-check-input" type="checkbox" id="st" name="start_scan" defaultChecked />
            <label className="form-check-label" htmlFor="st">Запустить сканирование</label>
          </div>
        )}
        <div className="row g-2 mt-1">
          <div className="col-auto form-check form-switch">
            <input className="form-check-input" type="checkbox" id="ext" checked={extract} onChange={e=>setExtract(e.target.checked)} />
            <label className="form-check-label" htmlFor="ext">Извлекать текст</label>
          </div>
          <div className="col-auto form-check form-switch">
            <input className="form-check-input" type="checkbox" id="llm" checked={useLLM} onChange={e=>setUseLLM(e.target.checked)} />
            <label className="form-check-label" htmlFor="llm">Использовать LLM</label>
          </div>
          <div className="col-auto form-check form-switch">
            <input className="form-check-input" type="checkbox" id="pr" checked={prune} onChange={e=>setPrune(e.target.checked)} />
            <label className="form-check-label" htmlFor="pr">Удалять отсутствующие</label>
          </div>
        </div>
        <div className="mt-3"><button className="btn btn-primary" type="submit" disabled={busy}>{busy?'Отправка…':'Импортировать'}</button></div>
        {!isAdmin && <div className="text-muted mt-2" style={{fontSize:12}}>Сканирование файлов выполняет администратор. После импорта он получит уведомление.</div>}
        {msg && <div className="mt-2">{msg}</div>}
      </form>
    </div>
  )
}
