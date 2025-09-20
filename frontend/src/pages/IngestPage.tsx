import React, { useEffect, useState } from 'react'
import { useAuth } from '../ui/Auth'

type Collection = { id: number; name: string }

export default function IngestPage(){
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'
  const canImport = isAdmin || !!user?.can_import
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState('')
  const [extract, setExtract] = useState(true)
  const [useLLM, setUseLLM] = useState(false)
  const [prune, setPrune] = useState(true)
  const [collections, setCollections] = useState<Collection[]>([])
  const [collectionId, setCollectionId] = useState<string>('')
  const [newCollection, setNewCollection] = useState<string>('')
  useEffect(()=>{
    if (!canImport) return
    (async()=>{
      try{
        const r=await fetch('/import')
        if (!r.ok) return
        const j=await r.json()
        const cols: Collection[] = Array.isArray(j.collections) ? j.collections : []
        setCollections(cols)
        if (cols[0]) setCollectionId(String(cols[0].id))
      }catch{}
    })()
  }, [canImport])

  if (!canImport) return <div className="card p-3">Недостаточно прав.</div>

  const importMany = async (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault(); setBusy(true); setMsg('')
    try{
      const fd = new FormData(e.currentTarget)
      fd.set('start_scan','on')
      if (extract) fd.set('extract_text','on')
      if (useLLM) fd.set('use_llm','on')
      if (prune) fd.set('prune','on')
      if (collectionId) fd.set('collection_id', collectionId)
      if (newCollection.trim()) fd.set('new_collection', newCollection.trim())
      const r = await fetch('/import', { method:'POST', body: fd })
      const ok = r.ok
      setMsg(ok ? 'Файлы загружены, сканирование запущено.' : 'Ошибка импорта')
    } finally { setBusy(false) }
  }

  return (
    <div className="row g-3">
      <div className="col-12">
        <div className="card p-3">
          <div className="fw-semibold mb-2">Импорт папки/файлов</div>
          <form onSubmit={importMany}>
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
            <div className="mt-2"><button className="btn btn-primary" type="submit" disabled={busy}>{busy?'Отправка…':'Импортировать'}</button></div>
          </form>
        </div>
      </div>
      {msg && <div className="col-12"><div className="card p-3">{msg}</div></div>}
    </div>
  )
}
