import React, { useEffect, useState } from 'react'
import { useAuth } from '../ui/Auth'

type Collection = { id: number; name: string }

export default function UploadPage(){
  const { user } = useAuth()
  const isAdmin = user?.role === 'admin'
  const [file, setFile] = useState<File|null>(null)
  const [busy, setBusy] = useState(false)
  const [msg, setMsg] = useState<string>('')
  const [collections, setCollections] = useState<Collection[]>([])
  const [collectionId, setCollectionId] = useState<string>('')
  const [newCollection, setNewCollection] = useState<string>('')
  const [isPrivate, setIsPrivate] = useState(!isAdmin)
  useEffect(()=>{
    (async()=>{
      try{
        const r=await fetch('/upload')
        if (!r.ok) return
        const j=await r.json()
        const cols: Collection[] = Array.isArray(j.collections) ? j.collections : []
        setCollections(cols)
        if(cols[0]) setCollectionId(String(cols[0].id))
      }catch{}
    })()
  }, [])
  const submit = async () => {
    if(!file) return
    setBusy(true); setMsg('')
    try{
      const fd = new FormData(); fd.append('file', file)
      if (collectionId) fd.append('collection_id', collectionId)
      if (newCollection.trim()) {
        fd.append('new_collection', newCollection.trim())
        if (isPrivate || !isAdmin) fd.append('private','on')
      }
      const r = await fetch('/upload', { method:'POST', body: fd })
      if (r.ok) setMsg('Загружено. Откройте каталог для сканирования.')
      else setMsg('Ошибка загрузки')
    }finally{ setBusy(false) }
  }

  return (
    <div className="card p-3">
      <div className="fw-semibold mb-2">Загрузка файла</div>
      <input className="form-control" type="file" onChange={e=>setFile(e.target.files?.[0] || null)} />
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
      {newCollection.trim().length > 0 && (
        <div className="form-check form-switch mt-2">
          <input className="form-check-input" type="checkbox" id="priv-upload" checked={isPrivate || !isAdmin} disabled={!isAdmin} onChange={e=>setIsPrivate(e.target.checked)} />
          <label className="form-check-label" htmlFor="priv-upload">Приватная коллекция</label>
        </div>
      )}
      <div className="mt-2"><button className="btn btn-primary" onClick={submit} disabled={!file||busy}>{busy?'Загрузка…':'Загрузить'}</button></div>
      {!isAdmin && <div className="text-muted mt-2" style={{fontSize:12}}>Файлы появятся в вашей личной коллекции или выбранной доступной подборке.</div>}
      {msg && <div className="mt-2">{msg}</div>}
    </div>
  )
}
