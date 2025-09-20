import React, { useEffect, useMemo, useRef, useState } from 'react'
import { Network, DataSet } from 'vis-network/standalone'

type Node = { id: string; label: string; type: string }
type Edge = { from: string; to: string; label?: string }

export default function GraphPage() {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [keys, setKeys] = useState<string[]>(['author'])
  const [availableKeys, setAvailableKeys] = useState<string[]>([])
  const [view, setView] = useState<'graph'|'cloud'>('graph')
  const [facetData, setFacetData] = useState<Record<string,[string,number][]>>({})
  const networkRef = useRef<Network | null>(null)
  const nodesRef = useRef<DataSet<any> | null>(null)
  const edgesRef = useRef<DataSet<any> | null>(null)
  const [minDegree, setMinDegree] = useState(1)
  const [limit, setLimit] = useState(600)
  const [hideSingletons, setHideSingletons] = useState(true)
  const [selection, setSelection] = useState<{id:string,label:string,type:string,degree:number}|null>(null)
  const [search, setSearch] = useState('')
  const [smartSearch, setSmartSearch] = useState(true)
  const [yearFrom, setYearFrom] = useState('')
  const [yearTo, setYearTo] = useState('')

  useEffect(() => {
    const run = async () => {
      setLoading(true)
      setError(null)
      try {
        const qs = new URLSearchParams()
        qs.set('keys', keys.join(','))
        qs.set('limit', String(limit))
        if (yearFrom) qs.set('year_from', yearFrom)
        if (yearTo) qs.set('year_to', yearTo)
        if (smartSearch && search.trim()) {
          qs.set('q', search.trim())
          qs.set('smart', '1')
        }
        const res = await fetch(`/api/graph?${qs.toString()}`)
        const data: { nodes: Node[]; edges: Edge[] } = await res.json()
        // degree filter (client)
        const deg: Record<string, number> = {}
        data.edges.forEach(e => { deg[e.from]=(deg[e.from]||0)+1; deg[e.to]=(deg[e.to]||0)+1 })
        let nodesFiltered = data.nodes.filter(n => (deg[n.id]||0) >= (hideSingletons ? Math.max(2, minDegree) : minDegree))
        if (search.trim() && !smartSearch) {
          const s = search.trim().toLowerCase()
          nodesFiltered = nodesFiltered.filter(n => (n.label||'').toLowerCase().includes(s))
        }
        const nodes = new DataSet(nodesFiltered.map(n => ({ ...n, color: (n as any).type?.startsWith('tag:') ? '#1f6feb' : '#2ea043' })))
        const nodeSet = new Set(nodes.getIds() as string[])
        const edges = new DataSet(data.edges.filter(e => nodeSet.has(e.from) && nodeSet.has(e.to)).map(e => ({ ...e, arrows: 'to', color: '#8b949e' })))
        const network = new Network(containerRef.current as HTMLDivElement, { nodes, edges }, {
          physics: { stabilization: true, barnesHut: { gravitationalConstant: -30000, centralGravity: 0.3 } },
          interaction: { hover: true, tooltipDelay: 120 },
          nodes: { shape: 'dot', size: 10, font: { color: '#c9d1d9' } },
          edges: { smooth: true }
        })
        networkRef.current = network; nodesRef.current = nodes; edgesRef.current = edges
        network.on('selectNode', (params:any) => {
          const id = (params.nodes||[])[0];
          highlight(id)
        })
        network.on('deselectNode', () => { clearHighlight() })
      } catch (e: any) {
        setError(String(e))
      } finally { setLoading(false) }
    }
    if (view === 'graph') run()
    // Cleanup created network and datasets on dependency change
    return () => {
      try { networkRef.current?.destroy() } catch {}
      try { nodesRef.current?.clear() } catch {}
      try { edgesRef.current?.clear() } catch {}
      networkRef.current = null; nodesRef.current = null; edgesRef.current = null
    }
  }, [view, keys, minDegree, hideSingletons, limit, search, smartSearch, yearFrom, yearTo])

  // Load facets once to discover tag keys and counts for cloud
  useEffect(() => {
    const load = async () => {
      try {
        const res = await fetch('/api/facets')
        const data = await res.json()
        const keys = Object.keys(data.tag_facets || {})
        setAvailableKeys(keys)
        setFacetData(data.tag_facets || {})
        if (!keys.includes('author') && keys.length) setKeys([keys[0]])
      } catch {}
    }
    load()
  }, [])

  function clearHighlight(){
    setSelection(null)
    const nodes = nodesRef.current; if (!nodes) return
    const all = nodes.get()
    nodes.update(all.map((n:any) => ({ id: n.id, hidden: false, color: n.type?.startsWith('tag:') ? '#1f6feb' : '#2ea043' })))
  }

  function highlight(id: string){
    const net = networkRef.current, nodes = nodesRef.current, edges = edgesRef.current
    if (!net || !nodes || !edges) return
    const neigh = new Set<string>([id, ...net.getConnectedNodes(id) as string[]])
    const degree = (net.getConnectedEdges(id) as string[]).length
    const node = nodes.get(id)
    setSelection({ id, label: node?.label || '', type: node?.type || '', degree })
    const all = nodes.get()
    nodes.update(all.map((n:any) => ({ id: n.id, hidden: !neigh.has(n.id) })))
  }

  const cloud = useMemo(() => {
    const key = keys[0]
    const list = (facetData[key] || []).slice(0, 80)
    if (!list.length) return null
    const counts = list.map(([,c]) => c)
    const min = Math.min(...counts), max = Math.max(...counts)
    const scale = (c: number) => {
      if (max === min) return 14
      const t = (c - min) / (max - min)
      return 12 + Math.round(18 * Math.pow(t, 0.7))
    }
    return { key, list, min, max, scale }
  }, [keys, facetData])

  return (
    <div className="row g-3">
      <div className="col-12 col-lg-3">
        <div className="card p-3">
          <div className="fw-semibold mb-2">Вид</div>
          <div className="btn-group mb-3">
            <button className={`btn btn-sm btn-outline-secondary ${view==='graph'?'active':''}`} onClick={()=>setView('graph')}>Граф</button>
            <button className={`btn btn-sm btn-outline-secondary ${view==='cloud'?'active':''}`} onClick={()=>setView('cloud')}>Облако тегов</button>
          </div>
          <div className="fw-semibold mb-2">Ключи тегов</div>
          <div className="d-flex flex-wrap gap-2">
            {availableKeys.map(k => (
              <button key={k} className={`btn btn-sm ${keys.includes(k)?'btn-secondary':'btn-outline-secondary'}`} onClick={()=>{
                setKeys(prev => prev.includes(k) ? prev.filter(x=>x!==k) : [...prev.slice(0,3), k])
              }}>{k}</button>
            ))}
          </div>
          <hr/>
          <div className="fw-semibold mb-2">Параметры</div>
          <input className="form-control mb-2" placeholder="Поиск узлов/тегов" value={search} onChange={e=>setSearch(e.target.value)} />
          <div className="form-check mb-2">
            <input id="smartSearch" className="form-check-input" type="checkbox" checked={smartSearch} onChange={e=>setSmartSearch(e.target.checked)} />
            <label className="form-check-label" htmlFor="smartSearch">Синонимы и словоформы</label>
          </div>
          <div className="d-flex gap-2 mb-2">
            <input className="form-control" placeholder="Год с" value={yearFrom} onChange={e=>setYearFrom(e.target.value)} />
            <input className="form-control" placeholder="Год по" value={yearTo} onChange={e=>setYearTo(e.target.value)} />
          </div>
          <label className="form-label">Макс. узлов: {limit}</label>
          <input type="range" min={100} max={2000} step={50} value={limit} onChange={e=>setLimit(parseInt(e.target.value))} className="form-range" />
          <label className="form-label">Мин. степень: {minDegree}</label>
          <input type="range" min={1} max={10} step={1} value={minDegree} onChange={e=>setMinDegree(parseInt(e.target.value))} className="form-range" />
          <div className="form-check">
            <input className="form-check-input" type="checkbox" id="hideSingles" checked={hideSingletons} onChange={e=>setHideSingletons(e.target.checked)} />
            <label htmlFor="hideSingles" className="form-check-label">Скрывать одиночные теги</label>
          </div>
          <hr/>
          <div className="fw-semibold mb-2">Легенда</div>
          <div className="text-secondary small">Зелёные — работы, Синие — теги. Размер узлов пропорционален степени (можно увеличить/уменьшить масштаб колесом мыши).</div>
          {cloud && (
            <div className="mt-2 text-secondary small">Размер шрифта ~ частотность [{cloud.min}…{cloud.max}]</div>
          )}
          <hr/>
          <button className="btn btn-sm btn-outline-secondary" onClick={()=>{
            const net = networkRef.current as any
            if (!net || !net.canvas || !net.canvas.frame || !net.canvas.frame.canvas) return
            const url = net.canvas.frame.canvas.toDataURL('image/png')
            const a = document.createElement('a'); a.href = url; a.download = 'graph.png'; a.click()
          }}>Экспорт PNG</button>
        </div>
      </div>
      <div className="col-12 col-lg-9">
        {view === 'graph' && (
          <div className="card p-3">
            <div className="fw-semibold mb-2">Граф (файл → теги: {keys.join(', ') || '—'})</div>
            {loading && <div>Загрузка…</div>}
            {error && <div className="text-danger">{error}</div>}
            <div ref={containerRef} style={{ height: '80vh', background: 'var(--surface)', border: '1px solid var(--border)', borderRadius: 6 }} />
          </div>
        )}
        {selection && (
          <div className="card p-3 mt-3" aria-live="polite">
            <div className="fw-semibold mb-2">Выделено</div>
            <div><strong>Метка:</strong> {selection.label}</div>
            <div><strong>Тип:</strong> {selection.type}</div>
            <div><strong>Степень:</strong> {selection.degree}</div>
            <div className="mt-2"><button className="btn btn-sm btn-outline-secondary" onClick={clearHighlight}>Сбросить</button></div>
          </div>
        )}
        {view === 'cloud' && cloud && (
          <div className="card p-3">
            <div className="fw-semibold mb-2">Облако тегов: {cloud.key}</div>
            <div style={{minHeight: '50vh'}}>
              {cloud.list.map(([val, cnt], i) => (
                <span key={i} className="me-2" style={{ fontSize: cloud.scale(cnt), lineHeight: '2.2rem' }} title={`${val} (${cnt})`}>{val}</span>
              ))}
            </div>
          </div>
        )}
        {view === 'cloud' && !cloud && (
          <div className="text-secondary">Недостаточно данных для облака</div>
        )}
      </div>
    </div>
  )
}
