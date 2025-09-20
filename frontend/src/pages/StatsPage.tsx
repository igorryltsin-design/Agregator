import React, { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { Chart, BarController, BarElement, CategoryScale, LinearScale, LogarithmicScale, LineController, LineElement, PointElement, ArcElement, Tooltip, Legend, DoughnutController } from 'chart.js'
Chart.register(BarController, BarElement, CategoryScale, LinearScale, LogarithmicScale, LineController, LineElement, PointElement, ArcElement, DoughnutController, Tooltip, Legend)

type StatKV = [string, number]
type StatsResponse = {
  authors: StatKV[]
  authors_cloud: StatKV[]
  years: StatKV[]
  types: StatKV[]
  exts: StatKV[]
  sizes: StatKV[]
  months: StatKV[]
  top_keywords: StatKV[]
  tag_keys: StatKV[]
  tag_values_cloud: StatKV[]
  weekdays: StatKV[]
  hours: StatKV[]
  avg_size_type: StatKV[]
  meta_presence: StatKV[]
  total_files: number
  total_size_bytes: number
  collections_counts?: StatKV[]
  collections_total_size?: [string, number][]
  largest_files?: [string, number][]
}

export default function StatsPage(){
  const nav = useNavigate()
  const [data, setData] = useState<StatsResponse|null>(null)
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState<string| null>(null)
  const [rangeMonths, setRangeMonths] = useState<number>(24)
  const [logScale, setLogScale] = useState<boolean>(false)
  const [cumulativeMonths, setCumulativeMonths] = useState<boolean>(false)
  const refTypes = useRef<HTMLCanvasElement|null>(null)
  const refMonths = useRef<HTMLCanvasElement|null>(null)
  const refYears = useRef<HTMLCanvasElement|null>(null)
  const refSizes = useRef<HTMLCanvasElement|null>(null)
  const refExts = useRef<HTMLCanvasElement|null>(null)
  const refWeekdays = useRef<HTMLCanvasElement|null>(null)
  const refHours = useRef<HTMLCanvasElement|null>(null)
  const refMeta = useRef<HTMLCanvasElement|null>(null)
  const refAvgSize = useRef<HTMLCanvasElement|null>(null)
  const refCollections = useRef<HTMLCanvasElement|null>(null)
  const refCollSize = useRef<HTMLCanvasElement|null>(null)
  const refLargest = useRef<HTMLCanvasElement|null>(null)
  const refAuthorsBar = useRef<HTMLCanvasElement|null>(null)
  const chartTypes = useRef<Chart | null>(null)
  const chartMonths = useRef<Chart | null>(null)
  const chartYears = useRef<Chart | null>(null)
  const chartSizes = useRef<Chart | null>(null)
  const chartExts = useRef<Chart | null>(null)
  const chartWeekdays = useRef<Chart | null>(null)
  const chartHours = useRef<Chart | null>(null)
  const chartMeta = useRef<Chart | null>(null)
  const chartAvg = useRef<Chart | null>(null)
  const chartCollections = useRef<Chart | null>(null)
  const chartCollSize = useRef<Chart | null>(null)
  const chartLargest = useRef<Chart | null>(null)
  const refTagKeys = useRef<HTMLCanvasElement|null>(null)
  const refTagVals = useRef<HTMLCanvasElement|null>(null)
  const chartTagKeys = useRef<Chart | null>(null)
  const chartTagVals = useRef<Chart | null>(null)
  const [selectedKey, setSelectedKey] = useState<string>('')
  const [tagVals, setTagVals] = useState<StatKV[]>([])
  const chartAuthorsBar = useRef<Chart | null>(null)
  useEffect(() => {
    const ac = new AbortController()
    ;(async ()=>{
      setLoading(true); setError(null)
      try{
        const r=await fetch('/api/stats', { signal: ac.signal })
        if (!r.ok) throw new Error('Ошибка загрузки статистики')
        const j: StatsResponse = await r.json(); setData(j)
      } catch (e:any) {
        if (e?.name !== 'AbortError') setError(String(e?.message||e))
      } finally{ setLoading(false) }
    })()
    return () => ac.abort()
  }, [])

  // palette from CSS variables
  const palette = useMemo(() => {
    const css = getComputedStyle(document.documentElement)
    return {
      primary: css.getPropertyValue('--primary-2').trim() || '#1f6feb',
      green: css.getPropertyValue('--primary-2').trim() || '#2ea043',
      accent: css.getPropertyValue('--accent').trim() || '#58a6ff',
      muted: css.getPropertyValue('--muted').trim() || '#8b949e',
      border: css.getPropertyValue('--border').trim() || '#30363d',
      gray: '#6e7781',
      yellow: '#e0a800',
      purple: '#8957e5',
      red: '#d73a49',
    }
  }, [document.documentElement.getAttribute('data-theme')])

  const commonOptions: any = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins:{ legend:{ display:false }, tooltip: { enabled: true } },
    scales:{ y:{ beginAtZero:true, type: logScale? 'logarithmic' : 'linear', grid:{ color: palette.border } }, x:{ grid:{ color: palette.border } } }
  }), [palette, logScale])
  const hbarOptions: any = useMemo(() => ({
    responsive: true,
    maintainAspectRatio: false,
    plugins:{ legend:{ display:false }, tooltip: { enabled: true } },
    scales:{ x:{ beginAtZero:true, type: logScale? 'logarithmic' : 'linear', grid:{ color: palette.border } }, y:{ grid:{ color: palette.border } } }
  }), [palette, logScale])
  useEffect(() => {
    if (!data) return
    const types = data.types || []
    if (refTypes.current) {
      chartTypes.current?.destroy()
      chartTypes.current = new Chart(refTypes.current, {
        type: 'bar',
        data: { labels: types.map((x:any)=>x[0]), datasets:[{ label:'По типам', data: types.map((x:any)=>x[1]), backgroundColor: palette.primary }] },
        options: {
          ...commonOptions,
          onClick: (_evt: any, els: any[]) => { if(!els?.length) return; const i=els[0].index; const label = (chartTypes.current as any)?.data?.labels?.[i]; if (label) nav(`/?type=${encodeURIComponent(String(label))}`) }
        }
      })
    }
    const monthsRaw = (data.months||[]).slice(rangeMonths>0? -rangeMonths: undefined)
    const months = cumulativeMonths ? (function(){
      let acc = 0
      return monthsRaw.map(([m,v]: any) => { acc += Number(v||0); return [m, acc] as any })
    })() : monthsRaw
    if (refMonths.current) {
      chartMonths.current?.destroy()
      chartMonths.current = new Chart(refMonths.current, {
        type:'line',
        data:{ labels: months.map((x:any)=>x[0]), datasets:[{ label:'По месяцам', data: months.map((x:any)=>x[1]), borderColor: palette.green, backgroundColor:'rgba(46,160,67,0.3)', tension: 0.2, fill: true }] },
        options:{ ...commonOptions }
      })
    }
    const years = data.years || []
    if (refYears.current) {
      chartYears.current?.destroy()
      chartYears.current = new Chart(refYears.current, {
        type:'bar',
        data:{ labels: years.map((x:any)=>x[0]), datasets:[{ label:'По годам', data: years.map((x:any)=>x[1]), backgroundColor: palette.gray }] },
        options:{ ...commonOptions }
      })
    }
    const sizes = data.sizes || []
    if (refSizes.current) {
      chartSizes.current?.destroy()
      chartSizes.current = new Chart(refSizes.current, {
        type:'doughnut',
        data:{ labels: sizes.map((x:any)=>x[0]), datasets:[{ data: sizes.map((x:any)=>x[1]), backgroundColor:[palette.green,palette.primary,palette.yellow,palette.purple,palette.red,palette.gray] }] },
        options:{
          cutout: '60%',
          plugins:{
            legend:{position:'bottom'},
            tooltip: {
              callbacks: {
                label: (ctx: any) => {
                  const total = (ctx.dataset.data||[]).reduce((a:number,b:number)=>a+b,0) || 1
                  const v = ctx.raw as number; const p = Math.round((v*100)/total)
                  return `${ctx.label}: ${v} (${p}%)`
                }
              }
            }
          },
          maintainAspectRatio:false
        }
      })
    }
    const exts = data.exts || []
    if (refExts.current) {
      chartExts.current?.destroy()
      chartExts.current = new Chart(refExts.current, {
        type:'bar',
        data:{ labels: exts.map((x:any)=>x[0]), datasets:[{ label:'По расширениям', data: exts.map((x:any)=>x[1]), backgroundColor: palette.accent }] },
        options:{ ...commonOptions }
      })
    }
    const weekdays = data.weekdays || []
    if (refWeekdays.current) {
      chartWeekdays.current?.destroy()
      chartWeekdays.current = new Chart(refWeekdays.current, {
        type:'bar',
        data:{ labels: weekdays.map((x:any)=>x[0]), datasets:[{ label:'Дни недели', data: weekdays.map((x:any)=>x[1]), backgroundColor: palette.purple }] },
        options:{ ...commonOptions,
          onClick: (_evt:any, els:any[]) => { if(!els?.length) return; const i=els[0].index; const label=(chartWeekdays.current as any)?.data?.labels?.[i]; if(label) nav(`/?q=${encodeURIComponent('weekday:'+label)}`) }
        }
      })
    }
    const hours = data.hours || []
    if (refHours.current) {
      chartHours.current?.destroy()
      chartHours.current = new Chart(refHours.current, {
        type:'bar',
        data:{ labels: hours.map((x:any)=>x[0]), datasets:[{ label:'Часы', data: hours.map((x:any)=>x[1]), backgroundColor: palette.yellow }] },
        options:{ ...commonOptions,
          onClick: (_evt:any, els:any[]) => { if(!els?.length) return; const i=els[0].index; const label=(chartHours.current as any)?.data?.labels?.[i]; if(label) nav(`/?q=${encodeURIComponent('hour:'+label)}`) }
        }
      })
    }
    const meta = data.meta_presence || []
    if (refMeta.current) {
      chartMeta.current?.destroy()
      chartMeta.current = new Chart(refMeta.current, {
        type:'bar',
        data:{ labels: meta.map((x:any)=>x[0]), datasets:[{ label:'Заполненность метаданных', data: meta.map((x:any)=>x[1]), backgroundColor: palette.gray }] },
        options:{ ...hbarOptions, indexAxis:'y' }
      })
    }
    const avg = data.avg_size_type || []
    if (refAvgSize.current) {
      chartAvg.current?.destroy()
      chartAvg.current = new Chart(refAvgSize.current, {
        type:'bar',
        data:{ labels: avg.map((x:any)=>x[0]), datasets:[{ label:'Средний размер (МБ)', data: avg.map((x:any)=>x[1]), backgroundColor: palette.green }] },
        options:{ ...commonOptions }
      })
    }
    // Files per collection
    const coll = data.collections_counts || []
    if (refCollections.current) {
      chartCollections.current?.destroy()
      chartCollections.current = new Chart(refCollections.current, {
        type:'bar',
        data:{ labels: coll.map((x:any)=>x[0]), datasets:[{ label:'По коллекциям', data: coll.map((x:any)=>x[1]), backgroundColor: palette.accent }] },
        options:{ ...hbarOptions, indexAxis:'y' }
      })
    }
    // Collections total size (MB)
    const collSize = (data.collections_total_size||[]).map(([name, bytes]) => [name, (Number(bytes||0)/(1024*1024))] as any)
    if (refCollSize.current) {
      chartCollSize.current?.destroy()
      chartCollSize.current = new Chart(refCollSize.current, {
        type:'bar',
        data:{ labels: collSize.map((x:any)=>x[0]), datasets:[{ label:'Объём (МБ)', data: collSize.map((x:any)=>Math.round(Number(x[1])*10)/10), backgroundColor: palette.primary }] },
        options:{ ...hbarOptions, indexAxis:'y' }
      })
    }
    // Largest files (MB)
    const largest = (data.largest_files||[]).map(([label, bytes]) => [label, (Number(bytes||0)/(1024*1024))] as any)
    if (refLargest.current) {
      chartLargest.current?.destroy()
      chartLargest.current = new Chart(refLargest.current, {
        type:'bar',
        data:{ labels: largest.map((x:any)=>x[0]), datasets:[{ label:'Размер (МБ)', data: largest.map((x:any)=>Math.round(Number(x[1])*10)/10), backgroundColor: palette.red }] },
        options:{ ...hbarOptions, indexAxis:'y' }
      })
    }
    // Tag keys
    const tagKeys = data.tag_keys || []
    if (!selectedKey && tagKeys.length>0) setSelectedKey(String(tagKeys[0][0]||''))
    if (refTagKeys.current) {
      chartTagKeys.current?.destroy()
      chartTagKeys.current = new Chart(refTagKeys.current, {
        type:'bar', data:{ labels: tagKeys.map((x:any)=>x[0]), datasets:[{ label:'Ключи тегов', data: tagKeys.map((x:any)=>x[1]), backgroundColor: palette.gray }] },
        options:{ ...hbarOptions, indexAxis:'y', onClick: (_evt:any, els:any[])=>{ if(!els?.length) return; const i=els[0].index; const label = (chartTagKeys.current as any)?.data?.labels?.[i]; if (label) setSelectedKey(String(label)) } }
      })
    }
    // Top authors bar (horizontal)
    const authors = data.authors || []
    if (refAuthorsBar.current) {
      chartAuthorsBar.current?.destroy()
      chartAuthorsBar.current = new Chart(refAuthorsBar.current, {
        type:'bar',
        data:{ labels: authors.map((x:any)=>x[0]), datasets:[{ label:'Топ авторов', data: authors.map((x:any)=>x[1]), backgroundColor: palette.yellow }] },
        options:{ ...hbarOptions, indexAxis:'y', onClick: (_evt:any, els:any[])=>{ if(!els?.length) return; const i=els[0].index; const label = (chartAuthorsBar.current as any)?.data?.labels?.[i]; if (label) nav(`/?q=${encodeURIComponent(String(label))}`) } }
      })
    }
    // cleanup on theme change or data change
    return () => {
      chartTypes.current?.destroy(); chartMonths.current?.destroy(); chartYears.current?.destroy(); chartSizes.current?.destroy(); chartExts.current?.destroy(); chartWeekdays.current?.destroy(); chartHours.current?.destroy(); chartMeta.current?.destroy(); chartAvg.current?.destroy(); chartAuthorsBar.current?.destroy(); chartCollections.current?.destroy(); chartCollSize.current?.destroy(); chartLargest.current?.destroy(); chartTagKeys.current?.destroy(); chartTagVals.current?.destroy()
  }
  // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [data, palette, rangeMonths, cumulativeMonths, logScale])

  function exportCSV(name: string, rows: StatKV[]) {
    const header = 'label,value\n'
    const body = rows.map(([k,v]) => `${String(k).replaceAll('"','""')},${v}`).join('\n')
    const csv = header + body
    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' })
    const a = document.createElement('a')
    const url = URL.createObjectURL(blob)
    a.href = url
    a.download = `${name}.csv`
    document.body.appendChild(a)
    a.click()
    document.body.removeChild(a)
    URL.revokeObjectURL(url)
  }

  useEffect(()=>{
    let ignore=false
    ;(async()=>{
      if (!selectedKey) { setTagVals([]); return }
      try{
        const r = await fetch(`/api/stats/tag-values?key=${encodeURIComponent(selectedKey)}&limit=30`)
        const j = await r.json(); if (!ignore){ setTagVals(j.items||[]) }
      }catch{ if (!ignore) setTagVals([]) }
    })()
    return ()=>{ ignore=true }
  }, [selectedKey])

  useEffect(()=>{
    if (!refTagVals.current) return
    chartTagVals.current?.destroy()
    chartTagVals.current = new Chart(refTagVals.current, {
      type:'bar', data:{ labels: tagVals.map(x=>x[0]), datasets:[{ label:`Значения тега: ${selectedKey||''}`, data: tagVals.map(x=>x[1]), backgroundColor: '#4e79a7' }] },
      options:{ responsive:true, maintainAspectRatio:false, indexAxis:'y', scales:{ x:{ type: logScale? 'logarithmic':'linear' } } }
    })
  }, [tagVals, selectedKey, logScale])

  const totalInfo = useMemo(() => {
    if (!data) return null
    const bytes = Number(data.total_size_bytes||0)
    const nf = new Intl.NumberFormat('ru-RU')
    const units = ['Б','КБ','МБ','ГБ','ТБ']
    let v = bytes; let i = 0
    while (v >= 1024 && i < units.length-1) { v/=1024; i++ }
    return { files: nf.format(data.total_files||0), size: `${v.toFixed(1)} ${units[i]}` }
  }, [data])
  return (
    <div className="card p-3">
      <div className="d-flex align-items-center justify-content-between mb-2">
        <div className="fw-semibold">Статистика</div>
        <div className="d-flex align-items-center gap-3">
          <label className="form-label m-0">Период:</label>
          <select className="form-select form-select-sm" value={rangeMonths} onChange={e=>setRangeMonths(parseInt(e.target.value))} style={{width:140}}>
            <option value={12}>12 мес</option>
            <option value={24}>24 мес</option>
            <option value={36}>36 мес</option>
            <option value={0}>Все</option>
          </select>
          <div className="form-check form-switch m-0">
            <input id="logscale" className="form-check-input" type="checkbox" checked={logScale} onChange={e=>setLogScale(e.target.checked)} />
            <label htmlFor="logscale" className="form-check-label">Лог‑шкала</label>
          </div>
          <div className="form-check form-switch m-0">
            <input id="cum" className="form-check-input" type="checkbox" checked={cumulativeMonths} onChange={e=>setCumulativeMonths(e.target.checked)} />
            <label htmlFor="cum" className="form-check-label">Накопит.</label>
          </div>
        </div>
      </div>
      {loading && (
        <div className="row g-3">
          {Array.from({length: 6}).map((_,i)=>(
            <div key={i} className="col-12 col-xl-4"><div className="skeleton" style={{height:200}} /></div>
          ))}
        </div>
      )}
      {error && <div className="text-danger">{error}</div>}
      {!loading && data && (
        <>
          <div className="row g-3">
            <div className="col-6 col-xl-3">
              <div className="card p-3 text-center kpi" aria-labelledby="kpi-files">
                <div id="kpi-files" className="label muted">Файлов</div>
                <div className="value">{totalInfo?.files}</div>
              </div>
            </div>
            <div className="col-6 col-xl-3">
              <div className="card p-3 text-center kpi" aria-labelledby="kpi-size">
                <div id="kpi-size" className="label muted">Размер</div>
                <div className="value">{totalInfo?.size}</div>
              </div>
            </div>
          </div>
          <div className="row g-3 mt-1">
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">По типам</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('types', data?.types || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refTypes}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">По месяцам</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> {
                    const monthsRaw = (data?.months||[]).slice(rangeMonths>0? -rangeMonths: undefined)
                    const rows = cumulativeMonths ? (function(){ let acc=0; return monthsRaw.map(([m,v]: any) => { acc += Number(v||0); return [m, acc] as any }) })() : monthsRaw
                    exportCSV('months', rows)
                  }}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refMonths}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-4">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Размеры файлов</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('sizes', data?.sizes || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refSizes}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-4">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">По расширениям</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('exts', data?.exts || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refExts}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-4">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Дни недели</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('weekdays', data?.weekdays || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refWeekdays}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-4">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Часы</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('hours', data?.hours || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refHours}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Заполненность метаданных</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('meta_presence', data?.meta_presence || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refMeta}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Средний размер (МБ)</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('avg_size_by_type', data?.avg_size_type || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refAvgSize}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">По годам</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('years', data?.years || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refYears}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Топ авторов</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('authors', data?.authors || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refAuthorsBar}/></div>
              </div>
            </div>
          </div>
          <div className="row g-3 mt-1">
            <div className="col-12 col-xl-6">
              <div className="card p-3">
                <div className="fw-semibold mb-2">Облако авторов</div>
                <div style={{minHeight:200}}>
                  {(data.authors_cloud||[]).map((x:any,i:number)=>{
                    const [name,cnt]=x; const size=12+Math.min(26, Math.round(Math.log(1+cnt)*8));
                    return (
                      <button key={i} className="btn btn-link p-0 me-2" style={{fontSize:size, lineHeight:'2.2rem'}} onClick={()=> nav(`/?q=${encodeURIComponent(String(name))}`)}>{name}</button>
                    )
                  })}
                </div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-3">
                <div className="fw-semibold mb-2">Облако ключевых слов</div>
                <div style={{minHeight:200}}>
                  {(data.top_keywords||[]).map((x:any,i:number)=>{
                    const [val,cnt]=x; const size=12+Math.min(24, Math.round(Math.log(1+cnt)*7));
                    return (
                      <button key={i} className="btn btn-link p-0 me-2" style={{fontSize:size, lineHeight:'2.2rem'}} onClick={()=> nav(`/?q=${encodeURIComponent(String(val))}`)}>{val}</button>
                    )
                  })}
                </div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">По коллекциям</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('collections', data?.collections_counts || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refCollections}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Ключи тегов</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('tag_keys', data?.tag_keys || [])}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refTagKeys}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Значения тега</div>
                  <div className="d-flex align-items-center gap-2">
                    <select className="form-select form-select-sm" style={{width:180}} value={selectedKey} onChange={e=>setSelectedKey(e.target.value)}>
                      {(data?.tag_keys||[]).map((x:any,i:number)=> <option key={i} value={String(x[0]||'')}>{x[0]}</option>)}
                    </select>
                    <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV(`tag_values_${selectedKey}`, tagVals || [])}>CSV</button>
                  </div>
                </div>
                <div className="chart-body"><canvas ref={refTagVals}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Объём по коллекциям (МБ)</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('collections_total_size_mb', (data?.collections_total_size||[]).map(([n,b]:any)=>[n, Math.round(Number(b||0)/(1024*1024))]))}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refCollSize}/></div>
              </div>
            </div>
            <div className="col-12 col-xl-6">
              <div className="card p-2 chart-card">
                <div className="d-flex align-items-center justify-content-between">
                  <div className="fw-semibold chart-title">Самые большие файлы (МБ)</div>
                  <button className="btn btn-sm btn-outline-secondary" onClick={()=> exportCSV('largest_files_mb', (data?.largest_files||[]).map(([n,b]:any)=>[n, Math.round(Number(b||0)/(1024*1024))]))}>CSV</button>
                </div>
                <div className="chart-body"><canvas ref={refLargest}/></div>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
