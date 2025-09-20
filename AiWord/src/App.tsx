import React, { useState, useEffect, useCallback, useMemo, useRef } from 'react';
import MarkdownIt from 'markdown-it';
import markdownItKatex from 'markdown-it-katex';
import 'katex/dist/katex.min.css';
import { parseBibTeX, BibDatabase, formatGostReference } from './components/BibliographyFormatter';
import { Document as DocxDocument, Packer, Paragraph, HeadingLevel, TextRun, Table as DocxTable, TableRow as DocxTableRow, TableCell as DocxTableCell, WidthType } from 'docx';
import { 
  Edit3, 
  FileText, 
  Download, 
  Copy, 
  Settings, 
  Play, 
  BookOpen, 
  MessageSquare, 
  Sparkles, 
  Eye,
  Upload,
  CheckCircle,
  AlertCircle,
  Clock,
  Brain,
  PenTool,
  Moon,
  Sun,
  Save,
  FolderOpen,
  Bold,
  Italic,
  List,
  ListOrdered,
  Quote,
  Code,
  Table,
  Hash,
  Link,
  Trash2,
  BookMarked,
  Zap,
  Search,
  Star,
  HelpCircle,
  ShieldCheck,
  StopCircle,
  Database
} from 'lucide-react';

type PdfTextItem = { str?: string } & Record<string, unknown>;
type PdfTextContent = { items: PdfTextItem[] };
type PdfPage = { getTextContent: () => Promise<PdfTextContent> };
type PdfDocument = { numPages: number; getPage: (pageNumber: number) => Promise<PdfPage> };
type PdfLoadingTask = { promise: Promise<PdfDocument> };
type PdfJsLib = {
  getDocument: (options: { data: ArrayBuffer }) => PdfLoadingTask;
  GlobalWorkerOptions?: { workerSrc?: string };
};

let pdfWorkerLoaded = false;
let pdfJsLibPromise: Promise<PdfJsLib> | null = null;

const loadPdfJsLib = async (): Promise<PdfJsLib> => {
  if (!pdfJsLibPromise) {
    pdfJsLibPromise = import('pdfjs-dist/build/pdf').then(mod => mod as unknown as PdfJsLib);
  }
  return pdfJsLibPromise;
};

const ensurePdfWorker = async () => {
  if (pdfWorkerLoaded) return;
  const worker = await import('pdfjs-dist/build/pdf.worker.min.mjs?url');
  const pdfjsLib = await loadPdfJsLib();
  if (pdfjsLib.GlobalWorkerOptions) {
    pdfjsLib.GlobalWorkerOptions.workerSrc = worker.default as string;
  }
  pdfWorkerLoaded = true;
};

const decodeXmlEntities = (value: string): string => value
  .replace(/&amp;/g, '&')
  .replace(/&quot;/g, '"')
  .replace(/&apos;/g, "'")
  .replace(/&lt;/g, '<')
  .replace(/&gt;/g, '>');

const extractDocxText = async (buffer: ArrayBuffer): Promise<string> => {
  try {
    const JSZipModule = await import('jszip');
    const zip = await JSZipModule.default.loadAsync(buffer);
    const docFile = zip.file('word/document.xml');
    if (!docFile) return '';
    const xml = await docFile.async('string');
    const parser = new DOMParser();
    const xmlDoc = parser.parseFromString(xml, 'application/xml');
    const paragraphs = Array.from(xmlDoc.getElementsByTagName('w:p'));
    const pieces = paragraphs.map((p) => {
      const texts = Array.from(p.getElementsByTagName('w:t'));
      const combined = texts.map(t => decodeXmlEntities(t.textContent || '')).join('');
      return combined.trim();
    }).filter(Boolean);
    return pieces.join('\n').replace(/\n{3,}/g, '\n\n').trim();
  } catch (error) {
    console.error('DOCX parse error:', error);
    return '';
  }
};

const extractPdfText = async (buffer: ArrayBuffer): Promise<string> => {
  await ensurePdfWorker();
  const pdfjsLib = await loadPdfJsLib();
  const loadingTask = pdfjsLib.getDocument({ data: buffer });
  const pdf = await loadingTask.promise;
  const pages: string[] = [];
  for (let pageNumber = 1; pageNumber <= pdf.numPages; pageNumber += 1) {
    const page = await pdf.getPage(pageNumber);
    const content = await page.getTextContent();
    const text = (content.items as PdfTextItem[]) 
      .map(item => (typeof item.str === 'string' ? item.str : ''))
      .join(' ')
      .replace(/\s+/g, ' ')
      .trim();
    if (text) pages.push(text);
  }
  return pages.join('\n\n').trim();
};

const extractTextForReviewFile = async (file: File): Promise<string> => {
  const extension = file.name.split('.').pop()?.toLowerCase();
  try {
    if (extension === 'docx') {
      return extractDocxText(await file.arrayBuffer());
    }
    if (extension === 'pdf') {
      return extractPdfText(await file.arrayBuffer());
    }
    return (await file.text()).trim();
  } catch (error) {
    console.error('Не удалось извлечь текст из файла для рецензии:', error);
    try {
      return (await file.text()).trim();
    } catch {
      return '';
    }
  }
};

const AGENT_CONTEXT_LIMIT = 6000;

const trimForAgent = (source: string, limit: number = AGENT_CONTEXT_LIMIT) => {
  const normalized = (source || '').trim();
  if (normalized.length <= limit) {
    return { text: normalized, truncated: false } as const;
  }
  return { text: normalized.slice(0, limit), truncated: true } as const;
};

const buildChainDocument = ({
  planText,
  catalogNotes,
  drafts,
  improvedText,
  reviewText,
  questionsText,
  summaryText,
  thesisText,
  factBlock,
}: {
  planText?: string;
  catalogNotes?: string[];
  drafts?: string[];
  improvedText?: string;
  reviewText?: string;
  questionsText?: string;
  summaryText?: string;
  thesisText?: string;
  factBlock?: string;
}): string => {
  const sections: string[] = [];
  const addSection = (title: string, body: string | undefined) => {
    const trimmed = (body || '').trim();
    if (!trimmed) return;
    const firstLine = trimmed.split('\n')[0] || '';
    const hasHeader = /^#{1,6}\s/.test(firstLine);
    sections.push(hasHeader ? trimmed : `## ${title}\n\n${trimmed}`);
  };

  if (planText?.trim()) {
    sections.push('# План', planText.trim());
  }

  if (catalogNotes && catalogNotes.length) {
    sections.push('## Каталог — данные', catalogNotes.join('\n\n'));
  }

  if (improvedText?.trim()) {
    addSection('Финальный текст', improvedText);
  } else if (drafts && drafts.length) {
    addSection('Черновики', drafts.join('\n\n'));
  }

  addSection('Рецензия агента', reviewText);
  addSection('Вопросы для проверки', questionsText);
  addSection('Краткий конспект', summaryText);
  addSection('Ключевые тезисы', thesisText);
  addSection('Факт-чекинг', factBlock);

  return sections.filter(Boolean).join('\n\n');
};

interface Project {
  title: string;
  language: string;
  style_guide: string;
  persona: string;
  content?: string;
  bibliography?: string;
  created_at?: string;
  updated_at?: string;
}

interface AgentLog {
  time: string;
  title: string;
  body: string;
  cached?: boolean;
}

interface CriteriaProfile {
  name: string;
  description: string;
  criteria: string;
  category: 'academic' | 'journal' | 'custom';
}

interface CacheEntry {
  input: string;
  output: string;
  timestamp: number;
  action: string;
}

type AgentCallOptions = {
  streamMode?: 'auto' | 'force' | 'off';
  applyToEditor?: boolean;
};

interface AgentPayloadShape {
  project?: Project;
  topic?: string;
  constraints?: string;
  outline_point?: string;
  context_md?: string;
  text_md?: string;
  instructions?: string;
  criteria?: string;
  mode?: string;
  catalog_context?: string;
}

const DEFAULT_PROJECT: Project = {
  title: "Новая статья",
  language: "ru",
  style_guide: "Пиши чётко и структурированно. Без воды. Сохраняй академический тон, «выводы» — коротко и по делу. Используй Markdown: заголовки, списки, таблицы. Формулы как $...$ или ```math``` при необходимости.",
  persona: "Ты — научный редактор и соавтор. Помогаешь с планом, структурой, стилем, и аргументацией.",
  content: "# Заголовок\n\nВставьте/пишите текст здесь...",
  created_at: new Date().toISOString(),
  updated_at: new Date().toISOString()
};

const DEFAULT_GOST_TEMPLATES: Record<string, string> = {
  "ГОСТ_Статья": `# Название статьи\n\n## Аннотация\nКраткое содержание (5–7 предложений).\n\n## Введение\nАктуальность, цель, задачи, объект/предмет.\n\n## Обзор литературы\nКлючевые источники, пробелы [@key2020].\n\n## Методы\nМодель, алгоритм, допущения.\n\n## Результаты\nОсновные находки, таблицы/рисунки.\n\n## Обсуждение\nСравнение с работами, ограничения.\n\n## Заключение\nВыводы, новизна, практическая значимость.\n\n## Список литературы\n(генерируется автоматически по [@citekey])\n`,
  "ГОСТ_Отчёт_НИР": `# Отчёт по НИР\n\n## Введение\nЦель, задачи, показатели.\n\n## Аналитический обзор\nСостояние проблемы, анализ решений [@petrov2019].\n\n## Теоретическая часть\nПостановка и формализация.\n\n## Экспериментальная часть\nПлан эксперимента, стенд, метрики.\n\n## Результаты и обсуждение\nТаблицы/графики, интерпретация.\n\n## Выводы и рекомендации\nКраткие тезисы.\n\n## Список сокращений\n…\n\n## Список литературы\n(автогенерация из BibTeX)\n`
};

const CRITERIA_PROFILES: CriteriaProfile[] = [
  {
    name: "ГОСТ Р 7.0.11-2011",
    description: "Стандарт для диссертаций и авторефератов",
    criteria: "Соблюдай ГОСТ Р 7.0.11-2011: структура, терминология, библиография. Академический стиль, без воды.",
    category: "academic"
  },
  {
    name: "ВАК (гуманитарные)",
    description: "Требования ВАК для гуманитарных наук",
    criteria: "Стиль ВАК: четкая аргументация, обзор литературы, методология. Избегай публицистики.",
    category: "academic"
  },
  {
    name: "ВАК (технические)",
    description: "Требования ВАК для технических наук",
    criteria: "Технический стиль ВАК: точность формулировок, количественные данные, воспроизводимость результатов.",
    category: "academic"
  },
  {
    name: "Nature/Science",
    description: "Стиль топовых международных журналов",
    criteria: "Стиль Nature/Science: краткость, значимость, новизна. Четкие выводы, минимум технических деталей.",
    category: "journal"
  },
  {
    name: "IEEE",
    description: "Стандарт IEEE для технических публикаций",
    criteria: "IEEE стиль: техническая точность, стандартная структура, количественные результаты, воспроизводимость.",
    category: "journal"
  },
  {
    name: "Популярная наука",
    description: "Для научно-популярных изданий",
    criteria: "Популярный стиль: доступность, примеры, аналогии. Сохраняй научность, избегай упрощений.",
    category: "journal"
  }
];

const QUICK_FORMATTERS = [
  { name: 'H2', icon: Hash, action: (text: string, selection: string) => `## ${selection || 'Заголовок'}` },
  { name: 'H3', icon: Hash, action: (text: string, selection: string) => `### ${selection || 'Подзаголовок'}` },
  { name: 'Жирный', icon: Bold, action: (text: string, selection: string) => `**${selection || 'текст'}**` },
  { name: 'Курсив', icon: Italic, action: (text: string, selection: string) => `*${selection || 'текст'}*` },
  { name: 'Список', icon: List, action: (text: string, selection: string) => `- ${selection || 'элемент списка'}` },
  { name: 'Нумерация', icon: ListOrdered, action: (text: string, selection: string) => `1. ${selection || 'элемент списка'}` },
  { name: 'Цитата', icon: Quote, action: (text: string, selection: string) => `> ${selection || 'цитата'}` },
  { name: 'Код', icon: Code, action: (text: string, selection: string) => `\`${selection || 'код'}\`` },
  { name: 'Таблица', icon: Table, action: () => `| Заголовок 1 | Заголовок 2 |\n|-------------|-------------|\n| Ячейка 1    | Ячейка 2    |` },
  { name: 'Ссылка', icon: Link, action: (text: string, selection: string) => `[${selection || 'текст ссылки'}](URL)` },
];

export default function App() {
  const [project, setProject] = useState<Project>(DEFAULT_PROJECT);
  const [content, setContent] = useState<string>(DEFAULT_PROJECT.content || "");
  const [preview, setPreview] = useState<string>("");
  const [bibliography, setBibliography] = useState<string>("");
  const [bibDb, setBibDb] = useState<BibDatabase>({});
  const [logs, setLogs] = useState<AgentLog[]>([]);
  const [status, setStatus] = useState<'ready' | 'working' | 'error'>('ready');
  const [selectedTemplate, setSelectedTemplate] = useState<string>("");
  const [isDarkMode, setIsDarkMode] = useState<boolean>(() => {
    const saved = localStorage.getItem('llm-writer-dark-mode');
    return saved ? JSON.parse(saved) : false;
  });
  const [activeTab, setActiveTab] = useState<'editor' | 'preview' | 'logs' | 'bibliography'>('editor');
  const [criteriaProfiles, setCriteriaProfiles] = useState(CRITERIA_PROFILES);
  const [selectedCriteria, setSelectedCriteria] = useState<string>(CRITERIA_PROFILES[0].criteria);
  const [templatesMap, setTemplatesMap] = useState<Record<string, string>>({ ...DEFAULT_GOST_TEMPLATES });
  const [selectedTemplateKey, setSelectedTemplateKey] = useState<string>("");
  const [contentHistory, setContentHistory] = useState<string[]>([]);
  const [exportToast, setExportToast] = useState<string>("");
  const [exportUrl, setExportUrl] = useState<string>("");
  const [savingPulse, setSavingPulse] = useState<boolean>(false);
  const [cache, setCache] = useState<Map<string, CacheEntry>>(new Map());
  const [showQuickFormat, setShowQuickFormat] = useState<boolean>(false);
  const [searchTerm, setSearchTerm] = useState<string>("");
  const [autoSaveEnabled, setAutoSaveEnabled] = useState<boolean>(true);
  const [llmBaseUrl, setLlmBaseUrl] = useState<string>(() => localStorage.getItem('llm-writer-base-url') || 'http://localhost:1234/v1');
  const [llmModel, setLlmModel] = useState<string>(() => localStorage.getItem('llm-writer-model') || 'google/gemma-3n-e4b');
  const [leftOpen, setLeftOpen] = useState<boolean>(true);
  const [rightOpen, setRightOpen] = useState<boolean>(true);
  const [focusMode, setFocusMode] = useState<boolean>(false);
  const [bibText, setBibText] = useState<string>("");
  const [contentRedo, setContentRedo] = useState<string[]>([]);
  const [showCiteBox, setShowCiteBox] = useState<boolean>(false);
  const [citeSuggestions, setCiteSuggestions] = useState<{key: string; title?: string; authors?: string; year?: string}[]>([]);
  const [citeIndex, setCiteIndex] = useState<number>(0);
  const [citeReplaceFrom, setCiteReplaceFrom] = useState<number>(-1);
  const [citeReplaceTo, setCiteReplaceTo] = useState<number>(-1);

  // --- Deep integration with Agregator ---
  interface AggCollection { id: number; name: string; slug: string; searchable: boolean; graphable: boolean; count?: number }
  interface AggSearchItem { file_id: number; title: string; rel_path?: string; score?: number; snippets?: string[]; hits?: unknown[]; matched_terms?: string[] }
  interface AiSearchRequest {
    query: string;
    top_k: number;
    collection_ids: number[];
    material_types?: string[];
    year_from?: string;
    year_to?: string;
    tag_filters?: string[];
    deep_search?: boolean;
  }
  const [useAgregator, setUseAgregator] = useState<boolean>(()=>{
    try{ return localStorage.getItem('aiword-use-agregator') === '1' }catch{ return false }
  });
  const [aggCollections, setAggCollections] = useState<AggCollection[]>([]);
  const [aggSelected, setAggSelected] = useState<number[]>([]);
  const [aggTopK, setAggTopK] = useState<number>(3);
  const [aggBusy, setAggBusy] = useState<boolean>(false);
  const [aggAnswer, setAggAnswer] = useState<string>('');
  const [aggItems, setAggItems] = useState<AggSearchItem[]>([]);
  const [aggPanelOpen, setAggPanelOpen] = useState<boolean>(false);
  const [aggDeepSearch, setAggDeepSearch] = useState<boolean>(true);
  const [aggTypes, setAggTypes] = useState<string>('');
  const [aggYearFrom, setAggYearFrom] = useState<string>('');
  const [aggYearTo, setAggYearTo] = useState<string>('');
  const [aggTags, setAggTags] = useState<string>('');
  const [aggQuery, setAggQuery] = useState<string>(() => (DEFAULT_PROJECT.title || '').slice(0, 400));
  const [aggQueryTouched, setAggQueryTouched] = useState<boolean>(false);
  const [aggProgress, setAggProgress] = useState<string[]>([]);

  // Streaming LLM state
  const [useStream, setUseStream] = useState<boolean>(true);
  const [liveText, setLiveText] = useState<string>('');
  const [abortCtrl, setAbortCtrl] = useState<AbortController | null>(null);
  const [streamToEditor, setStreamToEditor] = useState<boolean>(true);
  type StreamContext = {
    before: string;
    after: string;
    anchorStart: number;
    anchorEnd: number;
    scrollTop: number;
    mode: 'append' | 'replace' | 'insert';
  };
  const streamContextRef = useRef<StreamContext | null>(null);
  const reviewFileInputRef = useRef<HTMLInputElement | null>(null);
  const [workflowRunning, setWorkflowRunning] = useState<boolean>(false);
  const chainAbortRef = useRef<boolean>(false);
  const [showHelp, setShowHelp] = useState<boolean>(false);

  // Facet suggestions for filters
  type FacetType = { name: string; count: number };
  type FacetVal = { value: string; count: number };
  const [facetTypes, setFacetTypes] = useState<FacetType[]>([]);
  const [facetTags, setFacetTags] = useState<Record<string, FacetVal[]>>({});
  const [facetKey, setFacetKey] = useState<string>('');
  const [facetValue, setFacetValue] = useState<string>('');

  // Mapping for footnotes → sources list
  const [, setAggCiteMap] = useState<Map<number, AggSearchItem>>(new Map());


  // Автосохранение в localStorage
  useEffect(() => {
    if (autoSaveEnabled) {
      const saveData: {
        project: Project;
        content: string;
        bibText: string;
        timestamp: number;
      } = {
        project,
        content,
        bibText,
        timestamp: Date.now()
      };
      localStorage.setItem('llm-writer-autosave', JSON.stringify(saveData));
      setSavingPulse(true);
      const t = setTimeout(() => setSavingPulse(false), 600);
      return () => clearTimeout(t);
    }
  }, [project, content, bibText, autoSaveEnabled]);

  // Восстановление из localStorage при загрузке
  useEffect(() => {
    const saved = localStorage.getItem('llm-writer-autosave');
    if (saved) {
      try {
        const data = JSON.parse(saved);
        if (data.project && data.content) {
          setProject(data.project);
          setContent(data.content);
          setBibText(data.bibText || ""); // Восстанавливаем bibText
          try { if (data.bibText) setBibDb(parseBibTeX(data.bibText)); } catch { /* ignore broken bib */ }
        }
      } catch (error) {
        console.error('Ошибка восстановления автосохранения:', error);
      }
    }

    // Загрузка кеша
    const cachedData = localStorage.getItem('llm-writer-cache');
    if (cachedData) {
      try {
        const cacheArray = JSON.parse(cachedData);
        if (Array.isArray(cacheArray)) {
          setCache(new Map(cacheArray as [string, CacheEntry][]));
        }
      } catch (error) {
        console.error('Ошибка загрузки кеша:', error);
      }
    }
  }, []);

  // Persist Agregator toggle
  useEffect(()=>{ try{ localStorage.setItem('aiword-use-agregator', useAgregator ? '1':'0'); }catch{ /* storage disabled */ } }, [useAgregator]);

  // Load collections when needed
  useEffect(()=>{
    if (!useAgregator) return;
    fetch('/api/collections').then(r=>r.json()).then((cols: AggCollection[])=>{
      setAggCollections(cols);
      const pre = cols.filter(c=>c.searchable).map(c=>c.id);
      setAggSelected(s=> (s && s.length>0) ? s : pre);
    }).catch((error: unknown)=>{
      console.error('Не удалось загрузить коллекции для AiWord', error);
    });
    // Load facet suggestions from Agregator
    fetch('/api/facets').then(r=>r.json()).then((j)=>{
      const rawTypes = Array.isArray(j.types) ? (j.types as unknown[]) : [];
      const t: FacetType[] = rawTypes
        .map((entry): FacetType | null => {
          if (!Array.isArray(entry) || entry.length < 2) return null;
          const [name, count] = entry as [unknown, unknown];
          return { name: String(name ?? ''), count: Number(count ?? 0) };
        })
        .filter((x): x is FacetType => !!x && !!x.name);
      setFacetTypes(t.filter(x=>x.name));
      const tf: Record<string, FacetVal[]> = {};
      const src = (j.tag_facets || {}) as Record<string, unknown>;
      Object.entries(src).forEach(([key, value]) => {
        const list = Array.isArray(value) ? (value as unknown[]) : [];
        tf[key] = list
          .map((entry): FacetVal | null => {
            if (!Array.isArray(entry) || entry.length < 2) return null;
            const [val, count] = entry as [unknown, unknown];
            return { value: String(val ?? ''), count: Number(count ?? 0) };
          })
          .filter((x): x is FacetVal => !!x && !!x.value);
      });
      setFacetTags(tf);
      if (Object.keys(tf).length) {
        setFacetKey(prev => (prev ? prev : Object.keys(tf)[0]));
      }
    }).catch((error: unknown)=>{
      console.error('Не удалось загрузить фасеты для AiWord', error);
    });
  }, [useAgregator]);

  useEffect(() => {
    if (aggQueryTouched) return;
    const candidate = (project.title || '').slice(0, 400);
    if (candidate !== aggQuery) {
      setAggQuery(candidate);
    }
  }, [project.title, aggQueryTouched, aggQuery]);

  // Сохранение темы
  useEffect(() => {
    localStorage.setItem('llm-writer-dark-mode', JSON.stringify(isDarkMode));
    if (isDarkMode) {
      document.documentElement.classList.add('dark');
    } else {
      document.documentElement.classList.remove('dark');
    }
  }, [isDarkMode]);

  // Сохранение кеша
  useEffect(() => {
    const cacheArray = Array.from(cache.entries());
    localStorage.setItem('llm-writer-cache', JSON.stringify(cacheArray));
  }, [cache]);

  // Сохранение настроек LLM
  useEffect(() => {
    localStorage.setItem('llm-writer-base-url', llmBaseUrl);
    localStorage.setItem('llm-writer-model', llmModel);
  }, [llmBaseUrl, llmModel]);

  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if (!e.ctrlKey) return;
      if (e.key === '1') setActiveTab('editor');
      if (e.key === '2') setActiveTab('preview');
      if (e.key === '3') setActiveTab('bibliography');
    };
    window.addEventListener('keydown', onKey);
    return () => window.removeEventListener('keydown', onKey);
  }, []);

  // Markdown renderer (client-side)
  const md = useMemo(() => {
    const instance = new MarkdownIt({
      html: false,
      linkify: true,
      breaks: false
    }).use(markdownItKatex);
    return instance;
  }, []);

  const renderPreview = useCallback(() => {
    try {
      // Поддержка ```math``` как блочной формулы → $$...$$
      const prepped = content.replace(/```math\n([\s\S]*?)\n```/g, (_m, body) => `$$\n${body}\n$$`);
      const html = md.render(prepped);
      setPreview(html);
    } catch (error) {
      console.error('Preview render error:', error);
    }
  }, [content, md]);

  // Цитирование: [@key] -> [n] и генерация списка литературы
  const injectReferences = useCallback((markdownText: string): { text: string; bibliographyText: string } => {
    const citePattern = /\[@([A-Za-z0-9_:-]+)\]/g;
    const keysInOrder: string[] = [];
    const body = markdownText.replace(citePattern, (_, k: string) => {
      if (!keysInOrder.includes(k)) keysInOrder.push(k);
      const n = keysInOrder.indexOf(k) + 1;
      return `[${n}]`;
    });
    if (keysInOrder.length === 0) return { text: markdownText, bibliographyText: '' };
    const refs = keysInOrder.map((k, i) => formatGostReference(k, bibDb[k], i + 1));
    const biblioSection = `## Список литературы\n\n${refs.join('\n')}\n`;
    let output = body;
    if (/## Список литературы/.test(output)) {
      output = output.replace(/## Список литературы[\s\S]*$/m, biblioSection);
    } else {
      output = `${output}\n\n${biblioSection}`;
    }
    return { text: output, bibliographyText: refs.join('\n') };
  }, [bibDb]);

  const updateBibliography = useCallback(() => {
    try {
      const { bibliographyText } = injectReferences(content);
      setBibliography(bibliographyText);
    } catch (error) {
      console.error('Bibliography update error:', error);
    }
  }, [content, injectReferences]);

  useEffect(() => {
    renderPreview();
    updateBibliography();
  }, [content, renderPreview, updateBibliography]);

  useEffect(() => {
    const t = setTimeout(() => {
      setContentHistory(prev => {
        const next = [...prev, content];
        return next.length > 10 ? next.slice(-10) : next;
      });
    }, 800);
    return () => clearTimeout(t);
  }, [content]);

  // Подсказки по цитированию: триггер по [@... в редакторе
  const updateCiteSuggestions = useCallback(() => {
    const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
    if (!textarea) return;
    const pos = textarea.selectionStart ?? -1;
    if (pos < 0) { setShowCiteBox(false); return; }
    const left = content.slice(0, pos);
    const m = left.match(/\[@([A-Za-z0-9_:-]*)$/);
    if (!m) { setShowCiteBox(false); return; }
    const prefix = m[1] || '';
    const keys = Object.keys(bibDb || {});
    if (keys.length === 0) { setShowCiteBox(false); return; }
    const normalized = prefix.toLowerCase();
    const items = keys
      .map(k => {
        const entry = bibDb[k];
        const title = entry?.fields?.title || '';
        const year = entry?.fields?.year || '';
        const authors = (entry?.persons?.author || entry?.persons?.editor || []).join(', ');
        return { key: k, title, authors, year };
      })
      .filter(it => !normalized || it.key.toLowerCase().includes(normalized) || it.title.toLowerCase().includes(normalized) || it.authors.toLowerCase().includes(normalized));
    setCiteSuggestions(items.slice(0, 10));
    setCiteIndex(0);
    setCiteReplaceFrom(pos - prefix.length - 2); // "[@" + prefix
    setCiteReplaceTo(pos);
    setShowCiteBox(true);
  }, [content, bibDb]);

  const applyCiteSuggestion = useCallback((key: string) => {
    if (citeReplaceFrom < 0 || citeReplaceTo < 0) return;
    const before = content.slice(0, citeReplaceFrom);
    const after = content.slice(citeReplaceTo);
    const insertion = `[@${key}]`;
    const next = before + insertion + after;
    setContent(next);
    setShowCiteBox(false);
    // Восстановим курсор после закрывающей скобки
    setTimeout(() => {
      const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
      if (textarea) {
        const caret = before.length + insertion.length;
        textarea.focus();
        textarea.setSelectionRange(caret, caret);
      }
    }, 0);
  }, [content, citeReplaceFrom, citeReplaceTo]);

  const onEditorKeyDown = (e: React.KeyboardEvent<HTMLTextAreaElement>) => {
    if (!showCiteBox) return;
    if (e.key === 'ArrowDown') { e.preventDefault(); setCiteIndex(i => Math.min(i + 1, Math.max(0, citeSuggestions.length - 1))); }
    else if (e.key === 'ArrowUp') { e.preventDefault(); setCiteIndex(i => Math.max(i - 1, 0)); }
    else if (e.key === 'Enter') { e.preventDefault(); const sel = citeSuggestions[citeIndex]; if (sel) applyCiteSuggestion(sel.key); }
    else if (e.key === 'Escape') { e.preventDefault(); setShowCiteBox(false); }
  };

  const onEditorKeyUpOrClick = () => {
    updateCiteSuggestions();
  };

  const addLog = (title: string, body: string, cached: boolean = false) => {
    const time = new Date().toLocaleTimeString();
    setLogs(prev => [...prev, { time, title, body, cached }]);
  };

  // Кеширование запросов
  const getCacheKey = (action: string, payload: unknown): string => {
    return `${action}:${JSON.stringify(payload)}`;
  };

  // --- Клиентский LLM вызов через LM Studio (OpenAI совместимый) ---
  const callLmStudio = async (messages: { role: 'system'|'user'|'assistant'; content: string }[], temperature = 0.3, max_tokens = 1200) => {
    const baseUrl = llmBaseUrl.replace(/\/$/, '');
    const model = llmModel;
    const res = await fetch(`${baseUrl}/chat/completions`, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer lm-studio'
      },
      body: JSON.stringify({ model, messages, temperature, max_tokens })
    });
    if (!res.ok) throw new Error(await res.text());
    const data = await res.json();
    return data.choices?.[0]?.message?.content || '';
  };

  // Streaming version (OpenAI-compatible SSE)
  const callLmStudioStream = async (
    messages: { role: 'system'|'user'|'assistant'; content: string }[],
    temperature = 0.3,
    max_tokens = 1200,
    onDelta?: (chunk: string) => void,
    injectToEditor: boolean = streamToEditor
  ) => {
    const baseUrl = llmBaseUrl.replace(/\/$/, '');
    const model = llmModel;
    const url = `${baseUrl}/chat/completions`;
    const ctrl = new AbortController();
    setAbortCtrl(ctrl);
    const res = await fetch(url, {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
        'Authorization': 'Bearer lm-studio'
      },
      body: JSON.stringify({ model, messages, temperature, max_tokens, stream: true }),
      signal: ctrl.signal,
    });
    if (!res.ok) throw new Error(await res.text());
    const reader = res.body?.getReader();
    if (!reader) return '';
    const dec = new TextDecoder();
    let full = '';
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += dec.decode(value, { stream: true });
      // SSE: split by newlines, parse lines starting with 'data:'
      const lines = buf.split(/\r?\n/);
      buf = lines.pop() || '';
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed.startsWith('data:')) continue;
        const json = trimmed.slice(5).trim();
        if (json === '[DONE]') { reader.cancel().catch(()=>{}); break; }
        try{
          const obj = JSON.parse(json);
          const delta = obj?.choices?.[0]?.delta?.content || '';
          if (delta) {
            full += delta;
            onDelta?.(delta);
            if (injectToEditor) {
              const ctx = streamContextRef.current;
              if (ctx) {
                const nextContent = ctx.before + full + ctx.after;
                setContent(() => nextContent);
                requestAnimationFrame(() => {
                  const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
                  if (!textarea) return;
                  const caret = ctx.before.length + full.length;
                  textarea.setSelectionRange(caret, caret);
                  if (ctx.mode === 'append') {
                    textarea.scrollTop = textarea.scrollHeight;
                  } else if (!Number.isNaN(ctx.scrollTop)) {
                    textarea.scrollTop = ctx.scrollTop;
                  }
                });
              }
            }
          }
        }catch{ /* ignore malformed SSE chunk */ }
      }
      setLiveText(full);
    }
    setAbortCtrl(null);
    return full;
  };

  // Compose context from Agregator AI search
  interface AggSearchResult {
    items: AggSearchItem[];
    answer?: string;
    keywords?: string[];
    query: string;
  }

  const agregatorSearch = async (rawQuery: string, opts: { manual?: boolean } = {}): Promise<AggSearchResult> => {
    const normalized = (rawQuery || '').trim();
    if (!normalized) {
      return { items: [], answer: '', keywords: [], query: '' };
    }
    if (!aggQueryTouched || opts.manual) {
      setAggQuery(normalized);
    }
    if (opts.manual) {
      setAggQueryTouched(true);
    }
    setAggBusy(true);
    setAggAnswer('');
    setAggItems([]);
    setAggProgress(['Запуск поиска…']);
    try {
      const safeTopK = Math.max(1, Math.min(5, aggTopK));
      const body: AiSearchRequest = { query: normalized, top_k: safeTopK, collection_ids: aggSelected, deep_search: aggDeepSearch };
      const types = (aggTypes || '').split(',').map(s => s.trim().toLowerCase()).filter(Boolean);
      if (types.length) body.material_types = types;
      if (aggYearFrom) body.year_from = aggYearFrom.trim();
      if (aggYearTo) body.year_to = aggYearTo.trim();
      const tags = (aggTags || '').split(/\n|;|,/).map(s => s.trim()).filter(s => s.includes('='));
      if (tags.length) body.tag_filters = tags;
      const r = await fetch('/api/ai-search', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
      if (!r.ok) {
        const text = await r.text();
        throw new Error(text || `HTTP ${r.status}`);
      }
      const j = await r.json();
      const items = (j.items || []) as AggSearchItem[];
      setAggItems(items);
      setAggAnswer(j.answer || '');
      const progress = Array.isArray(j.progress) ? j.progress : [];
      setAggProgress(progress.length ? progress : ['Поиск завершён']);
      addLog('Agregator — поиск', `Запрос: ${j.query}\nКлючевые слова: ${(j.keywords || []).join(', ')}\nНайдено: ${items.length}`);
      if (j.answer) addLog('Agregator — краткий ответ', j.answer);
      return { items, answer: j.answer, keywords: j.keywords, query: j.query };
    } catch (error) {
      console.error('Agregator search error:', error);
      addLog('Agregator — ошибка', error instanceof Error ? error.message : String(error));
      setAggProgress([`Ошибка поиска: ${error instanceof Error ? error.message : String(error)}`]);
      return { items: [], answer: '', keywords: [], query: normalized };
    } finally {
      setAggBusy(false);
    }
  };

  const buildSystemPrompt = (p: Project) => (
    `Role: ${p.persona}\n` +
    `Language: ${p.language}\n` +
    `Style Guide: ${p.style_guide}\n` +
    `Общие правила:\n- Отвечай в формате Markdown.\n- Структурируй текст: H2/H3, списки, таблицы при необходимости.\n- В конце больших разделов давай краткие выводы (2–3 пункта).\n- Если информации недостаточно, делай допущения и помечай их.`
  );

  const callAgent = async (action: string, payload: AgentPayloadShape, options: AgentCallOptions = {}) => {
    const cacheKey = getCacheKey(action, payload);
    const cached = cache.get(cacheKey);
    
    // Проверяем кеш (действителен 1 час)
    if (cached && Date.now() - cached.timestamp < 3600000) {
      addLog(`${action} (кеш)`, cached.output, true);
      return { output: cached.output };
    }

    const streamMode = options.streamMode ?? 'auto';
    let streamingEnabled = useStream;
    if (streamMode === 'off') streamingEnabled = false;
    if (streamMode === 'force') streamingEnabled = true;
    const applyToEditor = options.applyToEditor ?? true;
    const injectToEditor = applyToEditor && streamingEnabled && streamToEditor;

    setStatus('working');
    try {
      // Выполняем локально вместо /api/agent/*
      let output = '';
      const sys = buildSystemPrompt(payload.project || project);
      let catalogContext = '';
      if (useAgregator) {
        // Determine a meaningful query for the catalogue
        let q = '';
        if (action === 'outline') q = String(payload.topic || project.title || '').slice(0, 300);
        else if (action === 'draft') q = `${String(payload.outline_point||'Раздел')} — ${String(project.title||'').slice(0,200)}`;
        else if (action === 'improve' || action === 'summary' || action === 'questions' || action === 'thesis' || action === 'factcheck') q = (String(payload.text_md||'').replace(/\s+/g,' ').slice(0, 220)) || (project.title||'');
        if (q) {
          const res = await agregatorSearch(q);
          const limit = Math.max(1, Math.min(3, aggTopK));
          const topItems = (res.items||[]).slice(0, limit);
          const lines = topItems.map((it, i) => {
            const baseTitle = it.title || (`file-${it.file_id}`);
            const rawSnippet = (it.snippets || []).join(' ');
            const normalized = rawSnippet.replace(/\s+/g, ' ').slice(0, 240);
            return `[${i+1}] ${baseTitle}: ${normalized}${rawSnippet.length > 240 ? '…' : ''}`;
          });
          const ans = (res.answer||'').trim();
          const contextBlock = [
            ans ? `Каталог — краткий ответ:\n${ans}` : '',
            lines.length ? `Каталог — фрагменты:\n${lines.join('\n')}` : ''
          ].filter(Boolean).join('\n\n');
          if (contextBlock) {
            catalogContext = contextBlock;
            addLog(`Agregator — контекст (${action})`, contextBlock);
          }
        }
      }
      // Capture insertion position if streaming to editor
      if (injectToEditor) {
        const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
        let start = content.length;
        let end = content.length;
        if (textarea && document.activeElement === textarea) {
          start = textarea.selectionStart;
          end = textarea.selectionEnd;
        } else {
          const sel = getSelectedText();
          if (sel.start >= 0) {
            start = sel.start;
            end = sel.end;
          }
        }
        const before = content.slice(0, start);
        const after = content.slice(end);
        const scrollTop = textarea ? textarea.scrollTop : 0;
        let mode: StreamContext['mode'] = 'replace';
        if (start === end && start === content.length) mode = 'append';
        else if (start === end) mode = 'insert';
        streamContextRef.current = { before, after, anchorStart: start, anchorEnd: end, scrollTop, mode };
      } else {
        streamContextRef.current = null;
      }

      if (action === 'outline') {
        const user = `Сформируй подробный план статьи (оглавление) с краткими аннотациями к каждому пункту. Укажи ориентировочный объём в процентах по разделам.\n\nТема: ${payload.topic || project.title}\n\nОграничения/пожелания: ${payload.constraints || ''}\n\n${catalogContext ? catalogContext + '\n\nПри составлении учитывай только эти сведения из каталога.' : ''}`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 1200, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'draft') {
        const user = `На основании контекста подготовь черновик раздела «${payload.outline_point || 'Раздел'}». Дай связный текст, 2–4 подзаголовка, списки при необходимости.\n\nКонтекст (Markdown):\n${payload.context_md || ''}\n\n${catalogContext ? catalogContext + '\n\nИспользуй сведения из каталога строго по тексту фрагментов; не выдумывай.' : ''}`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.4, 1400, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.4);
        }
      } else if (action === 'improve') {
        const user = `Отредактируй фрагмент по инструкции. Сохрани Markdown и структуру. Не выдумывай фактов.\n\nИнструкция: ${payload.instructions || 'Сделай яснее, компактнее, без потери смысла.'}\n\n${catalogContext ? catalogContext + '\n\nСогласуй термины и факты с каталогом.' : ''}\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || ''}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 800, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'review') {
        const user = `Сделай редакторский отзыв: сильные/слабые стороны, риски, недостающие разделы, стилистика, корректность терминологии. В конце — чек‑лист правок.\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || ''}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.1, 800, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.1);
        }
      } else if (action === 'summary') {
        const scope = payload.mode === 'selection' ? 'выделенный фрагмент' : 'весь текст';
        const user = `Сформулируй структурированный конспект (${scope}). Кратко, по делу, без воды. Используй маркированный список; каждую мысль делай отдельным пунктом. Не добавляй новых фактов.\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || content}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 700, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'questions') {
        const scope = payload.mode === 'selection' ? 'по выделенному фрагменту' : 'по всему тексту';
        const user = `Составь список проверочных вопросов ${scope}. 5–8 вопросов, требующих вдумчивого ответа. Не повторяйся и не добавляй новых фактов. Формат — пронумерованный список.\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || content}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 500, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'factcheck') {
        const scope = payload.mode === 'selection' ? 'выделенного фрагмента' : 'всего текста';
        const catalog = (payload.catalog_context || '').trim();
        const contextBlock = catalog ? `Контекст каталога:\n${catalog}\n\n` : '';
        const user = `${contextBlock}Проверь фактические утверждения ${scope}. Для каждого сформируй пункт списка: кратко переформулированный факт, затем статус **Подтверждено**, **Требует проверки** или **Не найдено**, и короткий комментарий.\n- Используй **Подтверждено** только если есть явное совпадение в предоставленных фрагментах и укажи ссылку [#n].\n- Если данных недостаточно или совпадение неточно — ставь **Требует проверки** и поясни, каких сведений не хватает.\n- Используй **Не найдено**, если фрагменты прямо противоречат утверждению.\nЗаверши вывод кратким списком ключевых рисков или неопределённостей.\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || content}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 700, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'thesis') {
        const modeLabel = payload.mode === 'selection' ? 'выделенный фрагмент' : 'текст документа';
        const user = `Сжато изложи ${modeLabel} списком из 3–6 тезисов. Каждый тезис — одна строка в виде Markdown-списка, начинай с «- ». Сохрани ключевые факты, но убери воду и повторы. Не добавляй новых сведений.\n\nТекст:\n\u0060\u0060\u0060md\n${payload.text_md || ''}\n\u0060\u0060\u0060`;
        setLiveText('');
        if (streamingEnabled) {
          output = await callLmStudioStream([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2, 600, (d)=> setLiveText(t=>t + d), injectToEditor);
        } else {
          output = await callLmStudio([{ role: 'system', content: sys }, { role: 'user', content: user }], 0.2);
        }
      } else if (action === 'chain') {
        const plan = await callAgent('outline', { project, topic: payload.topic, constraints: payload.criteria });
        const draft = await callAgent('draft', { project, outline_point: 'Черновик по плану', context_md: `${plan.output}\n\n${payload.context_md || ''}` });
        const improved = await callAgent('improve', { project, text_md: draft.output, instructions: payload.criteria });
        output = `# План\n\n${plan.output}\n\n# Черновик (с правкой по критериям)\n\n${improved.output}`;
      } else {
        throw new Error('Unknown action');
      }

      const data = { output };

      // Сохраняем в кеш
      const newCache = new Map(cache);
      newCache.set(cacheKey, {
        input: JSON.stringify(payload),
        output,
        timestamp: Date.now(),
        action
      });
      setCache(newCache);
      
      // Finalize streaming region to stable text
      if (applyToEditor) {
        if (injectToEditor) {
          const ctx = streamContextRef.current;
          if (ctx) {
            const nextContent = ctx.before + output + ctx.after;
            setContent(nextContent);
            requestAnimationFrame(() => {
              const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
              if (!textarea) return;
              const caret = ctx.before.length + output.length;
              textarea.focus();
              textarea.setSelectionRange(caret, caret);
              if (ctx.mode === 'append') {
                textarea.scrollTop = textarea.scrollHeight;
              } else {
                textarea.scrollTop = ctx.scrollTop;
              }
            });
          }
        } else {
          setContent(prev => `${prev}\n\n${output}\n\n`);
        }
      }
      streamContextRef.current = null;
      setStatus('ready');
      return data;
    } catch (error) {
      setStatus('error');
      throw error;
    }
  };

  // Insert helper: put text at editor caret
  const insertAtCaret = (text: string) => {
    const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
    if (!textarea) return;
    const start = textarea.selectionStart; const end = textarea.selectionEnd;
    const before = content.slice(0, start);
    const after = content.slice(end);
    const next = before + text + after;
    setContent(next);
    setTimeout(()=>{ textarea.focus(); const pos = start + text.length; textarea.setSelectionRange(pos, pos); }, 0);
  };

  // Build or update "## Источники каталога" section based on aggCiteMap
  const updateSourcesSection = (map: Map<number, AggSearchItem>) => {
    const keys = Array.from(map.keys()).sort((a, b) => a - b);
    const lines = keys.map(n => {
      const it = map.get(n)!;
      const title = it.title || `file-${it.file_id}`;
      const url = `/file/${it.file_id}`;
      return `- [${n}] ${title} — ${url}`;
    });
    const section = `## Источники каталога\n\n${lines.join('\n')}\n`;
    const re = /(##\s+Источники каталога[\s\S]*?)(?=\n##\s+|$)/m;
    setContent(prev => {
      if (re.test(prev)) {
        return prev.replace(re, section);
      }
      const trimmed = prev.trimEnd();
      return trimmed ? `${trimmed}\n\n${section}` : section;
    });
  };

  const insertFootnote = (preferred: number | null, item: AggSearchItem) => {
    let assigned = 0;
    let nextMap: Map<number, AggSearchItem> | null = null;
    setAggCiteMap(prev => {
      const used = new Set(prev.keys());
      let target = typeof preferred === 'number' && preferred > 0 && !used.has(preferred) ? preferred : 0;
      if (!target) {
        target = 1;
        while (used.has(target)) target += 1;
      }
      assigned = target;
      const next = new Map(prev);
      next.set(target, item);
      nextMap = next;
      return next;
    });
    if (assigned > 0) {
      insertAtCaret(`[${assigned}]`);
    }
    if (nextMap) {
      updateSourcesSection(nextMap);
    }
    return assigned;
  };

  const insertFootnotesRange = (count: number) => {
    const items = aggItems.slice(0, count);
    if (!items.length) return;
    let assigned: number[] = [];
    let nextMap: Map<number, AggSearchItem> | null = null;
    setAggCiteMap(prev => {
      const used = new Set(prev.keys());
      const next = new Map(prev);
      const numbers: number[] = [];
      items.forEach(item => {
        let candidate = 1;
        while (used.has(candidate)) candidate += 1;
        used.add(candidate);
        numbers.push(candidate);
        next.set(candidate, item);
      });
      assigned = numbers;
      nextMap = next;
      return next;
    });
    if (assigned.length) {
      const tokens = assigned.map(n => `[${n}]`).join('');
      insertAtCaret(tokens);
    }
    if (nextMap) {
      updateSourcesSection(nextMap);
    }
  };

  // Функция для получения выделенного текста (работает в редакторе и просмотре)
  const getSelectedText = () => {
    if (activeTab === 'editor') {
      const textarea = document.querySelector('#editor') as HTMLTextAreaElement;
      if (!textarea) return { text: '', start: -1, end: -1 };
      const start = textarea.selectionStart;
      const end = textarea.selectionEnd;
      const text = content.slice(start, end);
      return { text, start, end };
    } else if (activeTab === 'preview') {
      const selection = window.getSelection();
      if (!selection || selection.rangeCount === 0) return { text: '', start: -1, end: -1 };
      const text = selection.toString();
      // Ищем точное совпадение в исходнике
      let start = content.indexOf(text);
      if (start < 0) {
        // Пытаемся сопоставить по нормализованным пробелам/переводам строк
        const normalize = (s: string) => s.replace(/\s+/g, ' ').trim();
        const normalizedContent = normalize(content);
        const normalizedText = normalize(text);
        const approxIndex = normalizedContent.indexOf(normalizedText);
        if (approxIndex >= 0) {
          // Невозможно точно восстановить позицию в исходном content, просим перейти в редактор
          start = -1;
        }
      }
      const end = start >= 0 ? start + text.length : -1;
      return { text, start, end };
    }
    return { text: '', start: -1, end: -1 };
  };

  const copyToClipboard = async (value: string, label: string) => {
    try {
      await navigator.clipboard.writeText(value);
      addLog(label, 'Скопировано в буфер обмена');
    } catch (error) {
      console.error('Clipboard copy failed:', error);
      addLog('Ошибка копирования', error instanceof Error ? error.message : 'Недоступен буфер обмена');
    }
  };

  const takeAggQueryFromSelection = () => {
    const { text } = getSelectedText();
    const candidate = (text || '').replace(/\s+/g, ' ').trim();
    if (!candidate) {
      alert('Выделение пустое — нечего использовать для запроса.');
      return;
    }
    setAggQuery(candidate.slice(0, 400));
    setAggQueryTouched(true);
  };

  const takeAggQueryFromTitle = () => {
    const candidate = (project.title || '').trim();
    if (!candidate) {
      alert('Название проекта пустое — заполните его для использования в запросе.');
      return;
    }
    setAggQuery(candidate.slice(0, 400));
    setAggQueryTouched(true);
  };

  const runManualAggSearch = async () => {
    const source = (aggQuery || '').trim() || (project.title || '').trim();
    if (!source) {
      alert('Введите запрос для поиска по каталогу.');
      return;
    }
    await agregatorSearch(source, { manual: true });
  };

  const insertAggSnippet = (item: AggSearchItem) => {
    const snippet = (item.snippets || []).find(s => (s || '').trim());
    if (!snippet) {
      addLog('Agregator — фрагмент', 'Для этого источника нет доступного текста.');
      return;
    }
    const text = snippet.trim();
    const block = text.includes('\n') ? `\n> ${text.replace(/\n/g, '\n> ')}\n` : `\n> ${text}\n`;
    insertAtCaret(block);
  };

  const extractSections = (planText: string, limit = 4): string[] => {
    const lines = (planText || '').split(/\r?\n/);
    const sections: string[] = [];
    const seen = new Set<string>();
    const regex = /^\s*(?:##+\s+|\d+[).\s]+|[-*]\s+)(.+)$/;
    for (const raw of lines) {
      const match = raw.match(regex);
      if (!match) continue;
      const title = match[1]?.trim();
      if (!title) continue;
      const normalized = title.replace(/[.:]+$/, '').trim();
      if (!normalized || seen.has(normalized.toLowerCase())) continue;
      seen.add(normalized.toLowerCase());
      sections.push(normalized);
      if (sections.length >= limit) break;
    }
    return sections;
  };

  const handleQuickFormat = (formatter: typeof QUICK_FORMATTERS[0]) => {
    const textarea = document.querySelector('#editor') as HTMLTextAreaElement;
    if (!textarea) return;

    const start = textarea.selectionStart;
    const end = textarea.selectionEnd;
    const selectedText = content.slice(start, end);
    const formattedText = formatter.action(content, selectedText);
    
    const newContent = content.slice(0, start) + formattedText + content.slice(end);
    setContent(newContent);
    
    // Восстанавливаем фокус
    setTimeout(() => {
      textarea.focus();
      textarea.setSelectionRange(start, start + formattedText.length);
    }, 0);
  };

  const handleOutline = async () => {
    const topic = prompt('Тема статьи:', '') || '';
    const constraints = prompt('Пожелания/ограничения (необязательно):', '') || '';
    
    if (!topic) return;
    
    addLog('План — запрос', `${topic}\n${constraints}`);
    try {
      const { output } = await callAgent('outline', { project, topic, constraints });
      addLog('План — ответ', output);
      if (!(useStream && streamToEditor)) {
        setContent(prev => `${prev}\n\n${output}\n\n`);
      }
    } catch (error) {
      console.error('Outline error:', error);
    }
  };

  const handleDraft = async () => {
    const outline_point = prompt('Название раздела/подраздела для наброска:', '') || 'Раздел';
    addLog('Набросок — запрос', outline_point);
    
    try {
      const { output } = await callAgent('draft', { 
        project, 
        outline_point, 
        context_md: content 
      });
      addLog('Набросок — ответ', output);
      if (!(useStream && streamToEditor)) {
        setContent(prev => `${prev}\n\n${output}\n\n`);
      }
    } catch (error) {
      console.error('Draft error:', error);
    }
  };

  const handleImprove = async () => {
    const { text: selectedText, start, end } = getSelectedText();
    
    if (!selectedText.trim()) {
      alert('Выделите текст для улучшения');
      return;
    }

    // В предпросмотре требуем точного сопоставления в исходном тексте
    if (activeTab === 'preview' && (start < 0 || end < 0)) {
      alert('Не удалось однозначно сопоставить выделение в предпросмотре с исходным текстом. Пожалуйста, выполните замену в режиме Редактор.');
      return;
    }
    
    const originalLen = selectedText.length;
    const maxLen = Math.floor(originalLen * 1.15); // допускаем +15%
    const baseCriteria = selectedCriteria || 'Сделай яснее, компактнее, без потери смысла.';
    const strictGuard = `Строго: только копиэдитинг (орфография, пунктуация, стиль). Никаких новых фактов или рассуждений. Сохраняй Markdown и ссылки. Максимальная длина результата — ${maxLen} символов (исходник: ${originalLen}). Если текста слишком много — сократи, убери повторы.`;

    addLog('Улучшение — запрос', `${baseCriteria}\n\n${strictGuard} (фрагмент: ${selectedText.substring(0, 100)}...)`);

    try {
      const runImprove = async (extraGuard?: string) => {
        const payloadInstructions = extraGuard ? `${baseCriteria}\n\n${strictGuard}\n\n${extraGuard}` : `${baseCriteria}\n\n${strictGuard}`;
        const { output: raw } = await callAgent('improve', {
          project,
          text_md: selectedText,
          instructions: payloadInstructions
        }, { streamMode: 'off', applyToEditor: false });
        return (raw ?? '').trim();
      };

      let improved = await runImprove();

      if (!improved) {
        addLog('Улучшение — отклонено', 'Пустой ответ от агента. Оставил без изменений.');
        return;
      }

      if (improved.length > maxLen) {
        addLog('Улучшение — повтор', `Ответ слишком длинный (${improved.length} > ${maxLen}). Запрашиваю сжатую версию.`);
        improved = await runImprove('Сократи текст до объёма не больше исходника +10%. Удали повторения, лишние вводные и общие фразы.');
      }

      if (!improved) {
        addLog('Улучшение — отклонено', 'Повторный запрос дал пустой ответ. Оставил без изменений.');
        return;
      }

      if (improved.length > maxLen) {
        addLog('Улучшение — отклонено', `После повтора ответ всё ещё длиннее лимита (${improved.length} > ${maxLen}). Оставил без изменений.`);
        return;
      }

      addLog('Улучшение — ответ', improved);

      if (start >= 0 && end >= 0) {
        const newContent = content.slice(0, start) + improved + content.slice(end);
        setContent(newContent);

        if (activeTab === 'editor') {
          setTimeout(() => {
            const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
            if (textarea) {
              textarea.focus();
              textarea.setSelectionRange(start, start + improved.length);
            }
          }, 0);
        }
      }
  } catch (error) {
    console.error('Improve error:', error);
  }
  };

  const handleThesis = async () => {
    const { text: selectedText, start, end } = getSelectedText();
    const useSelection = !!selectedText.trim();
    const target = useSelection ? selectedText : content;

    if (!target.trim()) {
      alert('Нет текста для сжатия.');
      return;
    }

    addLog('Тезисы — запрос', `${useSelection ? 'Выделение' : 'Весь текст'} (${target.length} символов).`);

    try {
      const { output } = await callAgent('thesis', {
        project,
        text_md: target,
        mode: useSelection ? 'selection' : 'document'
      }, { streamMode: 'off', applyToEditor: false });

      const thesisRaw = (output ?? '').trim();
      if (!thesisRaw) {
        addLog('Тезисы — отклонено', 'Пустой ответ от агента. Изменения не внесены.');
        return;
      }

      const normalized = thesisRaw
        .split(/\r?\n/)
        .map(line => {
          const trimmed = line.trim();
          if (!trimmed) return '';
          return /^[-*]\s/.test(trimmed) ? trimmed : `- ${trimmed}`;
        })
        .filter(Boolean)
        .join('\n');

      addLog('Тезисы — ответ', normalized);

      if (useSelection && start >= 0 && end >= 0) {
        const newContent = content.slice(0, start) + normalized + content.slice(end);
        setContent(newContent);
        if (activeTab === 'editor') {
          setTimeout(() => {
            const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
            if (textarea) {
              textarea.focus();
              textarea.setSelectionRange(start, start + normalized.length);
            }
          }, 0);
        }
      } else {
        setContent(prev => {
          const trimmedPrev = prev.trimEnd();
          const block = `## Тезисы\n\n${normalized}`;
          return trimmedPrev ? `${trimmedPrev}\n\n${block}\n` : `${block}\n`;
        });
      }
  } catch (error) {
    console.error('Thesis error:', error);
    addLog('Тезисы — ошибка', error instanceof Error ? error.message : String(error));
  }
  };

  const handleSummary = async () => {
    const { text: selected, start, end } = getSelectedText();
    const useSelection = !!selected.trim();
    const target = useSelection ? selected : content;
    if (!target.trim()) {
      alert('Нет текста для суммирования.');
      return;
    }

    addLog('Конспект — запрос', useSelection ? 'По выделенному фрагменту' : 'По всему документу');

    try {
      const { text: summaryInput, truncated } = trimForAgent(target);
      if (truncated) {
        addLog('Конспект — заметка', `Текст сокращён до ${AGENT_CONTEXT_LIMIT} символов (${summaryInput.length}).`);
      }
      const { output } = await callAgent('summary', {
        project,
        text_md: summaryInput,
        mode: useSelection ? 'selection' : 'document'
      }, { streamMode: 'force', applyToEditor: false });

      const summary = (output ?? '').trim();
      if (!summary) {
        addLog('Конспект — отклонено', 'Агент вернул пустой ответ.');
        return;
      }

      const block = summary.split(/\r?\n/).map(line => {
        const trimmed = line.trim();
        if (!trimmed) return '';
        return trimmed.startsWith('-') ? trimmed : `- ${trimmed}`;
      }).filter(Boolean).join('\n');
      const formatted = `## Краткий конспект\n\n${block}${truncated ? '\n\n> _Конспект построен по усечённому тексту_' : ''}`;

      if (useSelection && start >= 0 && end >= 0) {
        const updated = content.slice(0, start) + formatted + content.slice(end);
        setContent(updated);
        setTimeout(() => {
          const textarea = document.querySelector('#editor') as HTMLTextAreaElement | null;
          if (textarea) {
            textarea.focus();
            textarea.setSelectionRange(start, start + formatted.length);
          }
        }, 0);
      } else {
        setContent(prev => `${prev.trimEnd()}\n\n${formatted}\n`);
      }
      addLog('Конспект — ответ', formatted);
    } catch (error) {
      console.error('Summary error:', error);
      addLog('Конспект — ошибка', error instanceof Error ? error.message : String(error));
    }
  };

  const handleQuestions = async () => {
    const { text: selected } = getSelectedText();
    const useSelection = !!selected.trim();
    const target = useSelection ? selected : content;
    if (!target.trim()) {
      alert('Нет текста для генерации вопросов.');
      return;
    }

    addLog('Вопросы — запрос', useSelection ? 'По выделенному фрагменту' : 'По всему документу');

    try {
      const { text: questionsInput, truncated } = trimForAgent(target);
      if (truncated) {
        addLog('Вопросы — заметка', `Текст сокращён до ${AGENT_CONTEXT_LIMIT} символов (${questionsInput.length}).`);
      }
      const { output } = await callAgent('questions', {
        project,
        text_md: questionsInput,
        mode: useSelection ? 'selection' : 'document'
      }, { streamMode: 'off', applyToEditor: false });

      const raw = (output ?? '').trim();
      if (!raw) {
        addLog('Вопросы — отклонено', 'Ответ пустой.');
        return;
      }

      const lines = raw.split(/\r?\n/).map(line => line.trim()).filter(Boolean);
      const formatted = lines.map((line, idx) => `${idx + 1}. ${line.replace(/^\d+\.?\s*/, '')}`).join('\n');
      const block = `## Вопросы для проверки\n\n${formatted}${truncated ? '\n\n> _Вопросы построены по усечённому тексту_' : ''}`;
      setContent(prev => `${prev.trimEnd()}\n\n${block}\n`);
      addLog('Вопросы — ответ', block);
    } catch (error) {
      console.error('Questions error:', error);
      addLog('Вопросы — ошибка', error instanceof Error ? error.message : String(error));
    }
  };

  const handleFactCheck = async () => {
    const { text: selected } = getSelectedText();
    const useSelection = !!selected.trim();
    const target = useSelection ? selected : content;
    if (!target.trim()) {
      alert('Нет текста для проверки.');
      return;
    }

    addLog('Факт-чекинг — запрос', useSelection ? 'По выделенному фрагменту' : 'По всему документу');

    const { text: factTarget, truncated } = trimForAgent(target);
    if (truncated) {
      addLog('Факт-чекинг — заметка', `Текст сокращён до ${AGENT_CONTEXT_LIMIT} символов (${factTarget.length}).`);
    }

    let catalogContext = '';
    if (useAgregator) {
      try {
        const res = await agregatorSearch((selected || project.title || '').slice(0, 120) || project.title || 'Текст', { manual: true });
        const lines = (res.items || []).slice(0, 5).map((item, idx) => `[#${idx + 1}] ${item.title || `file-${item.file_id}`}: ${(item.snippets || []).join(' ')}`);
        const answer = (res.answer || '').trim();
        catalogContext = [answer ? `Краткий ответ каталога:\n${answer}` : '', lines.length ? lines.join('\n') : ''].filter(Boolean).join('\n\n');
      } catch (error) {
        console.error('Ошибка каталога при факт-чекинге:', error);
      }
    }

    try {
      const { output } = await callAgent('factcheck', {
        project,
        text_md: factTarget,
        mode: useSelection ? 'selection' : 'document',
        catalog_context: catalogContext
      }, { streamMode: 'off', applyToEditor: false });

      const result = (output ?? '').trim();
      if (!result) {
        addLog('Факт-чекинг — отклонено', 'Ответ пустой.');
        return;
      }

      const title = useSelection ? 'Факт-чекинг выделенного фрагмента' : 'Факт-чекинг текста';
      const note = truncated ? '\n(Проверка выполнена по усечённой версии текста ~6000 символов)' : '';
      setContent(prev => `${prev.trimEnd()}\n\n## ${title}\n\n${result}${note}\n`);
      addLog('Факт-чекинг — ответ', result + note);
    } catch (error) {
      console.error('Fact-check error:', error);
      addLog('Факт-чекинг — ошибка', error instanceof Error ? error.message : String(error));
    }
  };

  const stopChainWorkflow = () => {
    if (!workflowRunning) return;
    chainAbortRef.current = true;
    if (abortCtrl) {
      try { abortCtrl.abort(); } catch (error) { console.error('Abort error:', error); }
      setAbortCtrl(null);
    }
    addLog('Chain — остановка', 'Ожидание завершения текущих операций...');
  };

  const handleReview = async () => {
    const instructions = selectedCriteria || 'Общая рецензия';
    addLog('Рецензия — запрос', `По критериям: ${instructions}`);
    
    try {
      const { output } = await callAgent('review', { 
        project, 
        text_md: content,
        instructions 
      });
      addLog('Рецензия — ответ', output);
      if (!(useStream && streamToEditor)) {
        setContent(prev => `${prev}\n\n> **Рецензия агента**\n>\n> ${output.replace(/\n/g, '\n> ')}\n\n`);
      }
    } catch (error) {
      console.error('Review error:', error);
    }
  };

  const handleChain = async () => {
    if (workflowRunning) {
      alert('Полная цепочка уже выполняется. Дождитесь завершения или нажмите «Остановить».');
      return;
    }

    const topic = project.title || 'Новый проект';
    const extraInput = prompt('Дополнительные требования (необязательно):', '');
    if (extraInput === null) {
      addLog('Chain — отменена', 'Пользователь отменил запуск.');
      return;
    }
    const extra = extraInput.trim();
    const criteria = selectedCriteria || 'Сделай яснее, компактнее, без воды; соблюдай ГОСТ.';
    const includeReview = confirm('Включить автоматическую рецензию и доработку?');
    const includeQuestions = confirm('Добавить блок контрольных вопросов?');
    const includeSummary = confirm('Добавить краткий конспект?');
    const includeThesis = confirm('Добавить перечень ключевых тезисов?');
    const includeFactCheck = useAgregator ? confirm('Запустить факт-чекинг с использованием каталога?') : false;

    const catalogNotes: string[] = [];
    const drafts: string[] = [];
    let planText = '';
    let improvedText = '';
    let reviewText = '';
    let questionsText = '';
    let summaryText = '';
    let thesisText = '';
    let factBlock = '';

    const checkAbort = () => {
      if (chainAbortRef.current) {
        throw new Error('chain_aborted');
      }
    };

    chainAbortRef.current = false;
    setWorkflowRunning(true);
    setStatus('working');
    addLog('Chain — старт', `${topic}${extra ? ` (дополнительно: ${extra})` : ''}`);

    try {
      const outlinePayload: AgentPayloadShape = { project, topic, constraints: extra };
      const planResp = await callAgent('outline', outlinePayload, { streamMode: 'off', applyToEditor: false });
      planText = (planResp.output || '').trim();
      addLog('Chain — план', planText);
      checkAbort();

      const sectionTitles = extractSections(planText, 6);
      for (const title of sectionTitles) {
        checkAbort();
        let contextMd = content;
        if (useAgregator) {
          try {
            const res = await agregatorSearch(`${title} ${topic}`.slice(0, 160), { manual: true });
            const answer = (res.answer || '').trim();
            const limitForNotes = Math.max(1, Math.min(3, aggTopK));
            const topItems = (res.items || []).slice(0, limitForNotes);
            const lines = topItems.map((item, idx) => {
              const baseTitle = item.title || `file-${item.file_id}`;
              const rawSnippet = (item.snippets || []).join(' ');
              const normalized = rawSnippet.replace(/\s+/g, ' ').slice(0, 240);
              return `[#${idx + 1}] ${baseTitle}: ${normalized}${rawSnippet.length > 240 ? '…' : ''}`;
            });
            const snippetBlock = [answer ? `Ответ каталога\n${answer}` : '', lines.length ? lines.join('\n') : ''].filter(Boolean).join('\n\n');
            if (snippetBlock) {
              const note = `### ${title}\n${snippetBlock}`;
              const existingIdx = catalogNotes.findIndex(entry => entry.startsWith(`### ${title}\n`));
              if (existingIdx >= 0) catalogNotes[existingIdx] = note;
              else catalogNotes.push(note);
              addLog('Chain — каталог', `${title}\n${snippetBlock}`);
              contextMd = `${contextMd}\n\nКаталог Agregator:\n${snippetBlock}`;
            }
          } catch (error) {
            console.error('Chain aggregator error:', error);
          }
        }
        checkAbort();

        const draftResp = await callAgent('draft', {
          project,
          outline_point: title,
          context_md: contextMd,
        }, { streamMode: 'off', applyToEditor: false });
        drafts.push(`## ${title}\n\n${(draftResp.output || '').trim()}`);
        addLog('Chain — набросок', `${title}: ${(draftResp.output || '').trim().slice(0, 140)}...`);
      }
      checkAbort();

      const combinedDraft = drafts.length ? drafts.join('\n\n') : content;
      const improveResp = await callAgent('improve', {
        project,
        text_md: combinedDraft,
        instructions: `${criteria}\n\nОбъедини разделы в цельный текст, добавь переходы и вывод. Согласуй терминологию, устрани повторы. Укажи практическую значимость.`,
      }, { streamMode: 'off', applyToEditor: false });
      improvedText = (improveResp.output || combinedDraft).trim();
      checkAbort();

      if (includeReview) {
        const reviewResp = await callAgent('review', {
          project,
          text_md: improvedText,
          instructions: criteria,
        }, { streamMode: 'off', applyToEditor: false });
        reviewText = (reviewResp.output || '').trim();
        addLog('Chain — рецензия', reviewText || 'Рецензия отсутствует.');
        checkAbort();

        if (reviewText) {
          const fixResp = await callAgent('improve', {
            project,
            text_md: improvedText,
            instructions: `${criteria}\n\nПримени следующие рекомендации рецензента:\n${reviewText}`,
          }, { streamMode: 'off', applyToEditor: false });
          improvedText = (fixResp.output || improvedText).trim();
        }
      } else {
        addLog('Chain — пропуск', 'Рецензия отключена пользователем.');
      }
      checkAbort();

      if (includeQuestions) {
        const questionsSrc = trimForAgent(improvedText);
        const questionsResp = await callAgent('questions', {
          project,
          text_md: questionsSrc.text,
          mode: 'document',
        }, { streamMode: 'off', applyToEditor: false });
        questionsText = (questionsResp.output || '').trim();
        if (questionsSrc.truncated) {
          questionsText += '\n\n> _Вопросы построены по усечённому тексту_';
        }
      } else {
        addLog('Chain — пропуск', 'Блок вопросов отключён.');
      }
      checkAbort();

      if (includeSummary) {
        const summarySrc = trimForAgent(improvedText);
        const summaryResp = await callAgent('summary', {
          project,
          text_md: summarySrc.text,
          mode: 'document',
        }, { streamMode: 'off', applyToEditor: false });
        summaryText = (summaryResp.output || '').trim();
        if (summarySrc.truncated) {
          summaryText += '\n\n> _Конспект построен по усечённому тексту_';
        }
      } else {
        addLog('Chain — пропуск', 'Конспект отключён.');
      }
      checkAbort();

      if (includeThesis) {
        const thesisSrc = trimForAgent(improvedText);
        const thesisResp = await callAgent('thesis', {
          project,
          text_md: thesisSrc.text,
          mode: 'document',
        }, { streamMode: 'off', applyToEditor: false });
        thesisText = (thesisResp.output || '').trim();
        if (thesisSrc.truncated) {
          thesisText += '\n\n> _Тезисы составлены по усечённому тексту_';
        }
      }
      checkAbort();

      if (includeFactCheck) {
        try {
          const factSrc = trimForAgent(improvedText);
          const factResp = await callAgent('factcheck', {
            project,
            text_md: factSrc.text,
            mode: 'document',
            catalog_context: catalogNotes.join('\n\n'),
          }, { streamMode: 'off', applyToEditor: false });
          factBlock = (factResp.output || '').trim();
          if (factSrc.truncated) {
            factBlock += '\n\n(Факт-чекинг выполнен по усечённому тексту)';
          }
        } catch (error) {
          console.error('Chain fact-check error:', error);
        }
      } else if (useAgregator) {
        addLog('Chain — пропуск', 'Факт-чекинг отключён.');
      }
      checkAbort();

      const finalSections = buildChainDocument({
        planText,
        catalogNotes,
        drafts,
        improvedText,
        reviewText,
        questionsText,
        summaryText,
        thesisText,
        factBlock,
      });

      if (finalSections) {
        setContent(prev => `${prev.trimEnd()}\n\n${finalSections}\n`);
      }
      setActiveTab('editor');
      addLog('Chain — завершено', 'Результаты добавлены в документ.');
    } catch (error) {
      const err = error as Error;
      const aborted = chainAbortRef.current || err?.message === 'chain_aborted' || /aborted/i.test(err?.message || '');
      const partialSections = buildChainDocument({
        planText,
        catalogNotes,
        drafts,
        improvedText,
        reviewText,
        questionsText,
        summaryText,
        thesisText,
        factBlock,
      });
      if (partialSections) {
        setContent(prev => `${prev.trimEnd()}\n\n${partialSections}\n`);
      }
      if (aborted) {
        addLog('Chain — отменена', partialSections ? 'Промежуточный результат добавлен.' : 'Процесс остановлен до генерации текста.');
      } else {
        console.error('Chain error:', error);
        addLog('Chain — ошибка', err?.message || String(error));
      }
    } finally {
      setWorkflowRunning(false);
      chainAbortRef.current = false;
      setStatus('ready');
    }
  };

  const handleTemplate = async () => {
    if (!selectedTemplate) return;
    const TPLS: Record<string, string> = {
      'ГОСТ_Статья': `# Название статьи

## Аннотация
Краткое содержание (5–7 предложений).

## Введение
Актуальность, цель, задачи, объект/предмет.

## Обзор литературы
Ключевые источники, пробелы [@key2020].

## Методы
Модель, алгоритм, допущения.

## Результаты
Основные находки, таблицы/рисунки.

## Обсуждение
Сравнение с работами, ограничения.

## Заключение
Выводы, новизна, практическая значимость.

## Список литературы
(генерируется автоматически по [@citekey])
`,
      'ГОСТ_Отчёт_НИР': `# Отчёт по НИР

## Введение
Цель, задачи, показатели.

## Аналитический обзор
Состояние проблемы, анализ решений [@petrov2019].

## Теоретическая часть
Постановка и формализация.

## Экспериментальная часть
План эксперимента, стенд, метрики.

## Результаты и обсуждение
Таблицы/графики, интерпретация.

## Выводы и рекомендации
Краткие тезисы.

## Список сокращений
…

## Список литературы
(автогенерация из BibTeX)
`
    };
    const template = TPLS[selectedTemplate] || '';
    setContent(prev => `${prev}\n\n${template}\n\n`);
  };

  const handleCopy = async () => {
    try {
      await navigator.clipboard.writeText(content);
      setStatus('ready');
      setTimeout(() => setStatus('ready'), 1200);
    } catch (error) {
      console.error('Copy error:', error);
    }
  };

  const handleExport = async (format: 'md' | 'docx') => {
    const filename = prompt('Имя файла:', 'article') || 'article';
    
    try {
      if (format === 'md') {
        const blob = new Blob([content], { type: 'text/markdown;charset=utf-8' });
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = url;
        a.download = `${filename}.md`;
        a.click();
        URL.revokeObjectURL(url);
        return;
      }

      if (format === 'docx') {
        // Build paragraphs, lists, and tables
        const lines = content.split('\n');
        const children: (Paragraph | DocxTable)[] = [];
        let currentTable: string[][] = [];
        const flushTable = () => {
          if (currentTable.length === 0) return;
          const rows = currentTable.map(r => new DocxTableRow({ children: r.map(c => new DocxTableCell({ width: { size: 100, type: WidthType.PERCENTAGE }, children: [new Paragraph({ children: [new TextRun(c)] })] })) }));
          children.push(new DocxTable({ width: { size: 100, type: WidthType.PERCENTAGE }, rows }));
          currentTable = [];
        };
        lines.forEach(raw => {
          const line = raw.replace(/\r$/, '');
          if (/^\|.*\|$/.test(line)) {
            const cells = line.split('|').slice(1, -1).map(s => s.trim());
            currentTable.push(cells);
            return;
          } else { flushTable(); }
          const h1 = line.match(/^#\s+(.*)/);
          const h2 = line.match(/^##\s+(.*)/);
          const h3 = line.match(/^###\s+(.*)/);
          if (h1) { children.push(new Paragraph({ text: h1[1], heading: HeadingLevel.HEADING_1 })); return; }
          if (h2) { children.push(new Paragraph({ text: h2[1], heading: HeadingLevel.HEADING_2 })); return; }
          if (h3) { children.push(new Paragraph({ text: h3[1], heading: HeadingLevel.HEADING_3 })); return; }
          if (/^-\s+/.test(line)) { children.push(new Paragraph({ children: [new TextRun('• ' + line.replace(/^-\s+/, ''))] })); return; }
          if (/^\d+\.\s+/.test(line)) { children.push(new Paragraph({ children: [new TextRun(line)] })); return; }
          if (line.trim() === '') { children.push(new Paragraph({ children: [new TextRun('')] })); return; }
          children.push(new Paragraph({ children: [new TextRun(line)] }));
        });
        flushTable();
        const doc = new DocxDocument({
          sections: [{
            properties: {
              page: { margin: { top: 1134, right: 1134, bottom: 1134, left: 1134 } }
            },
            children
          }],
          styles: {
            default: {
              document: {
                run: { font: 'Times New Roman', size: 28 },
                paragraph: { spacing: { line: 276 } }
              }
            }
          }
        });
        const blob = await Packer.toBlob(doc);
        const url = URL.createObjectURL(blob);
        setExportUrl(url);
        setExportToast('Экспорт завершён: DOCX');
        const a = document.createElement('a');
        a.href = url; a.download = `${filename}.docx`; a.click();
        return;
      }
    } catch (error) {
      console.error(`Export ${format} error:`, error);
      setExportToast('Ошибка экспорта');
    }
  };

  const saveProject = () => {
    const projectData = {
      ...project,
      language: project.language,
      content,
      bibText, // Добавляем bibText в сохраняемый проект
      updated_at: new Date().toISOString(),
      _ui: {
        selectedCriteria,
        templatesMap,
        bibText
      }
    };
    
    const dataStr = JSON.stringify(projectData, null, 2);
    const blob = new Blob([dataStr], { type: 'application/json' });
    const url = URL.createObjectURL(blob);
    const a = document.createElement('a');
    a.href = url;
    a.download = `${project.title.replace(/[^a-zA-Zа-яА-Я0-9]/g, '_')}.json`;
    a.click();
    URL.revokeObjectURL(url);
  };

  const loadProject = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const projectData = JSON.parse(e.target?.result as string);
        setProject({
          title: projectData.title || project.title,
          language: projectData.language || project.language,
          style_guide: projectData.style_guide || project.style_guide,
          persona: projectData.persona || project.persona,
          content: projectData.content || ''
        });
        setContent(projectData.content || '');
        setBibText(projectData.bibText || ""); // Восстанавливаем bibText из загруженного проекта
        if (projectData._ui?.selectedCriteria) setSelectedCriteria(projectData._ui.selectedCriteria);
        if (projectData._ui?.templatesMap) setTemplatesMap(projectData._ui.templatesMap);
        if (projectData._ui?.bibText) {
          setBibText(projectData._ui.bibText);
          try { setBibDb(parseBibTeX(projectData._ui.bibText)); } catch { /* ignore invalid saved bib */ }
        }
        addLog('Проект загружен', `${projectData.title || 'Без названия'}`);
      } catch (error) {
        console.error('Ошибка загрузки проекта:', error);
        addLog('Ошибка', 'Не удалось загрузить проект');
      }
    };
    reader.readAsText(file);
  };

  // Загрузка .bib файла: добавляем кнопку ввода в UI ниже через существующий sidebar экспорт
  const onLoadBib = (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = (e) => {
      try {
        const text = String(e.target?.result || '');
        setBibText(text);
        const db = parseBibTeX(text);
        setBibDb(db);
        addLog('BibTeX', `Загружено записей: ${Object.keys(db).length}`);
        updateBibliography();
      } catch (err) {
        console.error('BibTeX load error:', err);
        addLog('Ошибка', 'Не удалось загрузить .bib');
      }
    };
    reader.readAsText(file);
  };

  const triggerReviewImport = () => {
    reviewFileInputRef.current?.click();
  };

  const onImportReview = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;
    try {
      const extracted = await extractTextForReviewFile(file);
      const text = extracted.trim();
      if (!text) {
        addLog('Импорт для рецензии', `Не удалось извлечь текст из ${file.name}`);
        return;
      }
      const defaultLimit = '8000';
      const limitInput = prompt('Максимальное количество символов для рецензии (например, 8000). Оставьте пустым для значения по умолчанию.', defaultLimit);
      if (limitInput === null) {
        addLog('Импорт для рецензии', 'Загрузка отменена пользователем.');
        return;
      }
      const sanitized = (limitInput || defaultLimit).replace(/[^0-9]/g, '');
      const limit = Math.max(1000, parseInt(sanitized, 10) || parseInt(defaultLimit, 10));
      const limitedText = text.length > limit ? `${text.slice(0, limit)}\n\n(…текст усечён до ${limit} символов для рецензии…)` : text;
      if (text.length > limit) {
        addLog('Импорт для рецензии', `Текст усечён до ${limit} символов (из ${text.length}).`);
      }
      const originalBlock = `## Документ для рецензии (${file.name})\n\n${text}`;
      const reviewBlock = `## Текст для рецензии\n\n${limitedText}`;
      const base = content.trimEnd();
      let updatedDoc = base ? `${base}\n\n${originalBlock}\n\n${reviewBlock}\n` : `${originalBlock}\n\n${reviewBlock}\n`;
      setContent(updatedDoc);
      addLog('Импорт для рецензии', `Загружен ${file.name}`);
      setActiveTab('editor');
      const reviewResp = await callAgent('review', {
        project,
        text_md: `${reviewBlock}\n`,
        instructions: selectedCriteria
      }, { streamMode: 'off', applyToEditor: false });
      const reviewText = (reviewResp.output || '').trim();
      if (reviewText) {
        updatedDoc = `${updatedDoc.trimEnd()}\n\n## Рецензия импортированного документа\n\n${reviewText}\n`;
        setContent(updatedDoc);
        addLog('Импорт для рецензии', 'Рецензия сгенерирована.');
      }
    } catch (error) {
      console.error('Ошибка импорта для рецензии:', error);
      addLog('Импорт для рецензии', `Ошибка: ${error instanceof Error ? error.message : String(error)}`);
    } finally {
      if (event.target) event.target.value = '';
    }
  };

  const clearCache = () => {
    setCache(new Map());
    localStorage.removeItem('llm-writer-cache');
    addLog('Кеш очищен', 'Все кешированные ответы удалены');
  };

  const getStatusIcon = () => {
    switch (status) {
      case 'working': return <Clock className="w-4 h-4 animate-spin" />;
      case 'error': return <AlertCircle className="w-4 h-4 text-red-500" />;
      default: return <CheckCircle className="w-4 h-4 text-green-500" />;
    }
  };

  const getStatusText = () => {
    switch (status) {
      case 'working': return 'В работе...';
      case 'error': return 'Ошибка';
      default: return 'Готово';
    }
  };

  const filteredLogs = useMemo(() => {
    if (!searchTerm) return logs;
    return logs.filter(log => 
      log.title.toLowerCase().includes(searchTerm.toLowerCase()) ||
      log.body.toLowerCase().includes(searchTerm.toLowerCase())
    );
  }, [logs, searchTerm]);

  // Отсутствующие citekey из [@key]
  const missingCites = useMemo(() => {
    const set = new Set<string>();
    const re = /\[@([A-Za-z0-9_:-]+)\]/g;
    let m: RegExpExecArray | null;
    while ((m = re.exec(content))) {
      const key = m[1];
      if (!bibDb[key]) set.add(key);
    }
    return set;
  }, [content, bibDb]);

  return (
    <div className={`min-h-screen transition-colors duration-300 ${isDarkMode ? 'dark bg-gray-900' : 'bg-gray-50'}`}>
      {/* Header */}
      <header className="bg-white dark:bg-gray-800 shadow-sm border-b border-gray-200 dark:border-gray-700 sticky top-0 z-50">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <div className="flex items-center">
              <Brain className="w-8 h-8 text-blue-600 dark:text-blue-400 mr-3" />
              <div>
                <h1 className="text-xl font-bold text-gray-900 dark:text-white">
                  LLM Writer Agent {autoSaveEnabled && (<span title="Автосохранение" className={`${savingPulse ? 'opacity-100' : 'opacity-40'} transition-opacity text-sm`}>💾</span>)}
                </h1>
                <p className="text-sm text-gray-500 dark:text-gray-400">
                  Made by Ryltsin.I.A
                </p>
              </div>
            </div>
            
            <div className="flex items-center space-x-2">
              <button
                onClick={() => setLeftOpen(v => !v)}
                className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors lg:hidden"
                title={leftOpen ? 'Скрыть левую панель' : 'Показать левую панель'}
              >
                <BookOpen className="w-5 h-5" />
              </button>
              <button
                onClick={() => setRightOpen(v => !v)}
                className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors lg:hidden"
                title={rightOpen ? 'Скрыть правую панель' : 'Показать правую панель'}
              >
                <Settings className="w-5 h-5" />
              </button>
              <button
                onClick={() => setFocusMode(v => !v)}
                className={`p-2 rounded-lg transition-colors ${focusMode ? 'bg-indigo-100 dark:bg-indigo-900 text-indigo-600 dark:text-indigo-300' : 'hover:bg-gray-100 dark:hover:bg-gray-700'}`}
                title={focusMode ? 'Выйти из режима без отвлечений' : 'Режим без отвлечений'}
              >
                <Eye className="w-5 h-5" />
              </button>

              <div className="hidden sm:flex items-center space-x-2 text-sm text-gray-600 dark:text-gray-300">
                {getStatusIcon()}
                <span>{getStatusText()}</span>
                <span className="text-gray-400 dark:text-gray-600">|</span>
                <span>Символов: {content.length.toLocaleString('ru-RU')}</span>
              </div>

              <button onClick={() => handleExport('docx')} className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors" title="Экспорт в DOCX">
                <FileText className="w-5 h-5" />
              </button>
              <button onClick={saveProject} className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors" title="Сохранить проект">
                <Save className="w-5 h-5" />
              </button>

              <button
                onClick={() => setAutoSaveEnabled(!autoSaveEnabled)}
                className={`p-2 rounded-lg transition-colors ${
                  autoSaveEnabled 
                    ? 'bg-green-100 dark:bg-green-900 text-green-600 dark:text-green-400' 
                    : 'hover:bg-gray-100 dark:hover:bg-gray-700 text-gray-500'
                }`}
                title={autoSaveEnabled ? 'Автосохранение включено' : 'Автосохранение выключено'}
              >
                <Save className="w-4 h-4" />
              </button>
              
              <button
                onClick={() => setIsDarkMode(!isDarkMode)}
                className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                title="Переключить тему"
              >
                {isDarkMode ? <Sun className="w-5 h-5" /> : <Moon className="w-5 h-5" />}
              </button>
              <button
                onClick={() => setShowHelp(true)}
                className="p-2 rounded-lg hover:bg-gray-100 dark:hover:bg-gray-700 transition-colors"
                title="Справка по AIWord"
              >
                <HelpCircle className="w-5 h-5" />
              </button>
            </div>
          </div>
        </div>
      </header>

      {/* Help modal */}
      {showHelp && (
        <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/60 px-4">
          <div className="max-w-3xl w-full bg-white dark:bg-gray-900 rounded-xl shadow-xl border border-gray-200 dark:border-gray-700 p-6 overflow-y-auto max-h-[80vh]">
            <div className="flex items-start justify-between mb-4">
              <div>
                <h2 className="text-xl font-semibold text-gray-900 dark:text-white">AIWord — справка</h2>
                <p className="text-sm text-gray-600 dark:text-gray-400">Краткое описание функционала приложения и режимов агентов.</p>
              </div>
              <button onClick={() => setShowHelp(false)} className="text-gray-500 hover:text-gray-800 dark:text-gray-400 dark:hover:text-gray-200">
                ✕
              </button>
            </div>
            <div className="space-y-4 text-sm text-gray-700 dark:text-gray-300">
              <section>
                <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-1">О проекте</h3>
                <p>AIWord — рабочая среда для подготовки научных материалов. Приложение поддерживает Markdown, встроенную библиографию, интеграцию с каталогом Agregator и набор агентных режимов для генерации, редактирования и проверки текста.</p>
              </section>
              <section>
                <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-1">Основные режимы агента</h3>
                <ul className="list-disc pl-5 space-y-1">
                  <li><strong>План</strong> — строит подробную структуру документа с аннотациями.</li>
                  <li><strong>Набросок</strong> — развивает выбранный раздел с опорой на текущий контент.</li>
                  <li><strong>Улучшить</strong> — переписывает выделенный фрагмент по выбранным критериям (текст автоматически сокращается при необходимости).</li>
                  <li><strong>Конспект</strong> — формирует краткий список тезисов; при длинном тексте использует усечённую версию.</li>
                  <li><strong>Тезисы</strong> — создаёт короткие буллеты для вставки в документ.</li>
                  <li><strong>Вопросы</strong> — генерирует контрольные вопросы для самопроверки.</li>
                  <li><strong>Факт-чекинг</strong> — анализирует документ и отмечает утверждения как «Подтверждено», «Требует проверки» или «Не найдено». При активации каталога Agregator использует найденные фрагменты в качестве источников.</li>
                  <li><strong>Рецензия</strong> — готовит полноформатный отзыв и чек-лист доработок.</li>
                  <li><strong>Импорт для рецензии</strong> — загружает внешний документ (TXT/MD/DOCX/PDF), автоматически ограничивает объём и строит рецензию по тексту.</li>
                </ul>
              </section>
              <section>
                <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-1">Полная цепочка</h3>
                <p>Комбинированный сценарий выполняет последовательность действий: планирование, генерацию черновиков, улучшение текста, опциональные рецензия, вопросы, конспект, тезисы и факт-чекинг. Перед запуском цепочка предлагает выбрать необходимые шаги. Процесс можно остановить — все успевшие сгенерироваться блоки сохраняются в редакторе.</p>
              </section>
              <section>
                <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-1">Работа с длинными текстами</h3>
                <p>Для предотвращения переполнения контекста LLM длинные документы автоматически сокращаются (по умолчанию до {AGENT_CONTEXT_LIMIT.toLocaleString('ru-RU')} символов). Журнал фиксирует факт усечения, а в итоговых блоках добавляется примечание.</p>
              </section>
            </div>
            <div className="mt-6 text-right">
              <button onClick={() => setShowHelp(false)} className="px-4 py-2 rounded-lg bg-blue-600 text-white hover:bg-blue-700">Закрыть</button>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <div className="max-w-[1800px] mx-auto px-2 sm:px-4 lg:px-6 py-4">
        {useStream && liveText && (
          <div className="mb-3 bg-white dark:bg-gray-800 rounded-xl shadow-sm p-3 border border-gray-200 dark:border-gray-700">
            <div className="text-sm font-medium text-gray-700 dark:text-gray-300 mb-1">Промежуточный ответ LLM</div>
            <pre className="whitespace-pre-wrap text-sm text-gray-800 dark:text-gray-100 max-h-48 overflow-auto">{liveText}</pre>
          </div>
        )}
        <div className={`grid grid-cols-1 gap-4 ${focusMode ? 'lg:grid-cols-[1fr]' : 'lg:grid-cols-[minmax(0,300px)_minmax(0,1fr)_minmax(0,300px)]'}`}>
          {/* Left Panel (fixed content) */}
          {!focusMode && (
          <div className={`${leftOpen ? '' : 'hidden lg:block'} space-y-4 lg:sticky lg:top-20 self-start max-h-[calc(100vh-100px)] overflow-y-auto`}>
            {/* Agent Actions */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Sparkles className="w-5 h-5 mr-2" />
                Действия агента
              </h2>
              
              <div className="grid grid-cols-2 gap-2 mb-4">
                <button onClick={handleOutline} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Сгенерировать план статьи">
                  <FileText className="w-4 h-4 mr-1" />
                  План
                </button>
                <button onClick={handleDraft} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 focus:ring-2 focus:ring-green-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Набросок раздела по контексту">
                  <PenTool className="w-4 h-4 mr-1" />
                  Набросок
                </button>
                <button onClick={handleImprove} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-yellow-600 text-white rounded-lg hover:bg-yellow-700 focus:ring-2 focus:ring-yellow-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="✨ УЛУЧШИТЬ: Работает с выделенным текстом. Улучшает стиль и структуру по выбранным критериям без расширения.">
                  <Edit3 className="w-4 h-4 mr-1" />
                  Улучшить
                </button>
                <button onClick={handleSummary} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-sky-600 text-white rounded-lg hover:bg-sky-700 focus:ring-2 focus:ring-sky-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Конспект: делает краткое резюме выделения или всего текста">
                  <ListOrdered className="w-4 h-4 mr-1" />
                  Конспект
                </button>
                <button onClick={handleThesis} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-teal-600 text-white rounded-lg hover:bg-teal-700 focus:ring-2 focus:ring-teal-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Тезисы: преобразует выделение в краткий список">
                  <List className="w-4 h-4 mr-1" />
                  Тезисы
                </button>
                <button onClick={handleQuestions} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-rose-600 text-white rounded-lg hover:bg-rose-700 focus:ring-2 focus:ring-rose-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Вопросы: формирует вопросы для проверки понимания текста">
                  <HelpCircle className="w-4 h-4 mr-1" />
                  Вопросы
                </button>
                <button onClick={handleFactCheck} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-red-600 text-white rounded-lg hover:bg-red-700 focus:ring-2 focus:ring-red-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Факт-чекинг: проверяет утверждения с опорой на каталог">
                  <ShieldCheck className="w-4 h-4 mr-1" />
                  Факт-чекинг
                </button>
                <button onClick={handleReview} disabled={status === 'working'} className="flex items-center justify-center px-3 py-2 bg-purple-600 text-white rounded-lg hover:bg-purple-700 focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors" title="Редакторская рецензия и чек-лист">
                  <MessageSquare className="w-4 h-4 mr-1" />
                  Рецензия
                </button>
              </div>

              <div className="flex items-center gap-2 mb-2">
                <button onClick={triggerReviewImport} className="flex-1 flex items-center justify-center px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 transition-colors" title="Загрузить текст из файла и получить рецензию">
                  <Upload className="w-4 h-4 mr-1" />
                  Импорт для рецензии
                </button>
                <input ref={reviewFileInputRef} type="file" accept=".txt,.md,.json,.docx,.pdf" className="hidden" onChange={onImportReview} />
              </div>
              
              <button onClick={handleChain} disabled={status === 'working' || workflowRunning} className="w-full flex items-center justify-center px-4 py-2 bg-gradient-to-r from-purple-600 to-blue-600 text-white rounded-lg hover:from-purple-700 hover:to-blue-700 focus:ring-2 focus:ring-purple-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-all" title="План → Черновик → Правка по критериям">
                <Play className="w-4 h-4 mr-2" />
                Полная цепочка
              </button>

              {workflowRunning && (
                <button onClick={stopChainWorkflow} className="w-full mt-2 flex items-center justify-center px-4 py-2 border border-red-500 text-red-600 dark:text-red-300 rounded-lg hover:bg-red-50 dark:hover:bg-red-900/30 focus:ring-2 focus:ring-red-400 focus:ring-offset-2 transition-colors" title="Остановить текущий процесс">
                  <StopCircle className="w-4 h-4 mr-2" />
                  Остановить цепочку
                </button>
              )}
              
              <p className="text-xs text-gray-500 dark:text-gray-400 mt-2">
                Используется выбранный профиль критериев
              </p>
            </div>

            {/* Agregator integration */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                <Database className="w-5 h-5 mr-2" />
                Каталог Agregator
              </h2>
              <div className="flex items-center justify-between mb-2">
                <label className="flex items-center space-x-2 text-sm">
                  <input type="checkbox" className="rounded" checked={useAgregator} onChange={(e)=>setUseAgregator(e.target.checked)} />
                  <span>Использовать данные каталога</span>
                </label>
                <a href="/app" target="_blank" rel="noopener" className="text-xs text-blue-600 dark:text-blue-300 hover:underline">Открыть каталог</a>
              </div>
              <div className="flex items-center justify-between mb-2 text-sm">
                <label className="flex items-center space-x-2">
                  <input type="checkbox" className="rounded" checked={useStream} onChange={(e)=>setUseStream(e.target.checked)} />
                  <span>Стрим LLM (показывать бегущие токены)</span>
                </label>
                {abortCtrl && (
                  <button onClick={()=>{ abortCtrl.abort(); setAbortCtrl(null); }} className="text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600">Остановить поток</button>
                )}
              </div>
              <div className="flex items-center justify-between mb-2 text-sm">
                <label className="flex items-center space-x-2">
                  <input type="checkbox" className="rounded" checked={streamToEditor} onChange={(e)=>setStreamToEditor(e.target.checked)} />
                  <span>Стрим в редактор (live‑вставка)</span>
                </label>
              </div>
              {useAgregator && (
                <div className="space-y-3">
                  <div>
                    <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Запрос каталога</label>
                    <textarea
                      value={aggQuery}
                      onChange={(e)=>{ setAggQueryTouched(true); setAggQuery(e.target.value); }}
                      rows={2}
                      className="w-full text-sm border border-gray-300 dark:border-gray-600 rounded-lg px-3 py-2 dark:bg-gray-700 dark:text-white"
                      placeholder="Например: устойчивое развитие городов, цифровая трансформация образования"
                    />
                    <div className="flex flex-wrap gap-2 mt-2">
                      <button onClick={runManualAggSearch} disabled={aggBusy} className="px-3 py-1.5 text-xs bg-indigo-600 text-white rounded hover:bg-indigo-700 disabled:opacity-60">
                        {aggBusy ? 'Идёт поиск…' : 'Искать'}
                      </button>
                      <button onClick={takeAggQueryFromSelection} className="px-3 py-1.5 text-xs border border-gray-300 dark:border-gray-600 rounded">Из выделения</button>
                      <button onClick={takeAggQueryFromTitle} className="px-3 py-1.5 text-xs border border-gray-300 dark:border-gray-600 rounded">Из названия</button>
                    </div>
                  </div>
                  <button onClick={()=>setAggPanelOpen(v=>!v)} className="w-full text-left text-xs px-3 py-2 rounded border border-gray-300 dark:border-gray-600 hover:bg-gray-50 dark:hover:bg-gray-700/40">
                    {aggPanelOpen ? 'Скрыть расширенные параметры' : 'Расширенные параметры поиска'}
                  </button>
                  {aggPanelOpen && (
                    <div className="space-y-2">
                      <label className="flex items-center gap-2 text-sm">
                        <input type="checkbox" className="rounded" checked={aggDeepSearch} onChange={(e)=>setAggDeepSearch(e.target.checked)} />
                        <span>Глубокий поиск по тексту (читает документ кусками)</span>
                      </label>
                      <div>
                        <div className="text-xs text-gray-500 dark:text-gray-400 mb-1">Коллекции (по умолчанию — доступные для поиска)</div>
                        <div className="max-h-28 overflow-auto border border-gray-200 dark:border-gray-700 rounded p-2 space-y-1">
                          {aggCollections.map(c=> (
                            <label key={c.id} className="flex items-center text-sm gap-2">
                              <input type="checkbox" className="rounded" checked={aggSelected.includes(c.id)} onChange={(e)=>{
                                setAggSelected(prev=> e.target.checked ? [...prev, c.id] : prev.filter(x=>x!==c.id));
                              }} />
                              <span className="truncate">{c.name}</span>
                              <span className="text-xs text-gray-500">{typeof c.count==='number'? `(${c.count})`: ''}</span>
                            </label>
                          ))}
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="text-xs text-gray-500 dark:text-gray-400">Типы (через запятую)</label>
                          <input className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" placeholder="article, dissertation…" value={aggTypes} onChange={e=>setAggTypes(e.target.value)} />
                        </div>
                        <div>
                          <label className="text-xs text-gray-500 dark:text-gray-400">Теги (k=v; через , ; или перенос)</label>
                          <input className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" placeholder="author=Иванов, organization=МИЭТ" value={aggTags} onChange={e=>setAggTags(e.target.value)} />
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="text-xs text-gray-500 dark:text-gray-400">Подсказка типа</label>
                          <select className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" value={''} onChange={(e)=>{ const v = e.target.value; if (v) setAggTypes(t=> t? (t+','+v) : v); }}>
                            <option value="">—</option>
                            {facetTypes.map(ft=> (<option key={ft.name} value={ft.name}>{ft.name} ({ft.count})</option>))}
                          </select>
                        </div>
                        <div className="grid grid-cols-2 gap-2">
                          <div>
                            <label className="text-xs text-gray-500 dark:text-gray-400">Ключ тега</label>
                            <select className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" value={facetKey} onChange={(e)=>{ setFacetKey(e.target.value); setFacetValue(''); }}>
                              {Object.keys(facetTags).map(k=> (<option key={k} value={k}>{k}</option>))}
                            </select>
                          </div>
                          <div>
                            <label className="text-xs text-gray-500 dark:text-gray-400">Значение</label>
                            <div className="flex gap-1">
                              <select className="flex-1 text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" value={facetValue} onChange={(e)=> setFacetValue(e.target.value)}>
                                <option value="">—</option>
                                {(facetTags[facetKey]||[]).map(v=> (<option key={v.value} value={v.value}>{v.value} ({v.count})</option>))}
                              </select>
                              <button className="px-2 py-1 border rounded" onClick={()=>{ if (facetKey && facetValue) setAggTags(t=> t? `${t}, ${facetKey}=${facetValue}` : `${facetKey}=${facetValue}`); }}>+</button>
                            </div>
                          </div>
                        </div>
                      </div>
                      <div className="grid grid-cols-2 gap-2">
                        <div>
                          <label className="text-xs text-gray-500 dark:text-gray-400">Год от</label>
                          <input className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" value={aggYearFrom} onChange={e=>setAggYearFrom(e.target.value)} />
                        </div>
                        <div>
                          <label className="text-xs text-gray-500 dark:text-gray-400">Год до</label>
                          <input className="w-full text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" value={aggYearTo} onChange={e=>setAggYearTo(e.target.value)} />
                        </div>
                      </div>
                      <div className="flex items-center gap-2">
                        <label className="text-sm">Top K (1–5)</label>
                        <input type="number" min={1} max={5} value={aggTopK} onChange={(e)=>{
                          const parsed = parseInt(e.target.value || '3', 10);
                          const next = Number.isNaN(parsed) ? 3 : parsed;
                          setAggTopK(Math.max(1, Math.min(5, next)));
                        }} className="w-20 text-sm border rounded px-2 py-1 dark:bg-gray-700 dark:border-gray-600" />
                        <button disabled={aggBusy} onClick={runManualAggSearch} className="ml-auto text-xs px-2 py-1 rounded border border-gray-300 dark:border-gray-600">Применить</button>
                      </div>
                    </div>
                  )}
                  {aggAnswer && (
                    <div className="text-xs bg-gray-50 dark:bg-gray-700/40 border border-gray-200 dark:border-gray-600 rounded-lg p-3 space-y-2">
                      <div className="flex items-center justify-between gap-2">
                        <span className="font-semibold text-gray-800 dark:text-gray-200">Краткий ответ каталога</span>
                        <div className="flex items-center gap-2">
                          <button onClick={()=> insertAtCaret(`\n\n${aggAnswer}\n\n`)} className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded">Вставить</button>
                          <button onClick={()=> copyToClipboard(aggAnswer, 'Agregator — ответ')} className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded">Копировать</button>
                        </div>
                      </div>
                      <p className="text-gray-700 dark:text-gray-300 whitespace-pre-wrap">{aggAnswer}</p>
                    </div>
                  )}
                  {aggProgress.length > 0 && (
                    <div className="space-y-1 text-xs text-gray-500 dark:text-gray-400">
                      <div className="font-medium text-gray-600 dark:text-gray-300">Прогресс поиска</div>
                      <ul className="list-disc pl-4 space-y-1">
                        {aggProgress.map((line, idx) => (
                          <li key={`${line}-${idx}`}>{line}</li>
                        ))}
                      </ul>
                    </div>
                  )}
                  {aggItems.length > 0 && (
                    <div className="space-y-2 text-xs text-gray-600 dark:text-gray-300">
                      <div className="font-medium">Источники каталога ({aggItems.length})</div>
                      <div className="space-y-2 max-h-56 overflow-auto pr-1">
                        {aggItems.map((it, i)=> (
                          <div key={`${it.file_id}-${i}`} className="border border-gray-200 dark:border-gray-600 rounded-lg p-2">
                            <div className="flex gap-2">
                              <div className="flex-1 min-w-0">
                                <div className="font-medium text-gray-800 dark:text-gray-100 truncate">[{i+1}] {it.title || `file-${it.file_id}`}</div>
                                {typeof it.score === 'number' && (
                                  <div className="text-[11px] text-gray-500 dark:text-gray-400">score: {it.score.toFixed(3)}</div>
                                )}
                                {it.snippets && it.snippets.length > 0 && (
                                  <div className="text-[11px] text-gray-500 dark:text-gray-400 mt-1 max-h-16 overflow-hidden">{it.snippets[0]}</div>
                                )}
                              </div>
                              <div className="flex flex-col gap-1 items-stretch text-[11px]">
                                <button className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded" onClick={()=> insertFootnote(i+1, it)}>Назначить №{i+1}</button>
                                <button className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded" onClick={()=> insertFootnote(null, it)}>Следующий номер</button>
                                <button className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded" onClick={()=> insertAggSnippet(it)}>Фрагмент</button>
                                <a href={`/file/${it.file_id}`} target="_blank" rel="noopener" className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded text-center">Открыть</a>
                              </div>
                            </div>
                          </div>
                        ))}
                      </div>
                      <div className="flex gap-2 flex-wrap">
                        <button className="px-2 py-1 border border-gray-300 dark:border-gray-600 rounded" onClick={()=> insertFootnotesRange(Math.min(aggItems.length, aggTopK))}>Вставить топ {Math.min(aggItems.length, aggTopK)}</button>
                      </div>
                    </div>
                  )}
                  {aggBusy && (
                    <div className="text-xs text-gray-500 dark:text-gray-400">Каталог ищет подходящие записи…</div>
                  )}
                </div>
              )}
            </div>

            {/* Templates */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <BookOpen className="w-5 h-5 mr-2" />
                Шаблоны ГОСТ
              </h2>
              <div className="mb-3">
                <label className="block text-sm font-medium text-gray-700 dark:text-gray-300 mb-2">
                  Загрузить .bib
                </label>
                <input type="file" accept=".bib" onChange={onLoadBib} className="w-full text-sm text-gray-700 dark:text-gray-200 file:mr-3 file:py-2 file:px-3 file:rounded-lg file:border-0 file:bg-indigo-600 file:text-white hover:file:bg-indigo-700" />
              </div>
              
              <select value={selectedTemplate} onChange={(e) => { setSelectedTemplate(e.target.value); setSelectedTemplateKey(e.target.value); }} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent mb-2 dark:bg-gray-700 dark:text-white">
                <option value="">— выбрать —</option>
                {Object.keys(templatesMap).map((key) => (
                  <option key={key} value={key}>{key}</option>
                ))}
              </select>
              {selectedTemplateKey && (
                <div className="mb-2">
                  <label className="block text-xs text-gray-500 dark:text-gray-400 mb-1">Текст шаблона</label>
                  <textarea value={templatesMap[selectedTemplateKey]} onChange={(e)=>setTemplatesMap(prev=>({...prev, [selectedTemplateKey]: e.target.value}))} rows={6} className="w-full px-2 py-2 border border-gray-300 dark:border-gray-600 rounded-lg font-mono text-xs dark:bg-gray-700 dark:text-white" />
                  <div className="flex justify-between mt-2">
                    <button onClick={()=>setTemplatesMap({...DEFAULT_GOST_TEMPLATES})} className="text-xs px-2 py-1 border rounded">Сбросить по умолчанию</button>
                    <button onClick={()=>setContent(prev => `${prev}\n\n${templatesMap[selectedTemplateKey]}\n\n`)} className="text-xs px-2 py-1 bg-indigo-600 text-white rounded">Вставить в текст</button>
                  </div>
                </div>
              )}
              
              <button onClick={handleTemplate} disabled={!selectedTemplate} className="w-full px-4 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700 focus:ring-2 focus:ring-indigo-500 focus:ring-offset-2 disabled:opacity-50 disabled:cursor-not-allowed transition-colors">
                Вставить шаблон
              </button>
            </div>

            {/* Export */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Download className="w-5 h-5 mr-2" />
                Экспорт
              </h2>
              
              <div className="grid grid-cols-2 gap-2 mb-3">
                <button onClick={() => handleExport('md')} className="px-3 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors">.md</button>
                <button onClick={() => handleExport('docx')} className="px-3 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors">.docx</button>
              </div>

              <button onClick={handleCopy} className="w-full flex items-center justify-center px-4 py-2 border border-gray-300 dark:border-gray-600 text-gray-700 dark:text-gray-300 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700 focus:ring-2 focus:ring-blue-500 focus:ring-offset-2 transition-colors">
                <Copy className="w-4 h-4 mr-2" />
                Копировать
              </button>
            </div>
          </div>
          )}

          {/* Main Editor Area */}
          <div onContextMenu={(e)=>{ e.preventDefault(); if(selectedTemplateKey){ setContent(prev => `${prev}\n\n${templatesMap[selectedTemplateKey]}\n\n`);} }}>
            {/* Tabs */}
            <div className="flex border-b border-gray-200 dark:border-gray-700 mb-3 text-sm">
              {([
                { id: 'editor', label: 'Редактор', icon: Edit3 },
                { id: 'preview', label: 'Предпросмотр', icon: Eye },
                { id: 'bibliography', label: missingCites.size>0 ? `Библиография (${missingCites.size})` : 'Библиография', icon: BookMarked },
                { id: 'logs', label: 'Журнал агента', icon: MessageSquare }
              ] as const).map(({ id, label, icon: Icon }) => (
                <button
                  key={id}
                  onClick={() => setActiveTab(id)}
                  className={`flex items-center px-3 py-1 font-medium border-b-2 transition-colors ${
                    activeTab === id
                      ? 'border-blue-500 text-blue-600 dark:text-blue-400'
                      : 'border-transparent text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300'
                  }`}
                >
                  <Icon className="w-4 h-4 mr-2" />
                  {label}
                </button>
              ))}
            </div>

            {/* Tab Content */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm border border-gray-200 dark:border-gray-700 min-h-[600px]">
              {activeTab === 'editor' && (
                <div className="p-4">
                  <div className="mb-2 flex items-center justify-between">
                    <h3 className="text-base font-semibold text-gray-900 dark:text-white">Редактор (Markdown)</h3>
                    <button onClick={() => setShowQuickFormat(!showQuickFormat)} className="flex items-center px-2 py-1 text-xs bg-blue-100 dark:bg-blue-900 text-blue-800 dark:text-blue-200 rounded-lg hover:bg-blue-200 dark:hover:bg-blue-800 transition-colors">
                      <Zap className="w-4 h-4 mr-1" />
                      Форматирование
                    </button>
                  </div>
                  {showQuickFormat && (
                    <div className="mb-3 p-3 bg-gray-50 dark:bg-gray-700 rounded-lg">
                      <div className="grid grid-cols-5 gap-2">
                        {QUICK_FORMATTERS.map((formatter, index) => (
                          <button key={index} onClick={() => handleQuickFormat(formatter)} className="flex items-center justify-center px-2 py-2 text-xs bg-white dark:bg-gray-600 border border-gray-200 dark:border-gray-500 rounded hover:bg-gray-50 dark:hover:bg-gray-500 transition-colors" title={formatter.name}>
                            <formatter.icon className="w-4 h-4" />
                          </button>
                        ))}
                      </div>
                    </div>
                  )}
                  <textarea
                    id="editor"
                    value={content}
                    onChange={(e) => setContent(e.target.value)}
                    onKeyDown={onEditorKeyDown}
                    onKeyUp={onEditorKeyUpOrClick}
                    onClick={onEditorKeyUpOrClick}
                    className="w-full h-[70vh] px-4 py-3 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent font-mono text-sm dark:bg-gray-700 dark:text-white resize-none"
                    placeholder="# Заголовок\n\nВставьте/пишите текст здесь..."
                  />
                  {showCiteBox && citeSuggestions.length>0 && (
                    <div className="mt-2 max-h-56 overflow-auto bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg shadow-lg">
                      {citeSuggestions.map((s, i) => (
                        <button
                          key={s.key}
                          onMouseDown={(e)=>{ e.preventDefault(); applyCiteSuggestion(s.key); }}
                          className={`w-full text-left px-3 py-2 text-sm ${i===citeIndex? 'bg-blue-50 dark:bg-blue-900/30' : ''}`}
                        >
                          <div className="font-mono text-blue-700 dark:text-blue-300">[@{s.key}]</div>
                          {(s.title || s.authors || s.year) && (
                            <div className="text-xs text-gray-600 dark:text-gray-400 truncate">
                              {s.authors ? s.authors + ' — ' : ''}{s.title || ''}{s.year ? ' ('+s.year+')' : ''}
                            </div>
                          )}
                        </button>
                      ))}
                    </div>
                  )}
                  <div className="mt-2 flex gap-2">
                    <button onClick={()=>{ if (contentHistory.length>1){ const prev = contentHistory[contentHistory.length-2]; setContentRedo(r=>[content, ...r]); setContent(prev); setContentHistory(h=>h.slice(0,-1)); } }} className="px-3 py-1 text-xs border rounded">Откатить</button>
                    <button onClick={()=>{ if (contentRedo.length>0){ const next = contentRedo[0]; setContentHistory(h=>[...h, content]); setContent(next); setContentRedo(r=>r.slice(1)); } }} className="px-3 py-1 text-xs border rounded">Вернуть</button>
                  </div>
                </div>
              )}

              {activeTab === 'preview' && (
                <div className="p-4">
                  <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-2">Предпросмотр</h3>
                  <div className="prose prose-gray dark:prose-invert max-w-none" dangerouslySetInnerHTML={{ __html: preview }} />
                </div>
              )}

              {activeTab === 'bibliography' && (
                <div className="p-4">
                  <h3 className="text-base font-semibold text-gray-900 dark:text-white mb-2">Библиография (превью)</h3>
                  {missingCites.size > 0 && (
                    <div className="mb-3 p-3 border border-yellow-300 bg-yellow-50 dark:bg-yellow-900/20 dark:border-yellow-700 rounded">
                      <div className="text-sm text-yellow-800 dark:text-yellow-200">Отсутствуют записи для ключей: {[...missingCites].join(', ')}</div>
                    </div>
                  )}
                  {bibliography ? (
                    <div className="bg-gray-50 dark:bg-gray-700 p-4 rounded-lg">
                      <pre className="text-sm text-gray-800 dark:text-gray-200 whitespace-pre-wrap">{bibliography}</pre>
                    </div>
                  ) : (
                    <p className="text-gray-500 dark:text-gray-400 italic">Библиография будет отображена здесь после добавления ссылок [@citekey] в текст</p>
                  )}
                </div>
              )}

              {activeTab === 'logs' && (
                <div className="p-4">
                  <div className="flex items-center justify-between mb-2">
                    <h3 className="text-base font-semibold text-gray-900 dark:text-white">Журнал агента</h3>
                    <div className="flex items-center space-x-2">
                      <div className="relative">
                        <Search className="w-4 h-4 absolute left-3 top-1/2 transform -translate-y-1/2 text-gray-400" />
                        <input type="text" placeholder="Поиск в журнале..." value={searchTerm} onChange={(e) => setSearchTerm(e.target.value)} className="pl-10 pr-4 py-2 border border-gray-300 dark:border-gray-600 rounded-lg focus:ring-2 focus:ring-blue-500 focus:border-transparent text-sm dark:bg-gray-700 dark:text-white" />
                      </div>
                      <button onClick={() => setLogs([])} className="flex items-center px-3 py-2 text-sm text-gray-500 hover:text-gray-700 dark:text-gray-400 dark:hover:text-gray-300 border border-gray-300 dark:border-gray-600 rounded-lg hover:bg-gray-50 dark:hover:bg-gray-700">
                        <Trash2 className="w-4 h-4 mr-1" />
                        Очистить
                      </button>
                    </div>
                  </div>
                  <div className="space-y-3 max-h-96 overflow-y-auto">
                    {filteredLogs.length === 0 ? (
                      <p className="text-gray-500 dark:text-gray-400 italic">{logs.length === 0 ? 'Журнал пуст. Взаимодействия с агентом будут отображаться здесь.' : 'Ничего не найдено по запросу.'}</p>
                    ) : (
                      filteredLogs.map((log, index) => (
                        <div key={index} className={`border-l-4 pl-4 py-2 ${log.cached ? 'border-green-500 bg-green-50 dark:bg-green-900/20' : 'border-blue-500'}`}>
                          <div className="flex items-center text-sm text-gray-600 dark:text-gray-400 mb-1">
                            <span className="font-medium">[{log.time}]</span>
                            <span className="ml-2 font-semibold text-gray-900 dark:text-white">{log.title}</span>
                            {log.cached && (
                              <span className="ml-2 px-2 py-1 bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200 rounded-full text-xs">Из кеша</span>
                            )}
                          </div>
                          <pre className="text-sm text-gray-800 dark:text-gray-200 whitespace-pre-wrap font-mono bg-gray-50 dark:bg-gray-700 p-3 rounded-lg">{log.body}</pre>
                        </div>
                      ))
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>

          {/* Right Panel (moved content) */}
          {!focusMode && (
          <div className={`${rightOpen ? '' : 'hidden lg:block'} space-y-4 lg:sticky lg:top-20 self-start max-h-[calc(100vh-100px)] overflow-y-auto`}>
            {/* Project Management */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <FolderOpen className="w-5 h-5 mr-2" />
                Управление
              </h2>
              <div className="space-y-3">
                <div className="flex gap-2">
                  <button onClick={saveProject} className="flex-1 flex items-center justify-center px-3 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700">
                    <Download className="w-4 h-4 mr-1" />Сохранить
                  </button>
                  <label className="flex-1 flex items-center justify-center px-3 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 cursor-pointer">
                    <Upload className="w-4 h-4 mr-1" />Загрузить
                    <input type="file" accept=".json" onChange={loadProject} className="hidden" />
                  </label>
                </div>
                <button onClick={()=>{ setProject(DEFAULT_PROJECT); setContent(DEFAULT_PROJECT.content || ''); setSelectedCriteria(CRITERIA_PROFILES[0].criteria); setTemplatesMap({...DEFAULT_GOST_TEMPLATES}); }} className="w-full flex items-center justify-center px-3 py-2 bg-indigo-600 text-white rounded-lg hover:bg-indigo-700">
                  Новый проект
                </button>
                <button onClick={clearCache} className="w-full flex items-center justify-center px-3 py-2 bg-gray-600 text-white rounded-lg hover:bg-gray-700">
                  <Database className="w-4 h-4 mr-1" />Очистить кеш
                </button>
              </div>
            </div>

            {/* LLM Settings */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-4 flex items-center">
                <Settings className="w-5 h-5 mr-2" />
                Настройки LLM
              </h2>
              <div className="space-y-3">
                <div>
                  <label className="block text-sm mb-1 text-gray-700 dark:text-gray-300">Адрес сервера</label>
                  <input type="text" value={llmBaseUrl} onChange={(e)=>setLlmBaseUrl(e.target.value)} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white font-mono text-xs" placeholder="http://localhost:1234/v1" />
                </div>
                <div>
                  <label className="block text-sm mb-1 text-gray-700 dark:text-gray-300">Модель</label>
                  <input type="text" value={llmModel} onChange={(e)=>setLlmModel(e.target.value)} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white font-mono text-xs" placeholder="google/gemma-3n-e4b" />
                </div>
              </div>
            </div>

            {/* Project Settings */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                <Settings className="w-5 h-5 mr-2" />
                Проект
              </h2>
              <div className="space-y-3">
                <input type="text" value={project.title} onChange={(e)=>setProject(prev=>({...prev,title:e.target.value}))} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white" placeholder="Название" />
                <select value={project.language} onChange={(e)=>setProject(prev=>({...prev,language:e.target.value}))} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white">
                  <option value="ru">Русский</option>
                  <option value="en">English</option>
                </select>
                <textarea value={project.style_guide} onChange={(e)=>setProject(prev=>({...prev,style_guide:e.target.value}))} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white font-mono text-xs" rows={3} placeholder="Стиль-гайд" />
                <textarea value={project.persona} onChange={(e)=>setProject(prev=>({...prev,persona:e.target.value}))} className="w-full px-3 py-2 border border-gray-300 dark:border-gray-600 rounded-lg dark:bg-gray-700 dark:text-white font-mono text-xs" rows={2} placeholder="Персона" />
              </div>
            </div>

            {/* Criteria Profiles */}
            <div className="bg-white dark:bg-gray-800 rounded-xl shadow-sm p-4 border border-gray-200 dark:border-gray-700">
              <h2 className="text-lg font-semibold text-gray-900 dark:text-white mb-3 flex items-center">
                <Star className="w-5 h-5 mr-2" />Критерии
              </h2>
              <div className="space-y-2">
                {criteriaProfiles.map((profile, index) => (
                  <div key={index} className={`p-2 rounded border ${selectedCriteria===profile.criteria? 'border-blue-500 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-600'}`}>
                    <div className="flex items-center justify-between">
                      <input value={profile.name} onChange={(e)=>{
                        setCriteriaProfiles(prev=> prev.map((p,i)=> i===index? {...p, name: e.target.value }: p));
                      }} className="bg-transparent text-sm font-medium text-gray-900 dark:text-white outline-none" />
                      <span className={`px-2 py-0.5 rounded-full text-xs ${profile.category==='academic'?'bg-purple-100 dark:bg-purple-900 text-purple-800 dark:text-purple-200':'bg-green-100 dark:bg-green-900 text-green-800 dark:text-green-200'}`}>{profile.category==='academic'?'Академия':'Журнал'}</span>
                    </div>
                    <textarea value={profile.description} onChange={(e)=>{
                      setCriteriaProfiles(prev=> prev.map((p,i)=> i===index? {...p, description: e.target.value }: p));
                    }} className="w-full mt-1 text-xs bg-transparent text-gray-600 dark:text-gray-400 outline-none" />
                    <textarea value={profile.criteria} onChange={(e)=>{
                      setCriteriaProfiles(prev=> prev.map((p,i)=> i===index? {...p, criteria: e.target.value }: p));
                    }} className="w-full mt-1 text-xs bg-transparent text-gray-600 dark:text-gray-300 outline-none font-mono" />
                    <div className="mt-2 flex gap-2">
                      <button onClick={()=> setSelectedCriteria(profile.criteria)} className="px-2 py-1 text-xs border rounded">Выбрать</button>
                      <button onClick={()=> setCriteriaProfiles([...CRITERIA_PROFILES])} className="px-2 py-1 text-xs border rounded">Сбросить по умолчанию</button>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          </div>
          )}
        </div>
      </div>

      {/* Toast */}
      {exportToast && (
        <div className="fixed bottom-6 right-6 z-50 bg-gray-900 text-white px-4 py-3 rounded-lg shadow-lg flex items-center gap-3">
          <span>{exportToast}</span>
          {exportUrl && (
            <a href={exportUrl} target="_blank" rel="noreferrer" className="underline text-blue-300">Открыть</a>
          )}
          <button onClick={()=>{ setExportToast(""); setExportUrl(""); }} className="ml-2 text-sm opacity-80 hover:opacity-100">✕</button>
        </div>
      )}

      {/* Footer */}
      <footer className="bg-white dark:bg-gray-800 border-t border-gray-200 dark:border-gray-700 mt-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-4">
          <div className="flex items-center justify-between text-sm text-gray-600 dark:text-gray-400">
            <div className="flex items-center space-x-4">
              <span className="font-semibold">LM Studio:</span>
              <span className="px-2 py-1 bg-gray-100 dark:bg-gray-700 rounded font-mono">{llmBaseUrl}</span>
              <span className="px-2 py-1 bg-gray-100 dark:bg-gray-700 rounded font-mono">Модель: {llmModel}</span>
              {autoSaveEnabled && (
                <span className="px-2 py-1 bg-green-100 dark:bg-green-700 text-green-800 dark:text-green-200 rounded text-xs">
                  Автосохранение
                </span>
              )}
            </div>
            <span className="text-xs">
              Версия 2.7.1 • Инструмент для научного письма
            </span>
          </div>
        </div>
      </footer>
    </div>
  );
}
