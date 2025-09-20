/**
 * Форматтер библиографических ссылок по ГОСТ
 * 
 * Поддерживает различные типы источников:
 * - Статьи в журналах
 * - Книги и монографии
 * - Сборники и конференции
 * - Диссертации
 * - Электронные ресурсы
 */

export interface BibEntry {
  type: string;
  fields: Record<string, string>;
  persons: Record<string, string[]>;
}

export interface BibDatabase {
  [key: string]: BibEntry;
}

/**
 * Простейший парсер BibTeX (подмножество), достаточно для Better BibTeX экспорта.
 * Поддерживает поля, многострочные значения в { } и простые списки авторов через "and".
 */
export function parseBibTeX(bibText: string): BibDatabase {
  const entries: BibDatabase = {};
  const normalized = bibText.replace(/\r\n?/g, '\n');
  const blocks = normalized.split(/\n@/).map((b, i) => (i === 0 ? b : '@' + b)).filter(b => b.trim().startsWith('@'));
  for (const block of blocks) {
    const headerMatch = block.match(/^@([a-zA-Z]+)\s*\{\s*([^,\n]+)\s*,/);
    if (!headerMatch) continue;
    const type = headerMatch[1];
    const key = headerMatch[2];
    const body = block.slice(headerMatch[0].length);
    const fields: Record<string, string> = {};
    const persons: Record<string, string[]> = {};

    // Extract fields until the closing brace of entry
    // Greedy field matcher handling {...} and "..."
    const fieldRegex = /(\w+)\s*=\s*(\{[\s\S]*?\}|"[\s\S]*?"|[^,\n]+)\s*,?/g;
    let m: RegExpExecArray | null;
    while ((m = fieldRegex.exec(body))) {
      const name = m[1].toLowerCase();
      let value = m[2].trim();
      if ((value.startsWith('{') && value.endsWith('}')) || (value.startsWith('"') && value.endsWith('"'))) {
        value = value.slice(1, -1);
      }
      value = value.replace(/\s+\n\s+/g, ' ').replace(/\n/g, ' ').trim();
      if (name === 'author' || name === 'editor') {
        persons[name] = value.split(/\s+and\s+/i).map(s => s.trim()).filter(Boolean);
      } else {
        fields[name] = value;
      }
    }

    entries[key] = { type, fields, persons };
  }
  return entries;
}

/**
 * Форматирует библиографическую запись по ГОСТ 7.1-2003
 * 
 * @param key - Ключ записи
 * @param entry - Данные записи
 * @param index - Порядковый номер в списке
 * @returns Отформатированная строка
 * 
 * @example
 * const entry = {
 *   type: 'article',
 *   fields: { title: 'Заголовок', journal: 'Журнал', year: '2023', pages: '1-10' },
 *   persons: { author: ['Иванов И.И.'] }
 * };
 * formatGostReference('ivanov2023', entry, 1);
 * // => "1. Иванов И.И. Заголовок — Журнал, 2023. — С. 1-10."
 */
export function formatGostReference(key: string, entry: BibEntry, index: number): string {
  if (!entry) {
    return `${index}. [Нет в .bib] ${key}`;
  }

  const { fields, persons, type } = entry;
  
  // Авторы
  const authors = persons.author || persons.editor || [];
  const authorsStr = formatAuthors(authors, fields.organization);
  
  // Основные поля
  const title = fields.title || '[без названия]';
  const year = fields.year || '[г. н.]';
  
  // Специфичные для типа поля
  let source = '';
  let pages = '';
  switch (type.toLowerCase()) {
    case 'article':
      source = formatJournalSource(fields);
      pages = formatPages(fields.pages);
      break;
      
    case 'book':
    case 'monograph':
      source = formatBookSource(fields);
      pages = formatTotalPages(fields.pages);
      break;
      
    case 'inproceedings':
    case 'incollection':
      source = formatCollectionSource(fields);
      pages = formatPages(fields.pages);
      break;
      
    case 'phdthesis':
    case 'mastersthesis':
      source = formatThesisSource(fields, type);
      pages = formatTotalPages(fields.pages);
      break;
      
    case 'online':
    case 'misc':
      source = formatOnlineSource(fields);
      break;
      
    default:
      source = fields.publisher || fields.institution || '[источник не указан]';
  }

  // Сборка итоговой строки
  const parts = [`${index}. ${authorsStr} ${title}`];
  
  if (source) {
    parts.push(` — ${source}, ${year}.`);
  } else {
    parts.push(` — ${year}.`);
  }
  
  if (pages) {
    parts.push(` — ${pages}.`);
  }
  
  // DOI или URL
  if (fields.doi) {
    parts.push(` — DOI: ${fields.doi}.`);
  } else if (fields.url) {
    parts.push(` — Режим доступа: ${fields.url}.`);
  }
  
  return parts.join('');
}

/**
 * Форматирует список авторов
 */
function formatAuthors(authors: string[], organization?: string): string {
  if (authors.length === 0) {
    return organization || '[б. а.]';
  }
  
  if (authors.length === 1) {
    return authors[0];
  }
  
  if (authors.length <= 3) {
    return authors.join(', ');
  }
  
  // Более 3 авторов - указываем первого и "и др."
  return `${authors[0]} и др.`;
}

/**
 * Форматирует источник для журнальной статьи
 */
function formatJournalSource(fields: Record<string, string>): string {
  const journal = fields.journal || fields.journaltitle;
  if (!journal) return '[журнал не указан]';
  
  const volume = fields.volume;
  const number = fields.number;
  
  let source = journal;
  
  if (volume) {
    source += `. Т. ${volume}`;
  }
  
  if (number) {
    source += `. № ${number}`;
  }
  
  return source;
}

/**
 * Форматирует источник для книги
 */
function formatBookSource(fields: Record<string, string>): string {
  const publisher = fields.publisher;
  const address = fields.address;
  
  if (publisher && address) {
    return `${address}: ${publisher}`;
  }
  
  return publisher || address || '[издательство не указано]';
}

/**
 * Форматирует источник для сборника/конференции
 */
function formatCollectionSource(fields: Record<string, string>): string {
  const booktitle = fields.booktitle;
  const publisher = fields.publisher;
  const address = fields.address;
  
  let source = booktitle || '[сборник не указан]';
  
  if (publisher || address) {
    const pubInfo = [address, publisher].filter(Boolean).join(': ');
    source += ` — ${pubInfo}`;
  }
  
  return source;
}

/**
 * Форматирует источник для диссертации
 */
function formatThesisSource(fields: Record<string, string>, type: string): string {
  const school = fields.school || fields.institution;
  const address = fields.address;
  
  const thesisType = type === 'phdthesis' ? 'дис. ... д-ра наук' : 'дис. ... канд. наук';
  
  let source = thesisType;
  
  if (school) {
    source += ` — ${school}`;
  }
  
  if (address) {
    source += `, ${address}`;
  }
  
  return source;
}

/**
 * Форматирует источник для онлайн-ресурса
 */
function formatOnlineSource(fields: Record<string, string>): string {
  const howpublished = fields.howpublished;
  const organization = fields.organization;
  const publisher = fields.publisher;
  
  return howpublished || organization || publisher || 'Электронный ресурс';
}

/**
 * Форматирует страницы (диапазон)
 */
function formatPages(pages?: string): string {
  if (!pages) return '';
  
  // Обработка различных форматов страниц
  const cleanPages = pages.replace(/[{}]/g, '').trim();
  
  if (cleanPages.includes('-') || cleanPages.includes('–')) {
    return `С. ${cleanPages}`;
  }
  
  return `С. ${cleanPages}`;
}

/**
 * Форматирует общее количество страниц
 */
function formatTotalPages(pages?: string): string {
  if (!pages) return '';
  
  const cleanPages = pages.replace(/[{}]/g, '').trim();
  return `${cleanPages} с`;
}

/**
 * Тесты для форматтера ГОСТ-ссылок
 */
export const bibliographyTests = {
  /**
   * Тест форматирования журнальной статьи
   */
  testJournalArticle() {
    const entry: BibEntry = {
      type: 'article',
      fields: {
        title: 'Применение машинного обучения в анализе данных',
        journal: 'Вестник компьютерных наук',
        volume: '15',
        number: '3',
        pages: '45-52',
        year: '2023'
      },
      persons: {
        author: ['Иванов И.И.', 'Петров П.П.']
      }
    };
    
    const result = formatGostReference('ivanov2023', entry, 1);
    const expected = '1. Иванов И.И., Петров П.П. Применение машинного обучения в анализе данных — Вестник компьютерных наук. Т. 15. № 3, 2023. — С. 45-52.';
    
    console.assert(result === expected, `Journal article test failed: ${result}`);
    return result === expected;
  },

  /**
   * Тест форматирования книги
   */
  testBook() {
    const entry: BibEntry = {
      type: 'book',
      fields: {
        title: 'Основы программирования',
        publisher: 'Наука',
        address: 'Москва',
        pages: '320',
        year: '2022'
      },
      persons: {
        author: ['Сидоров С.С.']
      }
    };
    
    const result = formatGostReference('sidorov2022', entry, 2);
    const expected = '2. Сидоров С.С. Основы программирования — Москва: Наука, 2022. — 320 с.';
    
    console.assert(result === expected, `Book test failed: ${result}`);
    return result === expected;
  },

  /**
   * Тест форматирования статьи в сборнике
   */
  testInProceedings() {
    const entry: BibEntry = {
      type: 'inproceedings',
      fields: {
        title: 'Новые подходы к обработке естественного языка',
        booktitle: 'Труды международной конференции по ИИ',
        publisher: 'Техносфера',
        address: 'СПб.',
        pages: '123-130',
        year: '2023'
      },
      persons: {
        author: ['Козлов К.К.', 'Волков В.В.', 'Медведев М.М.', 'Зайцев З.З.']
      }
    };
    
    const result = formatGostReference('kozlov2023', entry, 3);
    const expected = '3. Козлов К.К. и др. Новые подходы к обработке естественного языка — Труды международной конференции по ИИ — СПб.: Техносфера, 2023. — С. 123-130.';
    
    console.assert(result === expected, `InProceedings test failed: ${result}`);
    return result === expected;
  },

  /**
   * Тест форматирования диссертации
   */
  testThesis() {
    const entry: BibEntry = {
      type: 'phdthesis',
      fields: {
        title: 'Методы машинного обучения в задачах классификации',
        school: 'МГУ им. М.В. Ломоносова',
        address: 'Москва',
        pages: '180',
        year: '2023'
      },
      persons: {
        author: ['Орлов О.О.']
      }
    };
    
    const result = formatGostReference('orlov2023', entry, 4);
    const expected = '4. Орлов О.О. Методы машинного обучения в задачах классификации — дис. ... д-ра наук — МГУ им. М.В. Ломоносова, Москва, 2023. — 180 с.';
    
    console.assert(result === expected, `Thesis test failed: ${result}`);
    return result === expected;
  },

  /**
   * Тест форматирования онлайн-ресурса
   */
  testOnlineResource() {
    const entry: BibEntry = {
      type: 'online',
      fields: {
        title: 'Документация по React',
        url: 'https://reactjs.org/docs',
        year: '2023'
      },
      persons: {
        author: []
      }
    };
    
    const result = formatGostReference('react2023', entry, 5);
    const expected = '5. [б. а.] Документация по React — Электронный ресурс, 2023. — Режим доступа: https://reactjs.org/docs.';
    
    console.assert(result === expected, `Online resource test failed: ${result}`);
    return result === expected;
  },

  /**
   * Запуск всех тестов
   */
  runAllTests() {
    console.log('Запуск тестов форматтера ГОСТ-ссылок...');
    
    const tests = [
      { name: 'Journal Article', test: this.testJournalArticle },
      { name: 'Book', test: this.testBook },
      { name: 'InProceedings', test: this.testInProceedings },
      { name: 'Thesis', test: this.testThesis },
      { name: 'Online Resource', test: this.testOnlineResource }
    ];
    
    let passed = 0;
    let failed = 0;
    
    tests.forEach(({ name, test }) => {
      try {
        if (test.call(this)) {
          console.log(`✅ ${name}: PASSED`);
          passed++;
        } else {
          console.log(`❌ ${name}: FAILED`);
          failed++;
        }
      } catch (error) {
        console.log(`❌ ${name}: ERROR - ${error}`);
        failed++;
      }
    });
    
    console.log(`\nРезультаты: ${passed} прошли, ${failed} не прошли`);
    return { passed, failed };
  }
};
