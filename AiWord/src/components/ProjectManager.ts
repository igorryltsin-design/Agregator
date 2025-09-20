/**
 * Менеджер проектов для LLM Writer Agent
 * Управляет сохранением, загрузкой и синхронизацией проектов
 */

export interface ProjectData {
  id: string;
  title: string;
  language: string;
  style_guide: string;
  persona: string;
  content: string;
  bibliography?: string;
  metadata: {
    created_at: string;
    updated_at: string;
    version: string;
    word_count: number;
    char_count: number;
  };
  settings: {
    auto_save: boolean;
    criteria_profile: string;
    templates_used: string[];
  };
}

type LegacyProject = Partial<ProjectData> & {
  created_at?: string;
  updated_at?: string;
  metadata?: Partial<ProjectData['metadata']> & { version?: string };
  settings?: Partial<ProjectData['settings']>;
};

export class ProjectManager {
  private static readonly STORAGE_KEY = 'llm-writer-projects';
  private static readonly CURRENT_PROJECT_KEY = 'llm-writer-current-project';
  private static readonly VERSION = '2.0';

  /**
   * Создает новый проект
   */
  static createProject(title: string = 'Новый проект'): ProjectData {
    const now = new Date().toISOString();
    
    return {
      id: this.generateId(),
      title,
      language: 'ru',
      style_guide: "Пиши чётко и структурированно. Без воды. Сохраняй академический тон, «выводы» — коротко и по делу. Используй Markdown: заголовки, списки, таблицы. Формулы как $...$ или ```math``` при необходимости.",
      persona: "Ты — научный редактор и соавтор. Помогаешь с планом, структурой, стилем, и аргументацией.",
      content: "# Заголовок\n\nВставьте/пишите текст здесь...",
      bibliography: '',
      metadata: {
        created_at: now,
        updated_at: now,
        version: this.VERSION,
        word_count: 0,
        char_count: 0
      },
      settings: {
        auto_save: true,
        criteria_profile: 'ГОСТ Р 7.0.11-2011',
        templates_used: []
      }
    };
  }

  /**
   * Сохраняет проект в localStorage
   */
  static saveProject(project: ProjectData): void {
    try {
      // Обновляем метаданные
      project.metadata.updated_at = new Date().toISOString();
      project.metadata.word_count = this.countWords(project.content);
      project.metadata.char_count = project.content.length;

      // Получаем существующие проекты
      const projects = this.getAllProjects();
      
      // Обновляем или добавляем проект
      const existingIndex = projects.findIndex(p => p.id === project.id);
      if (existingIndex >= 0) {
        projects[existingIndex] = project;
      } else {
        projects.push(project);
      }

      // Сохраняем в localStorage
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(projects));
      localStorage.setItem(this.CURRENT_PROJECT_KEY, project.id);
      
      console.log(`Проект "${project.title}" сохранен`);
    } catch (error) {
      console.error('Ошибка сохранения проекта:', error);
      throw new Error('Не удалось сохранить проект');
    }
  }

  /**
   * Загружает проект по ID
   */
  static loadProject(id: string): ProjectData | null {
    try {
      const projects = this.getAllProjects();
      const project = projects.find(p => p.id === id);
      
      if (project) {
        localStorage.setItem(this.CURRENT_PROJECT_KEY, id);
        return project;
      }
      
      return null;
    } catch (error) {
      console.error('Ошибка загрузки проекта:', error);
      return null;
    }
  }

  /**
   * Получает все сохраненные проекты
   */
  static getAllProjects(): ProjectData[] {
    try {
      const stored = localStorage.getItem(this.STORAGE_KEY);
      if (!stored) return [];
      
      const projects = JSON.parse(stored) as ProjectData[];
      
      // Миграция старых проектов
      return projects.map(project => this.migrateProject(project));
    } catch (error) {
      console.error('Ошибка получения проектов:', error);
      return [];
    }
  }

  /**
   * Получает текущий активный проект
   */
  static getCurrentProject(): ProjectData | null {
    try {
      const currentId = localStorage.getItem(this.CURRENT_PROJECT_KEY);
      if (!currentId) return null;
      
      return this.loadProject(currentId);
    } catch (error) {
      console.error('Ошибка получения текущего проекта:', error);
      return null;
    }
  }

  /**
   * Удаляет проект
   */
  static deleteProject(id: string): boolean {
    try {
      const projects = this.getAllProjects();
      const filteredProjects = projects.filter(p => p.id !== id);
      
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(filteredProjects));
      
      // Если удаляем текущий проект, сбрасываем указатель
      const currentId = localStorage.getItem(this.CURRENT_PROJECT_KEY);
      if (currentId === id) {
        localStorage.removeItem(this.CURRENT_PROJECT_KEY);
      }
      
      return true;
    } catch (error) {
      console.error('Ошибка удаления проекта:', error);
      return false;
    }
  }

  /**
   * Экспортирует проект в JSON
   */
  static exportProject(project: ProjectData): string {
    return JSON.stringify(project, null, 2);
  }

  /**
   * Импортирует проект из JSON
   */
  static importProject(jsonData: string): ProjectData {
    try {
      const project = JSON.parse(jsonData) as ProjectData;
      
      // Валидация обязательных полей
      if (!project.title || !project.content) {
        throw new Error('Неверный формат проекта');
      }
      
      // Генерируем новый ID для импортированного проекта
      project.id = this.generateId();
      project.metadata.updated_at = new Date().toISOString();
      
      return this.migrateProject(project);
    } catch (error) {
      console.error('Ошибка импорта проекта:', error);
      throw new Error('Не удалось импортировать проект');
    }
  }

  /**
   * Создает резервную копию всех проектов
   */
  static createBackup(): string {
    const projects = this.getAllProjects();
    const backup = {
      version: this.VERSION,
      created_at: new Date().toISOString(),
      projects
    };
    
    return JSON.stringify(backup, null, 2);
  }

  /**
   * Восстанавливает проекты из резервной копии
   */
  static restoreFromBackup(backupData: string): number {
    try {
      const backup = JSON.parse(backupData) as { projects?: unknown };

      if (!Array.isArray(backup.projects)) {
        throw new Error('Неверный формат резервной копии');
      }
      
      const projects = (backup.projects as LegacyProject[]).map(project => this.migrateProject(project));
      localStorage.setItem(this.STORAGE_KEY, JSON.stringify(projects));
      
      return projects.length;
    } catch (error) {
      console.error('Ошибка восстановления из резервной копии:', error);
      throw new Error('Не удалось восстановить проекты');
    }
  }

  /**
   * Автосохранение проекта
   */
  static autoSave(project: ProjectData): void {
    if (project.settings.auto_save) {
      // Сохраняем в отдельный ключ для автосохранения
      const autoSaveKey = `${this.STORAGE_KEY}-autosave-${project.id}`;
      localStorage.setItem(autoSaveKey, JSON.stringify(project));
    }
  }

  /**
   * Восстанавливает проект из автосохранения
   */
  static restoreFromAutoSave(projectId: string): ProjectData | null {
    try {
      const autoSaveKey = `${this.STORAGE_KEY}-autosave-${projectId}`;
      const stored = localStorage.getItem(autoSaveKey);
      
      if (stored) {
        return JSON.parse(stored) as ProjectData;
      }
      
      return null;
    } catch (error) {
      console.error('Ошибка восстановления автосохранения:', error);
      return null;
    }
  }

  /**
   * Очищает автосохранения
   */
  static clearAutoSaves(): void {
    const keys = Object.keys(localStorage);
    keys.forEach(key => {
      if (key.includes(`${this.STORAGE_KEY}-autosave-`)) {
        localStorage.removeItem(key);
      }
    });
  }

  /**
   * Получает статистику по проектам
   */
  static getStatistics(): {
    totalProjects: number;
    totalWords: number;
    totalCharacters: number;
    averageWordsPerProject: number;
    oldestProject: string;
    newestProject: string;
  } {
    const projects = this.getAllProjects();
    
    if (projects.length === 0) {
      return {
        totalProjects: 0,
        totalWords: 0,
        totalCharacters: 0,
        averageWordsPerProject: 0,
        oldestProject: '',
        newestProject: ''
      };
    }
    
    const totalWords = projects.reduce((sum, p) => sum + p.metadata.word_count, 0);
    const totalCharacters = projects.reduce((sum, p) => sum + p.metadata.char_count, 0);
    
    const sortedByDate = projects.sort((a, b) => 
      new Date(a.metadata.created_at).getTime() - new Date(b.metadata.created_at).getTime()
    );
    
    return {
      totalProjects: projects.length,
      totalWords,
      totalCharacters,
      averageWordsPerProject: Math.round(totalWords / projects.length),
      oldestProject: sortedByDate[0]?.title || '',
      newestProject: sortedByDate[sortedByDate.length - 1]?.title || ''
    };
  }

  /**
   * Генерирует уникальный ID
   */
  private static generateId(): string {
    return `project_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
  }

  /**
   * Подсчитывает количество слов в тексте
   */
  private static countWords(text: string): number {
    return text.trim().split(/\s+/).filter(word => word.length > 0).length;
  }

  /**
   * Мигрирует проект к текущей версии
   */
  private static migrateProject(project: LegacyProject): ProjectData {
    // Если проект уже в актуальной версии
    if (project.metadata?.version === this.VERSION) {
      return project as ProjectData;
    }

    // Миграция с версии 1.x
    const migrated: ProjectData = {
      id: project.id || this.generateId(),
      title: project.title || 'Безымянный проект',
      language: project.language || 'ru',
      style_guide: project.style_guide || '',
      persona: project.persona || '',
      content: project.content || '',
      bibliography: project.bibliography || '',
      metadata: {
        created_at: project.created_at || project.metadata?.created_at || new Date().toISOString(),
        updated_at: project.updated_at || project.metadata?.updated_at || new Date().toISOString(),
        version: this.VERSION,
        word_count: project.metadata?.word_count || this.countWords(project.content || ''),
        char_count: project.metadata?.char_count || (project.content || '').length
      },
      settings: {
        auto_save: project.settings?.auto_save ?? true,
        criteria_profile: project.settings?.criteria_profile || 'ГОСТ Р 7.0.11-2011',
        templates_used: project.settings?.templates_used || []
      }
    };

    return migrated;
  }
}
