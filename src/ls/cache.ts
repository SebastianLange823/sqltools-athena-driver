import { ContextValue } from '@sqltools/types';
import { ColumnItem, DatabaseItem, TableViewItem } from './types';

export class DriverObjectCache {
  private static instance: DriverObjectCache;
  private cache: Array<DatabaseItem | TableViewItem | ColumnItem> = [];

  private constructor() {}

  public static getInstance(): DriverObjectCache {
    if (!DriverObjectCache.instance) {
      DriverObjectCache.instance = new DriverObjectCache();
    }
    return DriverObjectCache.instance;
  }

  public add(item: DatabaseItem | TableViewItem | ColumnItem): void {
    const existing = this.get({
      schema: item.schema,
      database: item.database,
      label: item.label,
      type: item.type,
    });
    if (!existing) {
      this.cache.push(item);
    }
  }

  public get(match: { schema: string; database: string; label: string; type: ContextValue }): DatabaseItem | TableViewItem | ColumnItem | undefined {
    return this.cache.find((cached) =>
      cached.schema === match.schema &&
      cached.database === match.database &&
      cached.label === match.label &&
      cached.type === match.type
    );
  }

  public clear(): void {
    this.cache = [];
  }

  public getColumns(database: any, table: any): ColumnItem[] | undefined {
    const items = this.cache.filter(
      (i) => i.type === ContextValue.COLUMN && i.table === table && i.database == database
    ) as ColumnItem[];

    return items.length > 0 ? items : undefined;
  }

  public getTables(database?: string): TableViewItem[] | undefined {
    let items = this.cache.filter((i) => i.type === ContextValue.TABLE) as TableViewItem[];
    if (database) {
      items = items.filter(i => i.database === database);
    }
    return items.length > 0 ? items : undefined;
  }

  public getViews(database?: string): TableViewItem[] | undefined {
    let items = this.cache.filter((i) => i.type === ContextValue.VIEW) as TableViewItem[];
    if (database) {
      items = items.filter(i => i.database === database);
    }
    return items.length > 0 ? items : undefined;
  }

  public getDatabases(): DatabaseItem[] | undefined {
    const items = this.cache.filter((i) => i.type === ContextValue.DATABASE) as DatabaseItem[];
    return items.length > 0 ? items : undefined;
  }
} 