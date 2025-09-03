import { ContextValue, NSDatabase } from '@sqltools/types';

export type DatabaseItem = {
  database: string;
  label: string;
  type: ContextValue.DATABASE;
  schema: string;
  childType?: ContextValue.TABLE | ContextValue.VIEW;
};

export type TableViewItem = {
  database: string;
  label: string;
  type: ContextValue.TABLE | ContextValue.VIEW;
  schema: string;
  childType: ContextValue.COLUMN;
};

export type ColumnItem = {
  database: string;
  label: string;
  type: ContextValue.COLUMN;
  dataType: string;
  schema: string;
  childType: ContextValue.NO_CHILD;
  isNullable: boolean;
  iconName: string;
  table: NSDatabase.SearchableItem;
}; 