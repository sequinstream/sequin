type Column = {
  attnum: number;
  isPk?: boolean;
  name: string;
  type: string;
  filterType?: string;
};

type Table = {
  oid: number;
  schema: string;
  name: string;
  columns: Column[];
  defaultGroupColumns: number[];
  isEventTable: boolean;
};

type Database = {
  id: string;
  name: string;
  tables: Table[];
  publication_name: string;
  pg_major_version: number;
};

export type { Column, Database, Table };
