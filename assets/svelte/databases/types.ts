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

export type { Column, Table };
