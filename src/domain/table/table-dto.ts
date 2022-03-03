import { Table } from '../entities/table';

export interface TableDto {
  id: string;
  name: string;
  columns: string[];
  parents: Table[];
  statementDependencies: [string, string][][];
  lineageInfo: { [key: string]: string }[];
}

export const buildTableDto = (table: Table): TableDto => ({
  id: table.id,
  name: table.name,
  columns: table.columns,
  parents: table.parents,
  statementDependencies: table.statementDependencies,
  lineageInfo: table.lineageInfo
});
