import { Table } from '../entities/table';

export interface TableDto {
  id: string;
  name: string;
  columns: string[];
  parentNames: string[];
  statementDependencies: [string, string][][];
}

export const buildTableDto = (table: Table): TableDto => ({
  id: table.id,
  name: table.name,
  columns: table.columns,
  parentNames: table.parentNames,
  statementDependencies: table.statementDependencies,
});
