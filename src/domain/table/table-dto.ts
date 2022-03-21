import { Table } from '../entities/table';

export interface TableDto {
  id: string;
  name: string;
  modelId: string;
}

export const buildTableDto = (table: Table): TableDto => ({
  id: table.id,
  name: table.name,
  modelId: table.modelId,
});
