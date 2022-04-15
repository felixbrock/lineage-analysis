import { Table } from '../entities/table';

export interface TableDto {
  id: string;
  dbtModelId: string;
  name: string;
  modelId: string;
}

export const buildTableDto = (table: Table): TableDto => ({
  id: table.id,
  dbtModelId: table.dbtModelId,
  name: table.name,
  modelId: table.modelId,
});
