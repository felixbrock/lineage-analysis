import { Column } from '../entities/column';

export interface ColumnDto {
  id: string;
  dbtModelId: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
}

export const buildColumnDto = (column: Column): ColumnDto => ({
  id: column.id,
  dbtModelId: column.dbtModelId,
  name: column.name,
  index: column.index,
  type: column.type,
  materializationId: column.materializationId,
  lineageId: column.lineageId,
});
