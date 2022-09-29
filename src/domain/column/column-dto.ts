import { Column } from '../entities/column';

export interface ColumnDto {
  id: string;
  modelId: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  organizationId: string;
}

export const buildColumnDto = (column: Column): ColumnDto => ({
  id: column.id,
  modelId: column.modelId,
  name: column.name,
  index: column.index,
  type: column.type,
  materializationId: column.materializationId,
  lineageId: column.lineageId,
  organizationId: column.organizationId
});
