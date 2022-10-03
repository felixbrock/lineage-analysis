import { Column } from '../entities/column';

export interface ColumnDto {
  id: string;
  relationName: string;
  name: string;
  index: string;
  type: string;
  materializationId: string;
  lineageId: string;
  organizationId: string;
}

export const buildColumnDto = (column: Column): ColumnDto => ({
  id: column.id,
  relationName: column.relationName,
  name: column.name,
  index: column.index,
  type: column.type,
  materializationId: column.materializationId,
  lineageId: column.lineageId,
  organizationId: column.organizationId
});
