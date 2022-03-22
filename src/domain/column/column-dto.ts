import { Column } from '../entities/column';
import { Dependency } from '../value-types/dependency';

export interface ColumnDto {
  id: string;
  name: string;
  tableId: string;
  dependencies: Dependency[];
}

export const buildColumnDto = (column: Column): ColumnDto => ({
  id: column.id,
  name: column.name,
  tableId: column.tableId,
  dependencies: column.dependencies,
});
