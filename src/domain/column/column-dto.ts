import { Column } from '../entities/column';
import { Dependency } from '../value-types/dependency';
import { buildDependencyDto, DependencyDto } from './dependency-dto';

export interface ColumnDto {
  id: string;
  name: string;
  tableId: string;
  dependencies: DependencyDto[];
}

export const buildColumnDto = (column: Column): ColumnDto => ({
  id: column.id,
  name: column.name,
  tableId: column.tableId,
  dependencies: column.dependencies.map(dependency => buildDependencyDto(dependency)),
});
