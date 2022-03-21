import { buildColumnDto, ColumnDto } from '../column/column-dto';
import { buildTableDto, TableDto } from '../table/table-dto';
import { Lineage } from '../value-types/transient-types/lineage';

export interface LineageDto {
  table: TableDto;
  columns: ColumnDto[];
}

export const buildLineageDto = (lineage: Lineage): LineageDto => ({
  table: buildTableDto(lineage.table),
  columns: lineage.columns.map((column) => buildColumnDto(column)),
});
