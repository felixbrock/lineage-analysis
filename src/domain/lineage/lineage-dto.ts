import { ColumnDto } from "../column/column-dto";
import { TableDto } from "../table/table-dto";

export interface LineageDto {
  table: TableDto;
  column: ColumnDto[];
}

export const buildLineageDto = (lineage: Lineage): LineageDto => ({
  id: lineage.id,
  name: lineage.name,
  model: buildModelDto(lineage.model),
});
