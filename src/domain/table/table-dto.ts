import { Table } from '../entities/table';
import { Model } from '../value-types/model';
import { buildModelDto, ModelDto } from './model-dto';

export interface TableDto {
  id: string;
  name: string;
  model: ModelDto;
}

export const buildTableDto = (table: Table): TableDto => ({
  id: table.id,
  name: table.name,
  model: buildModelDto(table.model),
});
