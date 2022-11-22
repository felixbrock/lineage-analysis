import { Column } from '../entities/column';
import { IServiceRepo } from '../services/i-service-repo';

export type ColumnUpdateDto = undefined

export interface ColumnQueryDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
}

export type IColumnRepo =  IServiceRepo<Column, ColumnQueryDto, ColumnUpdateDto>;
