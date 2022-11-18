import { Column } from '../entities/column';
import { IBaseServiceRepo } from '../services/i-base-service-repo';

export type ColumnUpdateDto = undefined

export interface ColumnQueryDto {
  relationName?: string | string[];
  name?: string | string[];
  index?: string;
  type?: string;
  materializationId?: string | string[];
  lineageId: string;
}

export type IColumnRepo =  IBaseServiceRepo<Column, ColumnQueryDto, ColumnUpdateDto>;
