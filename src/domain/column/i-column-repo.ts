import { Column } from '../entities/column';
import { IBaseServiceRepo } from '../services/i-base-service-repo';

export type ColumnUpdateDto = undefined

export interface ColumnQueryDto {
  relationNames?: string[];
  names?: string[];
  index?: string;
  type?: string;
  materializationIds?: string[];
  lineageId: string;
}

export type IColumnRepo =  IBaseServiceRepo<Column, ColumnQueryDto, ColumnUpdateDto>;
