import { Column } from '../entities/column';
import { IServiceRepo } from '../services/i-service-repo';

export type ColumnUpdateDto = undefined

export interface ColumnQueryDto {
  relationNames?: string[];
  names?: string[];
  index?: string;
  type?: string;
  materializationIds?: string[];
  lineageId: string;
}

export type IColumnRepo =  IServiceRepo<Column, ColumnQueryDto, ColumnUpdateDto>;
