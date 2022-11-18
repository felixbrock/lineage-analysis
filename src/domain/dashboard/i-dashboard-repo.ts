import { Dashboard } from '../entities/dashboard';
import { IBaseServiceRepo } from '../services/i-base-service-repo';

export type DashboardUpdateDto = undefined

export interface DashboardQueryDto {
  url?: string;
  name?: string;
  materializationName?: string;
  columnName?: string;
  id?: string;
  columnId?: string;
  materializationId?: string;
  lineageId: string;
}

export type IDashboardRepo =  IBaseServiceRepo<Dashboard, DashboardQueryDto, DashboardUpdateDto>;
