import { Dashboard } from '../entities/dashboard';
import { IServiceRepo } from '../services/i-service-repo';

export type DashboardUpdateDto = undefined;

export interface DashboardQueryDto {
  id?: string;
  url?: string;
  name?: string;
}

export type IDashboardRepo = IServiceRepo<
  Dashboard,
  DashboardQueryDto,
  DashboardUpdateDto
>;
