import { Dashboard } from '../entities/dashboard';

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

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface IDashboardRepo {
  findOne(
    dashboardId: string,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard | null>;
  findBy(
    dashboardQueryDto: DashboardQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard[]>;
  all(auth: Auth, targetOrgId?: string): Promise<Dashboard[]>;
  insertOne(
    dashboard: Dashboard,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    dashboards: Dashboard[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
}
