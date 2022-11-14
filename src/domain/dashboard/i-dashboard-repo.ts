import { Dashboard } from '../entities/dashboard';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

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
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard | null>;
  findBy(
    dashboardQueryDto: DashboardQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard[]>;
  all(
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard[]>;
  insertOne(
    dashboard: Dashboard,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    dashboards: Dashboard[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
}
