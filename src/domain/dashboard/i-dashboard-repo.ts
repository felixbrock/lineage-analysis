import { Dashboard } from '../entities/dashboard';
import { DbConnection } from '../services/i-db';

export interface DashboardQueryDto {
    url?: string;
    name?: string;
    materializationName?: string;
    columnName?: string; 
    id?: string;
    columnId?: string,
    materializationId?: string;
    lineageId: string;
    organizationId: string;
}

export interface IDashboardRepo {
  findOne(id: string, dbConnection: DbConnection): Promise<Dashboard | null>;
  findBy(DashboardQueryDto: DashboardQueryDto, dbConnection: DbConnection): Promise<Dashboard[]>;
  all(dbConnection: DbConnection): Promise<Dashboard[]>;
  insertOne(Dashboard: Dashboard, dbConnection: DbConnection): Promise<string>;
  insertMany(dashboards: Dashboard[], dbConnection: DbConnection): Promise<string[]>;
}
