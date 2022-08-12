import { Dashboard } from "../entities/dashboard";

export interface DashboardDto {
    url?: string;
    name?: string;
    materializationName: string;
    columnName: string; 
    id: string;
    lineageId: string;
    columnId: string,
    materializationId: string;
    organizationId: string;
}

export const buildDashboardDto = (dashboard: Dashboard): DashboardDto => ({
    url: dashboard.url,
    name: dashboard.name,
    materializationName: dashboard.materializationName,
    columnName: dashboard.columnName,
    id: dashboard.id,
    lineageId: dashboard.lineageId,
    columnId: dashboard.columnId,
    materializationId: dashboard.materializationId,
    organizationId: dashboard.organizationId
});
