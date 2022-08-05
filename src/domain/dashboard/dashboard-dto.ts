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
});
