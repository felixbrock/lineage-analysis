import { Dashboard } from "../entities/dashboard";

export interface DashboardDto {
    url?: string;
    name?: string;
    materialisation: string;
    column: string; 
    id: string;
    lineageId: string;
    columnId: string,
    matId: string;
}

export const buildDashboardDto = (dashboard: Dashboard): DashboardDto => ({
    url: dashboard.url,
    name: dashboard.name,
    materialisation: dashboard.materialisation,
    column: dashboard.column,
    id: dashboard.id,
    lineageId: dashboard.lineageId,
    columnId: dashboard.columnId,
    matId: dashboard.matId,
});
