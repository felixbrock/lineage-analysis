import { Dashboard } from '../entities/dashboard';
import { Dependency } from '../entities/dependency';

export interface DashboardToDeleteRef {
  id: string;
}

export interface DashboardDataEnv {
  dashboardsToCreate: Dashboard[];
  dashboardsToReplace: Dashboard[];
  dashboardToDeleteRefs: DashboardToDeleteRef[];
}

export interface ExternalDataEnv extends DashboardDataEnv {
  dependenciesToCreate: Dependency[];
  deleteAllOldDependencies: boolean;
}

export interface ExternalDataEnvProps {
  dataEnv: ExternalDataEnv;
}
