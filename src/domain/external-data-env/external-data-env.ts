import { Dashboard } from '../entities/dashboard';
import { Dependency } from '../entities/dependency';

export interface DashboardToDeleteRef {
  id: string;
}

export interface ExternalDataEnv {
  dashboardsToCreate: Dashboard[];
  dashboardsToReplace: Dashboard[];
  dashboardToDeleteRefs: DashboardToDeleteRef[];
  dependenciesToCreate: Dependency[];
  deleteAllOldDependencies: boolean;
}

export interface ExternalDataEnvProps {
  dataEnv: ExternalDataEnv;
}
