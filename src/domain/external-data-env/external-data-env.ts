import { Dashboard } from '../entities/dashboard';
import { Dependency } from '../entities/dependency';

export interface ExternalDependencyToDeleteRef {
  targetId: string;
}

export interface ExternalDataEnv {
  dashboardsToCreate: Dashboard[];
  dashboardsToReplace: Dashboard[];
  dashboardToDeleteRefs: Dashboard[];
  dependenciesToCreate: Dependency[];
  dependencyToDeleteRefs: ExternalDependencyToDeleteRef[];
}

export interface ExternalDataEnvProps {
  dataEnv: ExternalDataEnv;
}
