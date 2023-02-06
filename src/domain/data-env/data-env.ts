import { Column } from '../entities/column';
import { Logic, ModelRepresentation } from '../entities/logic';
import { Materialization } from '../entities/materialization';

export interface MatToDeleteRef {
  id: string;
  name: string;
  schemaName: string;
  dbName: string;
}

export interface DependencyToDeleteRef {
  targetId: string;
}

export interface ColToDeleteRef {
  id: string;
  name: string;
  relationName: string;
  matId: string;
}

export interface LogicToDeleteRef {
  id: string;
  relationName: string;
}

export interface DataEnvDto {
  matToCreateRelationNames: string[];
  matToReplaceRelationNames: string[];
  matToDeleteRelationNames: string[];
}

export interface DataEnv {
  dashboardsToCreate: Dashboard[];
  dashboardsToReplace: Dashboard[];
  dashboardToDeleteRefs: Dashboard[];
  dependenciesToCreate: Dependency[];
  dependencyToDeleteRefs: DependencyToDeleteRef[];
}

export interface DataEnvProps {
  dataEnv: DataEnv;
  catalog: ModelRepresentation[];
  dbCoveredNames: string[];
}
