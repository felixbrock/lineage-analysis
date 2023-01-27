import { InjectionMode, asClass, createContainer } from 'awilix';

import { CreateLineage } from '../domain/lineage/create-lineage';
import AccountApiRepo from './persistence/account-api-repo';
import { GetAccounts } from '../domain/account-api/get-accounts';
import { ParseSQL } from '../domain/sql-parser-api/parse-sql';
import SQLParserApiRepo from './persistence/sql-parser-api-repo';
import { CreateMaterialization } from '../domain/materialization/create-materialization';
import LogicRepo from './persistence/logic-repo';
import MaterializationRepo from './persistence/materialization-repo';
import ColumnRepo from './persistence/column-repo';
import { ReadColumns } from '../domain/column/read-columns';
import { CreateLogic } from '../domain/logic/create-logic';
import { CreateColumn } from '../domain/column/create-column';
import { ReadLogics } from '../domain/logic/read-logics';
import { ReadMaterializations } from '../domain/materialization/read-materializations';
import { ReadMaterialization } from '../domain/materialization/read-materialization';
import LineageRepo from './persistence/lineage-repo';
import { ReadDependencies } from '../domain/dependency/read-dependencies';
import DependencyRepo from './persistence/dependency-repo';
import { CreateDependency } from '../domain/dependency/create-dependency';
import { ReadLineage } from '../domain/lineage/read-lineage';
import { ReadLogic } from '../domain/logic/read-logic';
import { QuerySfQueryHistory } from '../domain/snowflake-api/query-snowflake-history';
import { CreateExternalDependency } from '../domain/dependency/create-external-dependency';
import DashboardRepo from './persistence/dashboard-repo';
import { ReadDashboards } from '../domain/dashboard/read-dashboards';
import { CreateDashboard } from '../domain/dashboard/create-dashboard';
import SnowflakeApiRepo from './persistence/snowflake-api-repo';
import IntegrationApiRepo from './persistence/integration-api-repo';
import { QuerySnowflake } from '../domain/snowflake-api/query-snowflake';
import { GetSnowflakeProfile } from '../domain/integration-api/get-snowflake-profile';
import { GenerateSfDataEnv } from '../domain/data-env/generate-sf-data-env';
import { GenerateDbtDataEnv } from '../domain/data-env/generate-dbt-data-env';
import { UpdateSfDataEnv } from '../domain/data-env/update-sf-data-env';
import { BuildDependencies } from '../domain/dependency/build-dbt-dependencies';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  createLineage: asClass(CreateLineage),
  createLogic: asClass(CreateLogic),
  createMaterialization: asClass(CreateMaterialization),
  createColumn: asClass(CreateColumn),
  createDependency: asClass(CreateDependency),
  createExternalDependency: asClass(CreateExternalDependency),
  createDashboard: asClass(CreateDashboard),

  readMaterialization: asClass(ReadMaterialization),
  readLineage: asClass(ReadLineage),
  readLogic: asClass(ReadLogic),

  readLogics: asClass(ReadLogics),
  readMaterializations: asClass(ReadMaterializations),
  readColumns: asClass(ReadColumns),
  readDependencies: asClass(ReadDependencies),
  readDashboards: asClass(ReadDashboards),

  generateSfDataEnv: asClass(GenerateSfDataEnv),
  generateDbtDataEnv: asClass(GenerateDbtDataEnv),
  updateSfDataEnv: asClass(UpdateSfDataEnv),
  buildDependencies: asClass(BuildDependencies),

  parseSQL: asClass(ParseSQL),
  querySnowflake: asClass(QuerySnowflake),
  getSnowflakeProfile: asClass(GetSnowflakeProfile),
  getAccounts: asClass(GetAccounts),
  querySfQueryHistory: asClass(QuerySfQueryHistory),

  logicRepo: asClass(LogicRepo),
  materializationRepo: asClass(MaterializationRepo),
  columnRepo: asClass(ColumnRepo),
  dependencyRepo: asClass(DependencyRepo),
  lineageRepo: asClass(LineageRepo),
  dashboardRepo: asClass(DashboardRepo),

  accountApiRepo: asClass(AccountApiRepo),
  sqlParserApiRepo: asClass(SQLParserApiRepo),
  integrationApiRepo: asClass(IntegrationApiRepo),
  snowflakeApiRepo: asClass(SnowflakeApiRepo),
});

export default iocRegister;
