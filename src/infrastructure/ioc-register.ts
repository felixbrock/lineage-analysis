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
import { CreateDependencies } from '../domain/dependency/create-dependencies';
import { ReadLineage } from '../domain/lineage/read-lineage';
import { ReadLogic } from '../domain/logic/read-logic';
import { QuerySfQueryHistory } from '../domain/snowflake-api/query-snowflake-history';
import DashboardRepo from './persistence/dashboard-repo';
import { ReadDashboards } from '../domain/dashboard/read-dashboards';
import { CreateDashboards } from '../domain/dashboard/create-dashboards';
import SnowflakeApiRepo from './persistence/snowflake-api-repo';
import IntegrationApiRepo from './persistence/integration-api-repo';
import { QuerySnowflake } from '../domain/snowflake-api/query-snowflake';
import { GetSnowflakeProfile } from '../domain/integration-api/get-snowflake-profile';
import { GenerateSfDataEnv } from '../domain/data-env/generate-sf-data-env';
import { GenerateDbtDataEnv } from '../domain/data-env/generate-dbt-data-env';
import { UpdateSfDataEnv } from '../domain/data-env/update-sf-data-env';
import ObservabilityApiRepo from './persistence/observability-api-repo';
import { GenerateSfExternalDataEnv } from '../domain/external-data-env/generate-sf-external-data-env';
import { UpdateSfExternalDataEnv } from '../domain/external-data-env/update-sf-external-data-env';
import { GenerateSfEnvLineage } from '../domain/data-env/generate-sf-env-lineage';
import Dbo from './persistence/db/mongo-db';
import UpdateSfDataEnvRepo from './persistence/update-sf-data-env-repo';
import GenerateSfEnvLineageRepo from './persistence/generate-sf-env-lineage-repo';
import GetSfExternalDataEnvRepo from './persistence/get-sf-external-data-env-repo';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  createLineage: asClass(CreateLineage),
  createLogic: asClass(CreateLogic),
  createMaterialization: asClass(CreateMaterialization),
  createColumn: asClass(CreateColumn),
  createDependencies: asClass(CreateDependencies),
  createDashboards: asClass(CreateDashboards),

  readMaterialization: asClass(ReadMaterialization),
  readLineage: asClass(ReadLineage),
  readLogic: asClass(ReadLogic),

  readLogics: asClass(ReadLogics),
  readMaterializations: asClass(ReadMaterializations),
  readColumns: asClass(ReadColumns),
  readDependencies: asClass(ReadDependencies),
  readDashboards: asClass(ReadDashboards),

  generateSfDataEnv: asClass(GenerateSfDataEnv),
  generateSfExternalDataEnv: asClass(GenerateSfExternalDataEnv),
  generateDbtDataEnv: asClass(GenerateDbtDataEnv),
  generateSfEnvLineage: asClass(GenerateSfEnvLineage),
  updateSfDataEnv: asClass(UpdateSfDataEnv),
  updateSfExternalDataEnv: asClass(UpdateSfExternalDataEnv),

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
  observabilityApiRepo: asClass(ObservabilityApiRepo),
  snowflakeApiRepo: asClass(SnowflakeApiRepo),
  updateSfDataEnvRepo: asClass(UpdateSfDataEnvRepo),
  generateSfEnvLineageRepo: asClass(GenerateSfEnvLineageRepo),
  getSfExternalDataEnvRepo: asClass(GetSfExternalDataEnvRepo),

  dbo: asClass(Dbo).singleton(),
});

export default iocRegister;
