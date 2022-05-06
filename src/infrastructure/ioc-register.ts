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

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  createLineage: asClass(CreateLineage).scoped(),
  createLogic: asClass(CreateLogic),
  createMaterialization: asClass(CreateMaterialization),
  createColumn: asClass(CreateColumn),
  createDependency: asClass(CreateDependency),

  readMaterialization: asClass(ReadMaterialization),
  readLineage: asClass(ReadLineage),
  readLogic: asClass(ReadLogic),

  readLogics: asClass(ReadLogics),
  readMaterializations: asClass(ReadMaterializations),
  readColumns: asClass(ReadColumns),
  readDependencies: asClass(ReadDependencies),

  parseSQL: asClass(ParseSQL),
  getAccounts: asClass(GetAccounts),

  logicRepo: asClass(LogicRepo),
  materializationRepo: asClass(MaterializationRepo),
  columnRepo: asClass(ColumnRepo),
  dependencyRepo: asClass(DependencyRepo),
  lineageRepo: asClass(LineageRepo),

  accountApiRepo: asClass(AccountApiRepo),
  sqlParserApiRepo: asClass(SQLParserApiRepo),
});

export default iocRegister;
