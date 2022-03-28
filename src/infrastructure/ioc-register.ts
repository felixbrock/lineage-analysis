import { InjectionMode, asClass, createContainer } from 'awilix';

import LineageDomain from '../domain/lineage-domain';
import { CreateLineage } from '../domain/lineage/create-lineage';
import AccountApiRepo from './persistence/account-api-repo';
import { GetAccounts } from '../domain/account-api/get-accounts';
import { ParseSQL } from '../domain/sql-parser-api/parse-sql';
import SQLParserApiRepo from './persistence/sql-parser-api-repo';
import { CreateTable } from '../domain/table/create-table';
import ModelRepo from './persistence/model-repo';
import TableRepo from './persistence/table-repo';
import ColumnRepo from './persistence/column-repo';
import { ReadColumns } from '../domain/column/read-columns';
import { CreateModel } from '../domain/model/create-model';
import { CreateColumn } from '../domain/column/create-column';
import { ReadModels } from '../domain/model/read-models';
import { ReadTables } from '../domain/table/read-tables';
import { ReadTable } from '../domain/table/read-table';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  lineageDomain: asClass(LineageDomain),

  createLineage: asClass(CreateLineage),
  createModel: asClass(CreateModel),
  createTable: asClass(CreateTable),
  createColumn: asClass(CreateColumn),

  readTable: asClass(ReadTable),

  readModels: asClass(ReadModels),
  readTables: asClass(ReadTables),
  readColumns: asClass(ReadColumns),

  parseSQL: asClass(ParseSQL),
  getAccounts: asClass(GetAccounts),

  modelRepo: asClass(ModelRepo),
  tableRepo: asClass(TableRepo),
  columnRepo: asClass(ColumnRepo),

  accountApiRepo: asClass(AccountApiRepo),
  sqlParserApiRepo: asClass(SQLParserApiRepo),
});

const lineageMain = iocRegister.resolve<LineageDomain>('lineageDomain');

const register = {
  lineageMain,
  container: iocRegister,
};

export default register;
