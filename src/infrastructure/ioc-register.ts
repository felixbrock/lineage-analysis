import { InjectionMode, asClass, createContainer } from 'awilix';

import TableDomain from '../domain/lineage-domain';
import { CreateLineage } from '../domain/column/create-column';
import AccountApiRepo from './persistence/account-api-repo';
import { GetAccounts } from '../domain/account-api/get-accounts';
import { ParseSQL } from '../domain/sql-parser-api/parse-sql';
import SQLParserApiRepo from './persistence/sql-parser-api-repo';
import { CreateTable } from '../domain/table/create-table';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  lineageDomain: asClass(TableDomain),
  createLineage: asClass(CreateLineage),
  createTable: asClass(CreateTable),
  parseSQL: asClass(ParseSQL),
  getAccounts: asClass(GetAccounts),

  accountApiRepo: asClass(AccountApiRepo),
  sqlParserApiRepo: asClass(SQLParserApiRepo)

});

const lineageMain = iocRegister.resolve<TableDomain>('lineageDomain');

export default {
  lineageMain,
  container: iocRegister,
};
