import { InjectionMode, asClass, createContainer } from 'awilix';

import TableDomain from '../domain/table-domain';
import { ReadTable } from '../domain/table/read-table';
import AccountApiRepo from './persistence/account-api-repo';
import { GetAccounts } from '../domain/account-api/get-accounts';
import { ParseSQL } from '../domain/sql-parser-api/parse-sql';
import SQLParserApiRepo from './persistence/sql-parser-api-repo';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  tableDomain: asClass(TableDomain),
  readTable: asClass(ReadTable),
  parseSQL: asClass(ParseSQL),
  getAccounts: asClass(GetAccounts),

  accountApiRepo: asClass(AccountApiRepo),
  sqlParserApiRepo: asClass(SQLParserApiRepo)

});

const tableMain = iocRegister.resolve<TableDomain>('tableDomain');

export default {
  tableMain,
  container: iocRegister,
};
