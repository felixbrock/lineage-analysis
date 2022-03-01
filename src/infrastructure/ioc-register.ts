import { InjectionMode, asClass, createContainer } from 'awilix';

import SelectorDomain from '../domain/selector-domain';
import { ReadSelector } from '../domain/selector/read-selector';
import AccountApiRepository from './persistence/account-api-repository';
import { GetAccounts } from '../domain/account-api/get-accounts';

const iocRegister = createContainer({ injectionMode: InjectionMode.CLASSIC });

iocRegister.register({
  selectorDomain: asClass(SelectorDomain),
  readSelector: asClass(ReadSelector),
  getAccounts: asClass(GetAccounts),

  accountApiRepository: asClass(AccountApiRepository),

});

const selectorMain = iocRegister.resolve<SelectorDomain>('selectorDomain');

export default {
  selectorMain,
  container: iocRegister,
};
