import { Logic } from '../entities/logic';

export interface LogicQueryDto {
  relationName?: string;
  lineageId: string;
}

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface ILogicRepo {
  findOne(
    logicId: string,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Logic | null>;
  findBy(
    logicQueryDto: LogicQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Logic[]>;
  all(auth: Auth, targetOrgId?: string): Promise<Logic[]>;
  insertOne(logic: Logic, auth: Auth, targetOrgId?: string): Promise<string>;
  insertMany(
    logics: Logic[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
  replaceMany(
    logics: Logic[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<number>;
}
