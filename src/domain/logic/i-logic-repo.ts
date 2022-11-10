import { Logic } from '../entities/logic';

export interface LogicQueryDto {
  relationName?: string;
  logicId: string;
}

export interface Auth {
  jwt: string;
  callerOrgId: string;
  isSystemInternal: boolean;
}

export interface ILogicRepo {
  findOne(
    logicId: string,
    targetOrgId: string,
    auth: Auth
  ): Promise<Logic | null>;
  findBy(
    logicQueryDto: LogicQueryDto,
    targetOrgId: string,
    auth: Auth
  ): Promise<Logic[]>;
  all(targetOrgId: string, auth: Auth): Promise<Logic[]>;
  insertOne(logic: Logic, targetOrgId: string, auth: Auth): Promise<string>;
  insertMany(
    logics: Logic[],
    targetOrgId: string,
    auth: Auth
  ): Promise<string[]>;
  replaceMany(
    logics: Logic[],
    targetOrgId: string,
    auth: Auth
  ): Promise<number>;
}
