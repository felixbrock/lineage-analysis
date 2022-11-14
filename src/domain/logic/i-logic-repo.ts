import { Logic } from '../entities/logic';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

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
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Logic | null>;
  findBy(
    logicQueryDto: LogicQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Logic[]>;
  all(
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Logic[]>;
  insertOne(
    logic: Logic,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    logics: Logic[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
  replaceMany(
    logics: Logic[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<number>;
}
