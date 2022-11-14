import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

export interface MaterializationQueryDto {
  relationName?: string;
  type?: MaterializationType;
  name?: string | string[];
  schemaName?: string;
  databaseName?: string;
  logicId?: string;
  lineageId: string;
}

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface IMaterializationRepo {
  findOne(
    materializationId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization | null>;
  findBy(
    materializationQueryDto: MaterializationQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization[]>;
  all(
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization[]>;
  insertOne(
    materialization: Materialization,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    materializations: Materialization[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
  replaceMany(
    materializations: Materialization[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<number>;
}
