import {
  MaterializationType,
  Materialization,
} from '../entities/materialization';

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
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization | null>;
  findBy(
    materializationQueryDto: MaterializationQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization[]>;
  all(auth: Auth, targetOrgId?: string): Promise<Materialization[]>;
  insertOne(
    materialization: Materialization,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    materializations: Materialization[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
  replaceMany(
    materializations: Materialization[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<number>;
}
