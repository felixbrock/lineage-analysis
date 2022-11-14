import { Lineage } from '../entities/lineage';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

export interface LineageUpdateDto {
  completed?: boolean;
}

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface ILineageRepo {
  findOne(
    lineageId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Lineage | null>;
  findLatest(
    filter: { completed: boolean },
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Lineage | null>;
  all(
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Lineage[]>;
  insertOne(
    lineage: Lineage,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  updateOne(
    lineageId: string,
    updateDto: LineageUpdateDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
}
