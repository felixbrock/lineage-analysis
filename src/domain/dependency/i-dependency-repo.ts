import { Dependency, DependencyType } from '../entities/dependency';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';

export interface DependencyQueryDto {
  type?: DependencyType;
  headId?: string;
  tailId?: string;
  lineageId: string;
}

export interface Auth {
  jwt: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface IDependencyRepo {
  findOne(
    dependencyId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency | null>;
  findBy(
    dependencyQueryDto: DependencyQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency[]>;
  all(
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency[]>;
  insertOne(
    dependency: Dependency,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    dependencys: Dependency[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
}
