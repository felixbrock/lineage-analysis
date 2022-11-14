import { Dependency, DependencyType } from '../entities/dependency';

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
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency | null>;
  findBy(
    dependencyQueryDto: DependencyQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency[]>;
  all(auth: Auth, targetOrgId?: string): Promise<Dependency[]>;
  insertOne(
    dependency: Dependency,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string>;
  insertMany(
    dependencys: Dependency[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]>;
}
