// todo clean architecture violation

import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { Dashboard } from '../entities/dashboard';
import { ReadDependencies } from './read-dependencies';
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

export interface CreateExternalDependencyRequestDto {
  dashboard: Dashboard;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrgId?: string;
}

export type CreateExternalDependencyAuthDto = BaseAuth;

export type CreateExternalDependencyResponse = Result<Dependency>;

export class CreateExternalDependency
  implements
    IUseCase<
      CreateExternalDependencyRequestDto,
      CreateExternalDependencyResponse,
      CreateExternalDependencyAuthDto
    >
{
  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  constructor(
    readDependencies: ReadDependencies,
    dependencyRepo: IDependencyRepo
  ) {
    this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    req: CreateExternalDependencyRequestDto,
    auth: CreateExternalDependencyAuthDto,
    connPool: IConnectionPool
  ): Promise<CreateExternalDependencyResponse> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dependency = Dependency.create({
        id: uuidv4(),
        type: 'external',
        headId: req.dashboard.id,
        tailId: req.dashboard.columnId,
        lineageId: req.lineageId,
      });

      const readExternalDependenciesResult =
        await this.#readDependencies.execute(
          {
            type: 'external',
            headId: req.dashboard.id,
            tailId: req.dashboard.columnId,
            lineageId: req.lineageId,
            targetOrgId: req.targetOrgId,
          },
          auth,
          connPool
        );

      if (!readExternalDependenciesResult.success)
        throw new Error(readExternalDependenciesResult.error);
      if (!readExternalDependenciesResult.value)
        throw new Error('Creating external dependency failed');
      if (readExternalDependenciesResult.value.length)
        throw new Error(
          `Attempting to create an external dependency that already exists`
        );

      if (req.writeToPersistence)
        await this.#dependencyRepo.insertOne(
          dependency,
          auth,
          connPool,
          req.targetOrgId
        );

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
