// todo clean architecture violation

import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import {} from '../services/i-db';
import { Dashboard } from '../entities/dashboard';
import { ReadDependencies } from './read-dependencies';

export interface CreateExternalDependencyRequestDto {
  dashboard: Dashboard;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrgId?: string;
}

export interface CreateExternalDependencyAuthDto {
  isSystemInternal: boolean;
  callerOrgId?: string;
  jwt: string;
}

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
    request: CreateExternalDependencyRequestDto,
    auth: CreateExternalDependencyAuthDto
  ): Promise<CreateExternalDependencyResponse> {
    try {
      if (auth.isSystemInternal && !request.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dependency = Dependency.create({
        id: uuidv4(),
        type: 'external',
        headId: request.dashboard.id,
        tailId: request.dashboard.columnId,
        lineageId: request.lineageId,
      });

      const readExternalDependenciesResult =
        await this.#readDependencies.execute(
          {
            type: 'external',
            headId: request.dashboard.id,
            tailId: request.dashboard.columnId,
            lineageId: request.lineageId,
            targetOrgId: request.targetOrgId,
          },
          auth
        );

      if (!readExternalDependenciesResult.success)
        throw new Error(readExternalDependenciesResult.error);
      if (!readExternalDependenciesResult.value)
        throw new Error('Creating external dependency failed');
      if (readExternalDependenciesResult.value.length)
        throw new Error(
          `Attempting to create an external dependency that already exists`
        );

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(
          dependency,
          auth,
          request.targetOrgId
        );

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
