// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency, DependencyType } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { DbConnection } from '../services/i-db';
import { Dashboard } from '../entities/dashboard';
import { ReadDependencies } from './read-dependencies';

export interface CreateExternalDependencyRequestDto {
  dashboard: Dashboard;
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId?: string;
}

export interface CreateExternalDependencyAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateExternalDependencyResponse = Result<Dependency>;

export class CreateExternalDependency
  implements
    IUseCase<
      CreateExternalDependencyRequestDto,
      CreateExternalDependencyResponse,
      CreateExternalDependencyAuthDto,
      DbConnection
    >
{
  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  #dbConnection: DbConnection;

  constructor(
    readDependencies: ReadDependencies,
    dependencyRepo: IDependencyRepo
  ) {
    this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: CreateExternalDependencyRequestDto,
    auth: CreateExternalDependencyAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateExternalDependencyResponse> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
        if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      let organizationId: string;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.#dbConnection = dbConnection;

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: DependencyType.EXTERNAL,
        headId: request.dashboard.id,
        tailId: request.dashboard.columnId,
        lineageId: request.lineageId,
        organizationId,
      });

      const readExternalDependenciesResult =
        await this.#readDependencies.execute(
          {
            type: DependencyType.EXTERNAL,
            headId: request.dashboard.id,
            tailId: request.dashboard.columnId,
            lineageId: request.lineageId,
            targetOrganizationId: request.targetOrganizationId,
          },
          { isSystemInternal: auth.isSystemInternal, callerOrganizationId: auth.callerOrganizationId },
          dbConnection
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
        await this.#dependencyRepo.insertOne(dependency, this.#dbConnection);

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error)
        return Result.fail(error.stack || error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
