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
  targetOrganizationId: string;
}

export interface CreateExternalDependencyAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId: string;
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
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: DependencyType.EXTERNAL,
        headId: request.dashboard.id,
        tailId: request.dashboard.columnId,
        lineageId: request.lineageId,
        organizationId: request.targetOrganizationId
      });

      const readExternalDependenciesResult = await this.#readDependencies.execute(
        {
          type: DependencyType.EXTERNAL,
          headId: request.dashboard.id,
          tailId: request.dashboard.columnId,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId
        },
        { callerOrganizationId: auth.callerOrganizationId, isSystemInternal: auth.isSystemInternal },
        dbConnection
      );

      if (!readExternalDependenciesResult.success) throw new Error(readExternalDependenciesResult.error);
      if (!readExternalDependenciesResult.value) throw new Error('Creating external dependency failed');
      if (readExternalDependenciesResult.value.length)
        throw new Error(`Attempting to create an external dependency that already exists`);

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(dependency, this.#dbConnection);


      return Result.ok(dependency);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
