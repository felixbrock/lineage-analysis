// todo clean architecture violation
import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency, DependencyType } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { IDbConnection } from '../services/i-db';

export interface CreateDependenciesRequestDto {
  toCreate: { headId: string; tailId: string; type: DependencyType }[];
  writeToPersistence: boolean;
  targetOrgId?: string;
}

export interface CreateDependenciesAuthDto {
  isSystemInternal: boolean;
  callerOrgId?: string;
  jwt: string;
}

export type CreateDependenciesResponse = Result<Dependency[]>;

export class CreateDependencies
  implements
    IUseCase<
      CreateDependenciesRequestDto,
      CreateDependenciesResponse,
      CreateDependenciesAuthDto,
      IDbConnection
    >
{
  readonly #dependencyRepo: IDependencyRepo;

  constructor(dependencyRepo: IDependencyRepo) {
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    req: CreateDependenciesRequestDto,
    auth: CreateDependenciesAuthDto,
    dbConnection: IDbConnection
  ): Promise<CreateDependenciesResponse> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dependencies = req.toCreate.map((el) =>
        Dependency.create({
          id: uuidv4(),
          type: el.type,
          headId: el.headId,
          tailId: el.tailId,
        })
      );

      if (req.writeToPersistence)
        await this.#dependencyRepo.insertMany(dependencies, auth, dbConnection);

      return Result.ok(dependencies);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      console.warn(
        'todo - fix. Creating dependency failed. Empty Result returned instead'
      );
      // return Result.fail('');
      return Result.ok();
    }
  }
}
