// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency, DependencyType } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { DbConnection } from '../services/i-db';
import { Dashboard } from '../entities/dashboard';

export interface CreateExternalDependencyRequestDto {
  dashboard: Dashboard;
  selfDbtModelId: string;
  parentDbtModelIds: string[];
  lineageId: string;
  writeToPersistence: boolean;
}

export interface CreateExternalDependencyAuthDto {
  organizationId: string;
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
//   readonly #readColumns: ReadColumns;

//   readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  #dbConnection: DbConnection;

  constructor(
    // readColumns: ReadColumns,
    // readDependencies: ReadDependencies,
    dependencyRepo: IDependencyRepo
  ) {
    // this.#readColumns = readColumns;
    // this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: CreateExternalDependencyRequestDto,
    auth: CreateExternalDependencyAuthDto,
    dbConnection: DbConnection
    ): Promise<CreateExternalDependencyResponse> {
    console.log(auth);

    try {
      this.#dbConnection = dbConnection;

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: DependencyType.EXTERNAL,
        headId: request.dashboard.id,
        tailId: request.dashboard.columnId,
        lineageId: request.lineageId,
      });


      console.log(
        `${request.dashboard.url} depends on ${request.dashboard.column} from ${request.dashboard.materialisation}`
      );
    //   const readColumnsResult = await this.#readDependencies.execute(
    //     {
    //       type: request.dependencyRef.dependencyType,
    //       headId: headColumn.id,
    //       tailId: parentId,
    //       lineageId: request.lineageId,
    //     },
    //     { organizationId: auth.organizationId },
    //     dbConnection
    //   );

    //   if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    //   if (!readColumnsResult.value) throw new Error('Reading columns failed');
    //   if (readColumnsResult.value.length)
    //     throw new Error(`Column for materialization already exists`);

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(dependency, this.#dbConnection);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
