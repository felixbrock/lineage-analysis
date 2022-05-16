// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency} from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';
import { ColumnRef } from '../entities/logic';
import { ReadColumns } from '../column/read-columns';
import { Column } from '../entities/column';

export interface CreateDependencyRequestDto {
  dependencyRef: ColumnRef;
  selfDbtModelId: string;
  parentDbtModelIds: string[];
  lineageId: string;
  writeToPersistence: boolean;
}

export interface CreateDependencyAuthDto {
  organizationId: string;
}

export type CreateDependencyResponse = Result<Dependency>;

export class CreateDependency
  implements
    IUseCase<
      CreateDependencyRequestDto,
      CreateDependencyResponse,
      CreateDependencyAuthDto
    >
{
  readonly #readColumns: ReadColumns;

  readonly #readDependencies: ReadDependencies;

  readonly #dependencyRepo: IDependencyRepo;

  /* Returns the object id of the parent column which self column depends upon */
  #getParentId = async (
    dependencyRef: ColumnRef,
    parentDbtModelIds: string[],
    lineageId: string
  ): Promise<string> => {
    const readColumnsResult = await this.#readColumns.execute(
      { dbtModelId: parentDbtModelIds, name: dependencyRef.name, lineageId },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of parent columns failed`);

    const potentialParents = readColumnsResult.value;

    if (!potentialParents.length)
      throw new ReferenceError('No parent found that matches reference');

    if (potentialParents.length > 1)
      throw new ReferenceError('More than one matching parent');

    return potentialParents[0].id;
  };

  /* Returns only dbt model ids that include the self column's table name */
  #getMatchingDbtModelIds = (
    parentDbtModelIds: string[],
    parentMaterializationName: string
  ): string[] =>
    parentDbtModelIds.filter((id) =>
      id.toLowerCase().includes(parentMaterializationName.toLowerCase())
    );

  #getSelfColumn = async (
    selfDbtModelId: string,
    dependencyRef: ColumnRef,
    lineageId: string
  ): Promise<Column> => {
    const readSelfColumnResult = await this.#readColumns.execute(
      {
        dbtModelId: selfDbtModelId,
        lineageId,
        name: dependencyRef.alias || dependencyRef.name,
      },
      { organizationId: 'todo' }
    );

    if (!readSelfColumnResult.success)
      throw new Error(readSelfColumnResult.error);
    if (!readSelfColumnResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const selfColumnMatches = readSelfColumnResult.value;

    if (!selfColumnMatches.length)
    throw new RangeError('No self column found');

    if (selfColumnMatches.length === 1) return selfColumnMatches[0];

    throw new RangeError('0 or more than 1 selfColumns found');

    // const parentName: string = parentRef.name.includes('$')
    //   ? parentRef.name
    //   : parentRef.alias || parentRef.name;

    // const filterResult = readSelfColumnResult.value.filter(
    //   (column) => column.name === parentName
    // );

    // if (filterResult.length !== 1)
    //   throw new RangeError('0 or more than 1 selfColumns found');

    // return filterResult[0];
  };

  constructor(
    readColumns: ReadColumns,
    readDependencies: ReadDependencies,
    dependencyRepo: IDependencyRepo
  ) {
    this.#readColumns = readColumns;
    this.#readDependencies = readDependencies;
    this.#dependencyRepo = dependencyRepo;
  }

  async execute(
    request: CreateDependencyRequestDto,
    auth: CreateDependencyAuthDto
  ): Promise<CreateDependencyResponse> {
    console.log(auth);

    try {
      const headColumn = await this.#getSelfColumn(
        request.selfDbtModelId,
        request.dependencyRef,
        request.lineageId
      );

      // const parentName =
      //   request.parentRef.name.includes('$') && request.parentRef.alias
      //     ? request.parentRef.alias
      //     : request.parentRef.name;

      const parentId = await this.#getParentId(
        request.dependencyRef,
        request.parentDbtModelIds,
        request.lineageId
      );

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: request.dependencyRef.dependencyType,
        headId: headColumn.id,
        tailId: parentId,
        lineageId: request.lineageId,
      });

      const readColumnsResult = await this.#readDependencies.execute(
        {
          type: request.dependencyRef.dependencyType,
          headId: headColumn.id,
          tailId: parentId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for materialization already exists`);

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(dependency);

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
