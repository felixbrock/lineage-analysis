// todo clean architecture violation
import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';
import { ColumnRef } from '../entities/logic';
import { ReadColumns } from '../column/read-columns';
import { Column } from '../entities/column';
import {} from '../services/i-db';

export interface CreateDependencyRequestDto {
  dependencyRef: ColumnRef;
  selfRelationName: string;
  parentRelationNames: string[];
  lineageId: string;
  writeToPersistence: boolean;
  targetOrganizationId?: string;
}

export interface CreateDependencyAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
  jwt: string;
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

  #auth?: CreateDependencyAuthDto;

  #targetOrgId?: string;

  /* Returns the object id of the parent column which self column depends upon */
  #getParentId = async (
    dependencyRef: ColumnRef,
    parentRelationNames: string[],
    lineageId: string
  ): Promise<string> => {
    if (!this.#auth) throw new Error('auth missing');

    const readColumnsResult = await this.#readColumns.execute(
      {
        relationName: parentRelationNames,
        name: dependencyRef.name,
        lineageId,
        targetOrganizationId: this.#targetOrgId,
      },
      this.#auth
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of parent columns failed`);

    let potentialParents = readColumnsResult.value;

    if (!potentialParents.length)
      throw new ReferenceError('No parent found that matches reference');

    if (potentialParents.length > 1) {
      potentialParents = potentialParents.filter((parent) =>
        parent.relationName.includes(dependencyRef.materializationName)
      );
    }
    if (potentialParents.length !== 1)
      throw new ReferenceError('More than one matching parent');

    return potentialParents[0].id;
  };

  /* Returns column object that represents head of directed edge (the column that is referencing another column) */
  #getSelfColumn = async (
    selfRelationName: string,
    dependencyRef: ColumnRef,
    lineageId: string
  ): Promise<Column> => {
    if (!this.#auth) throw new Error('auth missing');

    const readSelfColumnResult = await this.#readColumns.execute(
      {
        relationName: selfRelationName,
        lineageId,
        name: dependencyRef.alias || dependencyRef.name,
        targetOrganizationId: this.#targetOrgId,
      },
      this.#auth
    );

    if (!readSelfColumnResult.success)
      throw new Error(readSelfColumnResult.error);
    if (!readSelfColumnResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const selfColumnMatches = readSelfColumnResult.value;

    if (!selfColumnMatches.length) throw new RangeError('No self column found');

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
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#auth = auth;
      this.#targetOrgId = request.targetOrganizationId;

      const headColumn = await this.#getSelfColumn(
        request.selfRelationName,
        request.dependencyRef,
        request.lineageId
      );

      // const parentName =
      //   request.parentRef.name.includes('$') && request.parentRef.alias
      //     ? request.parentRef.alias
      //     : request.parentRef.name;

      const parentId = await this.#getParentId(
        request.dependencyRef,
        request.parentRelationNames,
        request.lineageId
      );

      const dependency = Dependency.create({
        id: uuidv4(),
        type: request.dependencyRef.dependencyType,
        headId: headColumn.id,
        tailId: parentId,
        lineageId: request.lineageId,
      });

      const readDependencyResult = await this.#readDependencies.execute(
        {
          type: request.dependencyRef.dependencyType,
          headId: headColumn.id,
          tailId: parentId,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        auth
      );

      if (!readDependencyResult.success)
        throw new Error(readDependencyResult.error);
      if (!readDependencyResult.value)
        throw new Error('Creating dependency failed');
      if (readDependencyResult.value.length)
        throw new Error(
          `Attempting to create a dependency that already exists`
        );

      if (request.writeToPersistence)
        await this.#dependencyRepo.insertOne(
          dependency,
          auth,
          request.targetOrganizationId
        );

      return Result.ok(dependency);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      console.warn(
        'todo - fix. Creating dependency failed. Empty Result returned instead'
      );
      // return Result.fail('');
      return Result.ok();
    }
  }
}
