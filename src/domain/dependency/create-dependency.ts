// todo clean architecture violation
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dependency } from '../entities/dependency';
import { IDependencyRepo } from './i-dependency-repo';
import { ReadDependencies } from './read-dependencies';
import { ColumnRef } from '../value-types/logic';
import { ReadColumns } from '../column/read-columns';

enum DependencyType {
  DATA = 'DATA',
  QUERY = 'QUERY',
}

export interface CreateDependencyRequestDto {
  selfRef: ColumnRef;
  selfModelId: string;
  parentRef: ColumnRef;
  parentModelDbtIds: string[];
  lineageId: string;
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

  #getParentId = async (
    parentModelDbtIds: string[],
    parentName: string,
    lineageId: string
  ): Promise<string> => {
    const readColumnsResult = await this.#readColumns.execute(
      { tableId: parentModelDbtIds, name: parentName, lineageId },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const potentialParents = readColumnsResult.value;

    if (!potentialParents.length)
      throw new ReferenceError('No parent found that matches reference');

    if (potentialParents.length > 1)
      throw new ReferenceError('More than one matching parent');

    return potentialParents[0].id;
  };

  // #getDependencyPrototypes = async (
  //   selfRef: StatementReference,
  //   statementRefs: StatementReference[],
  //   parentTableIds: string[],
  //   lineageId: string
  // ): Promise<DependencyProperties[]> => {

  // };

  //   #getDependencyProperties = (
  //     statementRefs: Refs,
  //     tailColumnRefIndex: number
  //   ) => {
  //     const tailColumnRef = statementRefs.columns[tailColumnRefIndex];

  //     statementRefs.columns.forEach()

  //  // const dependencyReferences = statementRefs
  //     //   .map((reference) => this.#analyzeStatementReference(reference, selfRef))
  //     //   .filter((result) => result.isDependency);

  //     // if (!dependencyReferences.length) return [];

  //   };

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
      // const dependencyReferences = statementRefs
      //   .map((reference) => this.#analyzeStatementReference(reference, selfRef))
      //   .filter((result) => result.isDependency);

      // if (!dependencyReferences.length) return [];

      // const columnDependencyNames = dependencyReferences.map((reference) => {
      //   if (!reference.columnName)
      //     throw new ReferenceError('COLUMN reference is missing name');
      //   return reference.columnName;
      // });

      const readSelfColumnResult = await this.#readColumns.execute(
        { dbtModelId: request.selfModelId, lineageId: request.lineageId },
        { organizationId: 'todo' }
      );

      if (!readSelfColumnResult.success) throw new Error(readSelfColumnResult.error);
      if (!readSelfColumnResult.value)
        throw new ReferenceError(`Reading of dependency columns failed`);

      const [selfColumn] = readSelfColumnResult.value;

      const parentId = request.parentRef.tableName
        ? request.parentRef.tableName
        : await this.#getParentId(
            request.parentModelDbtIds,
            request.parentRef.name,
            request.lineageId
          );
      // const potentialColumnDependencies =
      //   await this.#getPotentialColumnDependencies(
      //     parentTableIds,
      //     lineageId,
      //     columnDependencyNames
      //   );

      // const finalMatches = matches.filter(
      //   (match) => match.referenceColumnRatio > 1
      // );
      // const matchedColumns = potentialColumnDependencies.filter(
      //   (column) =>
      //     finalMatches.filter(
      //       (match) => match.analysisResult.columnName === column.name
      //     ).length
      // );

      // const dependencyPropertyObjs: DependencyProperties[] = [];

      // dependencyPropertyObjs.push(
      //   ...matchedColumns.map((column) =>
      //     this.#buildDependencyPrototype(dependencyReferences, column)
      //   )
      // );

      // const matches: Match[] = dependencyReferences.map((reference) => ({
      //   analysisResult: reference,
      //   referenceColumnRatio: potentialColumnDependencies.filter(
      //     (column) => column.name === reference.columnName
      //   ).length,
      // }));

      // if (matches.every((match) => match.referenceColumnRatio === 1))
      //   return potentialColumnDependencies.map((column) =>
      //     this.#buildDependencyPrototype(dependencyReferences, column)
      //   );

      // if (matches.some((match) => match.referenceColumnRatio === 0))
      //   throw new ReferenceError(
      //     'Referenced column does not exist along data warehouse tables'
      //   );

      // const matchesToClarify: Match[] = matches.filter(
      //   (match) => match.referenceColumnRatio > 1
      // );
      // const columnsToClarify = potentialColumnDependencies.filter(
      //   (column) =>
      //     matchesToClarify.filter(
      //       (match) => match.analysisResult.columnName === column.name
      //     ).length
      // );

      // const clarifiedColumns: Column[] = await Promise.all(
      //   matchesToClarify.map(async (match) =>
      //     this.#getClarifiedMatchedColumn(
      //       lineageId,
      //       match,
      //       columnsToClarify,
      //       statementRefs
      //     )
      //   )
      // );

      // const finalMatches = matches.filter(
      //   (match) => match.referenceColumnRatio > 1
      // );
      // const matchedColumns = potentialColumnDependencies.filter(
      //   (column) =>
      //     finalMatches.filter(
      //       (match) => match.analysisResult.columnName === column.name
      //     ).length
      // );

      // const dependencyPropertyObjs: DependencyProperties[] = [];

      // dependencyPropertyObjs.push(
      //   ...matchedColumns.map((column) =>
      //     this.#buildDependencyPrototype(dependencyReferences, column)
      //   )
      // );
      // dependencyPropertyObjs.push(
      //   ...clarifiedColumns.map((column) =>
      //     this.#buildDependencyPrototype(dependencyReferences, column)
      //   )
      // );

      // return dependencyPropertyObjs;

      const dependency = Dependency.create({
        id: new ObjectId().toHexString(),
        type: DependencyType.DATA,
        headColumnId: selfColumn.id,
        tailColumnId: parentId,
        lineageId: request.lineageId,
      });

      const readColumnsResult = await this.#readDependencies.execute(
        {
          type: DependencyType.DATA,
          headColumnId: selfColumn.id,
          tailColumnId: parentId,
          lineageId: request.lineageId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(`Column for table already exists`);

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

// #getColumnSourceOfTable = async (
//   tableName: string,
//   columnName: string,
//   potentialDependencies: Column[],
//   lineageId: string
// ): Promise<Column | null> => {
//   const readTablesResult = await this.#readTables.execute(
//     { name: tableName, lineageId },
//     { organizationId: 'todo' }
//   );

//   if (!readTablesResult.success) throw new Error(readTablesResult.error);
//   if (!readTablesResult.value)
//     throw new ReferenceError(`Reading of table failed`);

//   const tables = readTablesResult.value;

//   if (tables.length === 0)
//     throw new ReferenceError('Requested table not found');

//   // todo - Assumption correct that table names have to be unique across data warehouse?
//   const potentialColumnSources = potentialDependencies.filter(
//     (column) => column.name === columnName && column.tableId === tables[0].id
//   );

//   if (!potentialColumnSources.length)
//     throw new ReferenceError('Failed to identify referenced column');
//   if (potentialColumnSources.length > 1)
//     throw new ReferenceError(
//       'The same table-column combination exist multiple times in Data Warehouse'
//     );

//   return potentialColumnSources[0];
// };

// #buildDependencyPrototype = (
//   dependencyReferences: DependencyAnalysisResult[],
//   column: Column
// ): DependencyProperties => {
//   const dependencyReference = dependencyReferences.filter(
//     (reference) => reference.columnName === column.name
//   );

//   if (dependencyReference.length === 0)
//     throw new ReferenceError('Invalid column match-reference relation');

//   const { type } = dependencyReference[0];
//   if (!type) throw new ReferenceError('Dependency type not declared');

//   return {
//     type,
//     columnId: column.id,
//     direction: Direction.UPSTREAM,
//   };
// };

// #getClarifiedMatchedColumn = async (
//   lineageId: string,
//   match: Match,
//   columnsToClarify: Column[],
//   statementRefs: StatementReference[]
// ): Promise<Column> => {
//   const { columnName, tableName } = match.analysisResult;

//   if (!columnName) throw new ReferenceError('Name of column to be clarified');

//   if (tableName) {
//     const dependencySource = await this.#getColumnSourceOfTable(
//       tableName,
//       columnName,
//       columnsToClarify,
//       lineageId
//     );
//     if (dependencySource) return dependencySource;
//   }
//   if (
//     match.analysisResult.path.includes(SQLElement.FROM_EXPRESSION_ELEMENT)
//   ) {
//     const fromTables = statementRefs.filter((element) =>
//       [SQLElement.FROM_EXPRESSION_ELEMENT, SQLElement.TABLE_REFERENCE].every(
//         (key) => element.path.includes(key)
//       )
//     );

//     if (fromTables.length > 1)
//       throw new ReferenceError("Multiple 'from' tables identified");
//     if (!fromTables.length)
//       throw new ReferenceError("'From' table not found");
//     if (!fromTables[0].tableName)
//       throw new ReferenceError(
//         'table name of TABLE reference does not exist'
//       );

//     const dependencySource = await this.#getColumnSourceOfTable(
//       fromTables[0].tableName,
//       columnName,
//       columnsToClarify,
//       lineageId
//     );
//     if (dependencySource) return dependencySource;
//   }

//   throw new ReferenceError(`Table for column ${columnName} not found`);
// };

// interface DependencyAnalysisResult extends StatementReference {
//   isDependency: boolean;
// }

// interface Match {
//   analysisResult: DependencyAnalysisResult;
//   referenceColumnRatio: number;
// }

// const dependencyPrototypes = await this.#getDependencyPrototypes(
//   request.selfRef,
//   request.statementSourceReferences,
//   request.parentTableIds,
//   request.lineageId
// );

// if (!request.selfRef.columnName)
//   throw new ReferenceError('Name of column to be created is undefined');

// export interface CreateColumnRequestDto {
//   selfRef: StatementReference;
//   tableId: string;
//   statementSourceReferences: StatementReference[];
//   parentTableIds: string[];
//   lineageId: string;
// }
