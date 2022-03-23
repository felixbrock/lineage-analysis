import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { CreateTable } from '../table/create-table';
import { Column } from '../entities/column';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';
import { ObjectId } from 'mongodb';
import { StatementReference } from '../entities/model';
import { request } from 'express';
import { IColumnRepo } from './i-column-repo';
import { ReadColumns } from './read-columns';
import { ColumnDto } from './column-dto';
import { ReadTables, ReadTablesRequestDto } from '../table/read-tables';
import { Dependency, Direction } from '../value-types/dependency';

export interface CreateColumnRequestDto {
  tableId: string;
  selfReference: StatementReference;
  statementSourceReferences: StatementReference[];
  parentTableIds: string[];
}

export interface CreateColumnAuthDto {
  organizationId: string;
}

export type CreateColumnResponseDto = Result<Column>;

export class CreateColumn
  implements
    IUseCase<
      CreateColumnRequestDto,
      CreateColumnResponseDto,
      CreateColumnAuthDto
    >
{
  #columnRepo: IColumnRepo;

  #readColumns: ReadColumns;
  #readTables: ReadTables;

  #isStatementDependency = (key: string, columnReference: string): boolean => {
    const lastKeyStatementIndex = key.lastIndexOf(SQLElement.STATEMENT);
    if (lastKeyStatementIndex === -1 || !lastKeyStatementIndex)
      throw new RangeError('Statement not found for column reference');

    const keyStatement = key.slice(
      0,
      lastKeyStatementIndex + SQLElement.STATEMENT.length
    );

    const lastRootStatementIndex = columnReference.lastIndexOf(
      SQLElement.STATEMENT
    );
    if (lastRootStatementIndex === -1 || !lastRootStatementIndex)
      throw new RangeError('Statement not found for column root');

    const rootStatement = key.slice(
      0,
      lastRootStatementIndex + SQLElement.STATEMENT.length
    );

    return (
      !key.includes(SQLElement.INSERT_STATEMENT) &&
      !key.includes(SQLElement.COLUMN_DEFINITION) &&
      keyStatement === rootStatement &&
      key.includes(SQLElement.COLUMN_REFERENCE)
    );
  };

  // #getDependencyType = (key: string) => {
  //   if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT))
  //     return 'select';
  //   else if (key.includes(SQLElement.JOIN_ON_CONDITION))
  //     return 'join_condition';
  //   return 'unspecified';
  // };

  #getDependencySourceByTableRef = async (
    tableName: string,
    columnName: string,
    potentialDependencies: Column[]
  ) => {
    const readTablesResult = await this.#readTables.execute(
      { name: tableName },
      { organizationId: 'todo' }
    );

    if (!readTablesResult.success) throw new Error(readTablesResult.error);
    if (!readTablesResult.value)
      throw new ReferenceError(`Reading of table failed`);

    const tables = readTablesResult.value;

    if (tables.length === 0)
      throw new ReferenceError('Requested table not found');

    const columnMatchesPerTable = tables.map((table) =>
      potentialDependencies.filter(
        (column) => column.name === columnName && column.tableId === table.id
      )
    );

    const finalMatches = columnMatchesPerTable.map((tableColumns) => {
      if (tableColumns.length > 2)
        throw new ReferenceError(
          'Multiple columns with the same name found in the same table'
        );
      if (tableColumns.length === 1) return tableColumns[0];
    });

    if (!finalMatches.length)
      throw new ReferenceError('Failed to identify referenced column');
    if (finalMatches.length > 1)
      throw new ReferenceError(
        'The same table-column exist multiple times in Data Warehouse'
      );

    return finalMatches[0];
  };

  #getDependencies = async (
    selfColumnReference: StatementReference,
    statementReferences: StatementReference[],
    parentTableIds: string[]
  ): Promise<Dependency[]> => {
    const statementColumnReferences = statementReferences.filter((reference) =>
      this.#isStatementDependency(reference[0], selfColumnReference[0])
    );

    if (!statementColumnReferences.length) return [];

    const columnReferenceNames = statementColumnReferences.map(
      (reference) => reference[1]
    );

    const readColumnsResult = await this.#readColumns.execute(
      { tableId: parentTableIds, name: columnReferenceNames },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const potentialSourceColumns = readColumnsResult.value;

    if (potentialSourceColumns.length === 0)
      throw new ReferenceError(`No source columns found`);

    const matchesPerName: [StatementReference, number][] =
      statementColumnReferences.map((reference) => [
        reference,
        potentialSourceColumns.filter((column) => column.name === reference[1])
          .length,
      ]);

    if (matchesPerName.every((match) => match[1] === 1))
      return potentialSourceColumns.map((column) =>
        Dependency.create({
          type: 'todo',
          columnId: column.id,
          direction: Direction.UPSTREAM,
        })
      );

    if (matchesPerName.filter((match) => match[1] === 0).length)
      throw new ReferenceError(
        'Referenced column does not exist along data warehouse tables'
      );

    const matchesToClarify = matchesPerName.filter((match) => match[1] > 1);
    const columnsToClarify = potentialSourceColumns.filter(
      (column) =>
        matchesToClarify.filter((match) => match[0][1] === column.name).length
    );

    const clarifiedMatches: Column[] = await Promise.all(
      matchesToClarify.map(async (match) => {
        const columnName = match[0].includes('.')
          ? match[0][1].split('.').slice(-1)[0]
          : match[0][1];
        const tableName = match[0].includes('.')
          ? match[0][1].split('.').slice(0)[0]
          : '';

        if (tableName) {
          const dependencySource = await this.#getDependencySourceByTableRef(
            tableName,
            columnName,
            columnsToClarify
          );
          if (dependencySource) return dependencySource;
        }
        if (match[0][0].includes(SQLElement.FROM_EXPRESSION_ELEMENT)) {
          const fromTables = statementReferences.filter((element) =>
            [
              SQLElement.FROM_EXPRESSION_ELEMENT,
              SQLElement.TABLE_REFERENCE,
            ].every((key) => element[0].includes(key))
          );

          if (fromTables.length > 1)
            throw new ReferenceError("Multiple 'from' tables identified");
          if (!fromTables.length)
            throw new ReferenceError("'From' table not found");

          const dependencySource = await this.#getDependencySourceByTableRef(
            fromTables[0][1],
            columnName,
            columnsToClarify
          );
          if (dependencySource) return dependencySource;
        }

        throw new ReferenceError(`Table for column ${columnName} not found`);
      })
    );

    const dependencies: Dependency[] = [];

    const matches = matchesPerName.filter((match) => match[1] === 1);
    const matchedColumns = potentialSourceColumns.filter(
      (column) => matches.filter((match) => match[0][1] === column.name).length
    );

    dependencies.concat(
      matchedColumns.map((column) =>
        Dependency.create({
          type: 'todo',
          columnId: column.id,
          direction: Direction.UPSTREAM,
        })
      )
    );
    dependencies.concat(
      clarifiedMatches.map((column) =>
        Dependency.create({
          type: 'todo',
          columnId: column.id,
          direction: Direction.UPSTREAM,
        })
      )
    );

    return dependencies;
  };

  constructor(
    readColumns: ReadColumns,
    readTables: ReadTables,
    columnRepo: IColumnRepo
  ) {
    this.#readColumns = readColumns;
    this.#readTables = readTables;
    this.#columnRepo = columnRepo;
  }

  async execute(
    request: CreateColumnRequestDto,
    auth: CreateColumnAuthDto
  ): Promise<CreateColumnResponseDto> {
    try {
      const dependencies = await this.#getDependencies(
        request.selfReference,
        request.statementSourceReferences,
        request.parentTableIds
      );

      const column = Column.create({
        id: new ObjectId().toHexString(),
        name: request.selfReference[1],
        tableId: request.tableId,
        dependencies,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.selfReference[1],
          tableId: request.tableId,
        },
        { organizationId: auth.organizationId }
      );

      if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
      if (!readColumnsResult.value) throw new Error('Reading columns failed');
      if (readColumnsResult.value.length)
        throw new Error(
          `Column for table already exists`
        );

      await this.#columnRepo.insertOne(column);

      // todo-write to persistence

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(column);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
