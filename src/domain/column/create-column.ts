import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { CreateTable } from '../table/create-table';
import { Column } from '../entities/column';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';
import { ObjectId } from 'mongodb';
import { request } from 'express';
import { IColumnRepo } from './i-column-repo';
import { ReadColumns } from './read-columns';
import { ColumnDto } from './column-dto';
import { ReadTables, ReadTablesRequestDto } from '../table/read-tables';
import {
  Dependency,
  DependencyProperties,
  Direction,
} from '../value-types/dependency';
import { Table } from '../entities/table';
import { StatementReference } from '../value-types/logic';

export interface CreateColumnRequestDto {
  tableId: string;
  selfReference: StatementReference;
  statementSourceReferences: StatementReference[];
  parentTableIds: string[];
  lineageId: string;
}

export interface CreateColumnAuthDto {
  organizationId: string;
}

export type CreateColumnResponseDto = Result<Column>;

interface DependencyAnalysisResult {
  path: string;
  columnName: string;
  tableName?: string;
  isDependency: boolean;
  type?: string;
}

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

  #DependencyTypes = { SELECT: 'select', JOIN_CONDITION: 'join_condition' };

  #analyzeStatementReference = (
    potentialDependency: StatementReference,
    selfReference: StatementReference
  ): DependencyAnalysisResult => {
    const selfReferencePath = selfReference[0];
    const selfReferenceName = selfReference[1];

    const dependencyPath = potentialDependency[0];
    const dependencyName = potentialDependency[1].includes('.')
      ? potentialDependency[1].split('.').slice(-1)[0]
      : potentialDependency[1];
    const dependencyTable = potentialDependency[1].includes('.')
      ? potentialDependency[1].split('.').slice(0)[0]
      : '';

    const lastKeyStatementIndex = dependencyPath.lastIndexOf(
      SQLElement.STATEMENT
    );
    if (lastKeyStatementIndex === -1 || !lastKeyStatementIndex)
      throw new RangeError('Statement not found for column reference');

    const keyStatement = dependencyPath.slice(
      0,
      lastKeyStatementIndex + SQLElement.STATEMENT.length
    );

    const lastRootStatementIndex = selfReferencePath.lastIndexOf(
      SQLElement.STATEMENT
    );
    if (lastRootStatementIndex === -1 || !lastRootStatementIndex)
      throw new RangeError('Statement not found for column root');

    const rootStatement = dependencyPath.slice(
      0,
      lastRootStatementIndex + SQLElement.STATEMENT.length
    );

    const isStatementDependency =
      !dependencyPath.includes(SQLElement.INSERT_STATEMENT) &&
      !dependencyPath.includes(SQLElement.COLUMN_DEFINITION) &&
      keyStatement === rootStatement &&
      dependencyPath.includes(SQLElement.COLUMN_REFERENCE);

    const result: DependencyAnalysisResult = {
      path: dependencyPath,
      columnName: dependencyName,
      isDependency: false,
    };
    if (dependencyTable) result.tableName = dependencyTable;

    if (!isStatementDependency) return result;

    if (selfReferencePath.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
      if (
        dependencyPath.includes(SQLElement.SELECT_CLAUSE_ELEMENT) &&
        dependencyName === selfReferenceName
      ) {
        result.isDependency = true;
        result.type = this.#DependencyTypes.SELECT;

        return result;
      }
    }
    return result;
  };

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

  #getDependencyPrototypes = async (
    selfColumnReference: StatementReference,
    statementReferences: StatementReference[],
    parentTableIds: string[]
  ): Promise<DependencyProperties[]> => {
    const dependencyReferences = statementReferences
      .map((reference) =>
        this.#analyzeStatementReference(reference, selfColumnReference)
      )
      .filter((result) => result.isDependency);

    if (!dependencyReferences.length) return [];

    const dependencyNames = dependencyReferences.map(
      (reference) => reference.columnName
    );

    const readColumnsResult = await this.#readColumns.execute(
      { tableId: parentTableIds, name: dependencyNames },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    const potentialSourceColumns = readColumnsResult.value;

    if (potentialSourceColumns.length === 0)
      throw new ReferenceError(`No source columns found`);

    const columnMatchesPerReference: [DependencyAnalysisResult, number][] =
      dependencyReferences.map((reference) => [
        reference,
        potentialSourceColumns.filter(
          (column) => column.name === reference.columnName
        ).length,
      ]);

    if (columnMatchesPerReference.every((match) => match[1] === 1))
      return potentialSourceColumns.map((column) => {
        const dependencyReference = dependencyReferences.filter(
          (reference) => reference.columnName === column.name
        );

        if (dependencyReference.length !== 1)
          throw new ReferenceError('Invalid column match-reference relation');

        const type = dependencyReference[0].type;
        if (!type) throw new ReferenceError('Dependency type not declared');

        return {
          type,
          columnId: column.id,
          direction: Direction.UPSTREAM,
        };
      });

    if (columnMatchesPerReference.some((match) => match[1] === 0))
      throw new ReferenceError(
        'Referenced column does not exist along data warehouse tables'
      );

    const matchesToClarify = columnMatchesPerReference.filter(
      (match) => match[1] > 1
    );
    const columnsToClarify = potentialSourceColumns.filter(
      (column) =>
        matchesToClarify.filter((match) => match[0].columnName === column.name)
          .length
    );

    const clarifiedMatches: Column[] = await Promise.all(
      matchesToClarify.map(async (match) => {
        const columnName = match[0].columnName;
        const tableName = match[0].tableName;

        if (tableName) {
          const dependencySource = await this.#getDependencySourceByTableRef(
            tableName,
            columnName,
            columnsToClarify
          );
          if (dependencySource) return dependencySource;
        }
        if (match[0].path.includes(SQLElement.FROM_EXPRESSION_ELEMENT)) {
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

    const dependencyPropertyObjs: DependencyProperties[] = [];

    const matches = columnMatchesPerReference.filter((match) => match[1] === 1);
    const matchedColumns = potentialSourceColumns.filter(
      (column) =>
        matches.filter((match) => match[0].columnName === column.name).length
    );

    const type = dependencyPropertyObjs.push(
      ...matchedColumns.map((column) => {
        const dependencyReference = dependencyReferences.filter(
          (reference) => reference.columnName === column.name
        );

        if (dependencyReference.length !== 1)
          throw new ReferenceError('Invalid column match-reference relation');

        const type = dependencyReference[0].type;
        if (!type) throw new ReferenceError('Dependency type not declared');

        return {
          type,
          columnId: column.id,
          direction: Direction.UPSTREAM,
        };
      })
    );
    dependencyPropertyObjs.push(
      ...clarifiedMatches.map((column) => {
        const dependencyReference = dependencyReferences.filter(
          (reference) => reference.columnName === column.name
        );

        if (dependencyReference.length !== 1)
          throw new ReferenceError('Invalid column match-reference relation');

        const type = dependencyReference[0].type;
        if (!type) throw new ReferenceError('Dependency type not declared');

        return {
          type,
          columnId: column.id,
          direction: Direction.UPSTREAM,
        };
      })
    );

    return dependencyPropertyObjs;
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
      const dependencyPrototypes = await this.#getDependencyPrototypes(
        request.selfReference,
        request.statementSourceReferences,
        request.parentTableIds
      );

      const column = Column.create({
        id: new ObjectId().toHexString(),
        name: request.selfReference[1],
        tableId: request.tableId,
        dependencyPrototypes,
        lineageId: request.lineageId
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
        throw new Error(`Column for table already exists`);

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
