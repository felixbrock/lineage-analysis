import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Column } from '../entities/column';
import SQLElement from '../value-types/sql-element';
import { IColumnRepo } from './i-column-repo';
import { ReadColumns } from './read-columns';
import { ReadTables } from '../table/read-tables';
import { DependencyProperties, Direction } from '../value-types/dependency';
import { StatementReference } from '../value-types/logic';

export interface CreateColumnRequestDto {
  tableId: string;
  selfReference: SelfColumnReference;
  statementSourceReferences: StatementReference[];
  parentTableIds: string[];
  lineageId: string;
}

export interface CreateColumnAuthDto {
  organizationId: string;
}

export interface SelfColumnReference {
  columnName: string;
  tableName?: string;
  path: string;
  statementIndex: number;
}

export type CreateColumnResponseDto = Result<Column>;

interface DependencyAnalysisResult {
  path: string;
  columnName: string;
  tableName?: string;
  isDependency: boolean;
  type?: string;
}

interface Match {
  analysisResult: DependencyAnalysisResult;
  referenceColumnRatio: number;
}

export class CreateColumn
  implements
    IUseCase<
      CreateColumnRequestDto,
      CreateColumnResponseDto,
      CreateColumnAuthDto
    >
{
  readonly #columnRepo: IColumnRepo;

  readonly #readColumns: ReadColumns;

  readonly #readTables: ReadTables;

  readonly #dependencyTypes = {
    SELECT: 'select',
    JOIN_CONDITION: 'join_condition',
  };

  #getStatementRoot = (path: string): string => {
    const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
    if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
      // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
      throw new RangeError('Statement root not found for column reference');

    return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  };

  #analyzeStatementReference = (
    potentialDependency: StatementReference,
    selfReference: StatementReference
  ): DependencyAnalysisResult => {
    const selfReferencePath = selfReference.path;
    // const selfReferenceName = selfReference.name.includes('.')
    //   ? selfReference.name.split('.').slice(-1)[0]
    //   : selfReference.name;
    // const selfReferenceTable = selfReference.name.includes('.')
    //   ? selfReference.name.split('.').slice(0)[0]
    //   : '';

    const dependencyPath = potentialDependency.path;
    const dependencyName = potentialDependency.name.includes('.')
      ? potentialDependency.name.split('.').slice(-1)[0]
      : potentialDependency.name;
    const dependencyTable = potentialDependency.name.includes('.')
      ? potentialDependency.name.split('.').slice(0)[0]
      : '';

    const dependencyStatementRoot = this.#getStatementRoot(dependencyPath);
    const selfStatementRoot = this.#getStatementRoot(selfReferencePath);

    const isStatementDependency =
      !dependencyPath.includes(SQLElement.INSERT_STATEMENT) &&
      !dependencyPath.includes(SQLElement.COLUMN_DEFINITION) &&
      dependencyStatementRoot === selfStatementRoot &&
      dependencyPath.includes(SQLElement.COLUMN_REFERENCE);

    const resultObj: DependencyAnalysisResult = {
      path: dependencyPath,
      columnName: dependencyName,
      isDependency: false,
    };

    if (!isStatementDependency) return resultObj;

    if (selfReferenceTable) resultObj.tableName = dependencyTable;
    else if (dependencyTable) resultObj.tableName = dependencyTable;

    if (selfReferencePath.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
      // todo - future use-cases will be added
      if (
        dependencyPath.includes(SQLElement.SELECT_CLAUSE_ELEMENT) &&
        dependencyName === selfReferenceName
      ) {
        resultObj.isDependency = true;
        resultObj.type = this.#dependencyTypes.SELECT;

        return resultObj;
      }
    }
    return resultObj;
  };

  #getColumnSourceOfTable = async (
    tableName: string,
    columnName: string,
    potentialDependencies: Column[],
    lineageId: string
  ): Promise<Column | null> => {
    const readTablesResult = await this.#readTables.execute(
      { name: tableName, lineageId },
      { organizationId: 'todo' }
    );

    if (!readTablesResult.success) throw new Error(readTablesResult.error);
    if (!readTablesResult.value)
      throw new ReferenceError(`Reading of table failed`);

    const tables = readTablesResult.value;

    if (tables.length === 0)
      throw new ReferenceError('Requested table not found');

    // todo - Assumption correct that table names have to be unique across data warehouse?
    const potentialColumnSources = potentialDependencies.filter(
      (column) => column.name === columnName && column.tableId === tables[0].id
    );

    if (!potentialColumnSources.length)
      throw new ReferenceError('Failed to identify referenced column');
    if (potentialColumnSources.length > 1)
      throw new ReferenceError(
        'The same table-column combination exist multiple times in Data Warehouse'
      );

    return potentialColumnSources[0];
  };

  #getPotentialColumnDependencies = async (
    parentTableIds: string[],
    lineageId: string,
    columnDependencyNames: string[]
  ): Promise<Column[]> => {
    // Read those columns
    const readColumnsResult = await this.#readColumns.execute(
      { tableId: parentTableIds, name: columnDependencyNames, lineageId },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of dependency columns failed`);

    if (readColumnsResult.value.length === 0)
      throw new ReferenceError(`No source columns found`);

    return readColumnsResult.value;
  };

  #buildDependencyPrototype = (
    dependencyReferences: DependencyAnalysisResult[],
    column: Column
  ): DependencyProperties => {
    const dependencyReference = dependencyReferences.filter(
      (reference) => reference.columnName === column.name
    );

    if (dependencyReference.length !== 1)
      throw new ReferenceError('Invalid column match-reference relation');

    const { type } = dependencyReference[0];
    if (!type) throw new ReferenceError('Dependency type not declared');

    return {
      type,
      columnId: column.id,
      direction: Direction.UPSTREAM,
    };
  };

  #getClarifiedMatchedColumn = async (
    lineageId: string,
    match: Match,
    columnsToClarify: Column[],
    statementReferences: StatementReference[]
  ): Promise<Column> => {
    const { columnName, tableName } = match.analysisResult;

    if (tableName) {
      const dependencySource = await this.#getColumnSourceOfTable(
        tableName,
        columnName,
        columnsToClarify,
        lineageId
      );
      if (dependencySource) return dependencySource;
    }
    if (
      match.analysisResult.path.includes(SQLElement.FROM_EXPRESSION_ELEMENT)
    ) {
      const fromTables = statementReferences.filter((element) =>
        [SQLElement.FROM_EXPRESSION_ELEMENT, SQLElement.TABLE_REFERENCE].every(
          (key) => element.path.includes(key)
        )
      );

      if (fromTables.length > 1)
        throw new ReferenceError("Multiple 'from' tables identified");
      if (!fromTables.length)
        throw new ReferenceError("'From' table not found");

      const dependencySource = await this.#getColumnSourceOfTable(
        fromTables[0].name,
        columnName,
        columnsToClarify,
        lineageId
      );
      if (dependencySource) return dependencySource;
    }

    throw new ReferenceError(`Table for column ${columnName} not found`);
  };

  #getDependencyPrototypes = async (
    selfColumnReference: StatementReference,
    statementReferences: StatementReference[],
    parentTableIds: string[],
    lineageId: string
  ): Promise<DependencyProperties[]> => {
    const dependencyReferences = statementReferences
      .map((reference) =>
        this.#analyzeStatementReference(reference, selfColumnReference)
      )
      .filter((result) => result.isDependency);

    if (!dependencyReferences.length) return [];

    const columnDependencyNames = dependencyReferences.map(
      (reference) => reference.columnName
    );

    const potentialColumnDependencies =
      await this.#getPotentialColumnDependencies(
        parentTableIds,
        lineageId,
        columnDependencyNames
      );

    const matches: Match[] = dependencyReferences.map((reference) => ({
      analysisResult: reference,
      referenceColumnRatio: potentialColumnDependencies.filter(
        (column) => column.name === reference.columnName
      ).length,
    }));

    if (matches.every((match) => match.referenceColumnRatio === 1))
      return potentialColumnDependencies.map((column) =>
        this.#buildDependencyPrototype(dependencyReferences, column)
      );

    if (matches.some((match) => match.referenceColumnRatio === 0))
      throw new ReferenceError(
        'Referenced column does not exist along data warehouse tables'
      );

    const matchesToClarify: Match[] = matches.filter(
      (match) => match.referenceColumnRatio > 1
    );
    const columnsToClarify = potentialColumnDependencies.filter(
      (column) =>
        matchesToClarify.filter(
          (match) => match.analysisResult.columnName === column.name
        ).length
    );

    const clarifiedColumns: Column[] = await Promise.all(
      matchesToClarify.map(async (match) =>
        this.#getClarifiedMatchedColumn(
          lineageId,
          match,
          columnsToClarify,
          statementReferences
        )
      )
    );

    const finalMatches = matches.filter(
      (match) => match.referenceColumnRatio === 1
    );
    const matchedColumns = potentialColumnDependencies.filter(
      (column) =>
        finalMatches.filter(
          (match) => match.analysisResult.columnName === column.name
        ).length
    );

    const dependencyPropertyObjs: DependencyProperties[] = [];

    dependencyPropertyObjs.push(
      ...matchedColumns.map((column) =>
        this.#buildDependencyPrototype(dependencyReferences, column)
      )
    );
    dependencyPropertyObjs.push(
      ...clarifiedColumns.map((column) =>
        this.#buildDependencyPrototype(dependencyReferences, column)
      )
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
        request.parentTableIds,
        request.lineageId
      );

      const column = Column.create({
        id: new ObjectId().toHexString(),
        name: request.selfReference.columnName,
        tableId: request.tableId,
        dependencyPrototypes,
        lineageId: request.lineageId,
      });

      const readColumnsResult = await this.#readColumns.execute(
        {
          name: request.selfReference.columnName,
          tableId: request.tableId,
          lineageId: request.lineageId,
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
