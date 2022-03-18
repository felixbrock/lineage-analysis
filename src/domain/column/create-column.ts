import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
// todo - Clean Code dependency violation. Fix
import { ColumnDto } from './column-dto';
import { CreateTable, CreateTableResponseDto } from '../table/create-table';
import { Column } from '../entities/column';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';
import { ObjectId } from 'mongodb';
import { StatementReference } from '../entities/model';

export interface CreateColumnRequestDto {
  tableId: string;
  name: string;
  statementReferences: StatementReference[][];
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
  #createTable: CreateTable;

  #ColumnElement = {
    NAME: 'name',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
    TYPE_ORDERBY_CLAUSE: 'oderby_clause',
  };

  #isColumnDependency = (key: string): boolean =>
    !key.includes(SQLElement.INSERT_STATEMENT) &&
    key.includes(SQLElement.COLUMN_REFERENCE);

  #analyzeColumnDependency = (
    dependency: [string, string],
    dependencyTableName: string,
    statementReferencesObj: StatementReference[]
  ) => {
    const key = dependency[0];
    const value = dependency[1].includes('.')
      ? dependency[1].split('.').slice(-1)[0]
      : dependency[1];

    if (!this.#isColumnDependency(key)) return;

    const result: { [key: string]: any } = {};

    if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
      result[this.#ColumnElement.TABLE] = dependencyTableName;
      result[this.#ColumnElement.TARGETS] = [value];
      result[this.#ColumnElement.DEPENDENCY_TYPE] =
        this.#ColumnElement.TYPE_SELECT;
    } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
      result[this.#ColumnElement.TABLE] = dependencyTableName;
      result[this.#ColumnElement.TARGETS] = [];
      result[this.#ColumnElement.DEPENDENCY_TYPE] =
        this.#ColumnElement.TYPE_JOIN_CONDITION;
    } else if (key.includes(SQLElement.ODERBY_CLAUSE)) {
      result[this.#ColumnElement.TABLE] = '';
      result[this.#ColumnElement.TARGETS] = [];
      result[this.#ColumnElement.DEPENDENCY_TYPE] =
        this.#ColumnElement.TYPE_ORDERBY_CLAUSE;
    }
    result[this.#ColumnElement.COLUMN] = value;
    return result;
  };

  #findDependencyTable = (
    dependency: [string, string],
    statementReferences: StatementReference[],
    parents: TableDto[]
  ): TableDto => {
    const columnName = dependency[1].includes('.')
      ? dependency[1].split('.').slice(-1)[0]
      : dependency[1];
    const tableName = dependency[1].includes('.')
      ? dependency[1].split('.').slice(0)[0]
      : '';

    const potentialDependencyTables = parents.filter((table) =>
      table.columns.includes(columnName)
    );

    if (potentialDependencyTables.length === 1)
      return potentialDependencyTables[0];
    else if (potentialDependencyTables.length === 0)
      throw new ReferenceError(`Table for column ${columnName} not found`);

    if (tableName) {
      const dependencyTableMatches = potentialDependencyTables.filter(
        (table) => table.name === tableName
      );
      if (dependencyTableMatches.length === 1) return dependencyTableMatches[0];
      throw new ReferenceError('Multiple parents with the same name exist');
    }

    if (dependency[0].includes(SQLElement.FROM_EXPRESSION_ELEMENT)) {
      const potentialMatches = statementReferences.filter((element) =>
        [SQLElement.FROM_EXPRESSION_ELEMENT, SQLElement.TABLE_REFERENCE].every(
          (key) => element[0].includes(key)
        )
      );

      const dependencyTableMatches = potentialDependencyTables.filter(
        (element) =>
          potentialMatches.map((match) => match[1]).includes(element.name)
      );
      if (dependencyTableMatches.length === 1) return dependencyTableMatches[0];
      throw new ReferenceError('Multiple parents with the same name exist');
    }

    throw new ReferenceError(`Table for column ${columnName} not found`);
  };

  #getColumn = (
    statementReferences: StatementReference[][],
    columns: string[],
    parents: TableDto[]
  ): { [key: string]: string }[] => {
    const column: { [key: string]: string }[] = [];

    statementReferences.forEach((statement) => {
      statement
        .filter((dependency) => this.#isColumnDependency(dependency[0]))
        .forEach((dependency) => {
          const dependencyTable = this.#findDependencyTable(
            dependency,
            statement,
            parents
          );

          const result = this.#analyzeColumnDependency(
            dependency,
            dependencyTable.name,
            statement
          );

          if (!result)
            throw new ReferenceError(
              'No information for column reference found'
            );

          column.push(result);
        });
    });

    return column;
  };

  constructor(createTable: CreateTable) {
    this.#createTable = createTable;
  }

  async execute(
    request: CreateColumnRequestDto,
    auth: CreateColumnAuthDto
  ): Promise<CreateColumnResponseDto> {
    try {
      // const createTableResult: CreateTableResponseDto =
      //   await this.#createTable.execute(
      //     { name: request.name },
      //     { organizationId: 'todo' }
      //   );

      // if (!createTableResult.success) throw new Error(createTableResult.error);
      // if (!createTableResult.value)
      //   throw new Error(`Creation of table ${request.name} failed`);

      // const createTableResults = await Promise.all(
      //   createTableResult.value.parentNames.map(
      //     async (element) =>
      //       await this.#createTable.execute(
      //         { name: element },
      //         { organizationId: 'todo' }
      //       )
      //   )
      // );

      // const parents: TableDto[] = [];
      // createTableResults.forEach((result) => {
      //   if (!result.success) throw new Error(result.error);
      //   if (!result.value) throw new Error(`Creation of parent table failed`);

      //   parents.push(result.value);

      //   // todo is async ok here?
      //   this.execute({ name: result.value.name }, { organizationId: 'todo' });
      // });

      const column = this.#getColumn(
        createTableResult.value.statementReferences,
        createTableResult.value.columns,
        parents
      );

      const columnObj = Column.create({
        id: new ObjectId().toHexString(),
        column,
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok({
        column: columnObj.column,
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
