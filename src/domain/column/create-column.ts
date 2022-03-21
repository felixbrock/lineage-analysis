import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { CreateTable } from '../table/create-table';
import { Column } from '../entities/column';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';
import { ObjectId } from 'mongodb';
import { StatementReference } from '../entities/model';
import { request } from 'express';

export interface CreateColumnRequestDto {
  tableId: string;
  reference: StatementReference;
  statementReferences: StatementReference[][];
  parentTableIds: string [];
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
  #ColumnElement = {
    NAME: 'name',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
  };

  #isStatementDependency = (key: string, columnReference: string): boolean => {
    const lastKeyStatementIndex = key.lastIndexOf(SQLElement.STATEMENT);
    if (lastKeyStatementIndex === -1 || !lastKeyStatementIndex)
      throw new RangeError('Statement not found for column reference');
  
    const keyStatement = key.slice(0, lastKeyStatementIndex + SQLElement.STATEMENT.length);

    const lastRootStatementIndex = columnReference.lastIndexOf(SQLElement.STATEMENT);
    if (lastRootStatementIndex === -1 || !lastRootStatementIndex)
      throw new RangeError('Statement not found for column root');
  
    const rootStatement = key.slice(0, lastRootStatementIndex + SQLElement.STATEMENT.length);

    return (
      !key.includes(SQLElement.INSERT_STATEMENT) &&
      !key.includes(SQLElement.COLUMN_DEFINITION) &&
      keyStatement === rootStatement &&
      key.includes(SQLElement.COLUMN_REFERENCE)
    );
  };

  #findDependencyColumnId = (
    dependency: StatementReference,
    statementReferences: StatementReference[],
    parentTableIds: string[]
  ): TableDto => {
    const columnName = dependency[1].includes('.')
      ? dependency[1].split('.').slice(-1)[0]
      : dependency[1];
    const tableName = dependency[1].includes('.')
      ? dependency[1].split('.').slice(0)[0]
      : '';

    // todo - columns where tableId and columnName
// todo - output: potential columnIds

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

  #analyzeDependency = (dependency: StatementReference, columnName: string) => {
    const key = dependency[0];
    const value = dependency[1].includes('.')
      ? dependency[1].split('.').slice(-1)[0]
      : dependency[1];

    if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT) ) {
      result[this.#ColumnElement.TABLE] = dependencyTableName;
      result[this.#ColumnElement.TARGETS] = [value];
      result[this.#ColumnElement.DEPENDENCY_TYPE] =
        this.#ColumnElement.TYPE_SELECT;
    } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
      result[this.#ColumnElement.TABLE] = dependencyTableName;
      result[this.#ColumnElement.TARGETS] = [];
      result[this.#ColumnElement.DEPENDENCY_TYPE] =
        this.#ColumnElement.TYPE_JOIN_CONDITION;
    }
    result[this.#ColumnElement.COLUMN] = value;
    return result;
  }

  // #analyzeColumnDependency = (
  //   dependency: [string, string],
  //   dependencyTableName: string,
  //   statementReferencesObj: StatementReference[]
  // ) => {
  //   const key = dependency[0];
  //   const value = dependency[1].includes('.')
  //     ? dependency[1].split('.').slice(-1)[0]
  //     : dependency[1];

  //   // if (!this.#isStatementDependency(key)) return;

  //   const result: { [key: string]: any } = {};

  //   if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
  //     result[this.#ColumnElement.TABLE] = dependencyTableName;
  //     result[this.#ColumnElement.TARGETS] = [value];
  //     result[this.#ColumnElement.DEPENDENCY_TYPE] =
  //       this.#ColumnElement.TYPE_SELECT;
  //   } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
  //     result[this.#ColumnElement.TABLE] = dependencyTableName;
  //     result[this.#ColumnElement.TARGETS] = [];
  //     result[this.#ColumnElement.DEPENDENCY_TYPE] =
  //       this.#ColumnElement.TYPE_JOIN_CONDITION;
  //   } else if (key.includes(SQLElement.ODERBY_CLAUSE)) {
  //     result[this.#ColumnElement.TABLE] = '';
  //     result[this.#ColumnElement.TARGETS] = [];
  //     result[this.#ColumnElement.DEPENDENCY_TYPE] =
  //       this.#ColumnElement.TYPE_ORDERBY_CLAUSE;
  //   }
  //   result[this.#ColumnElement.COLUMN] = value;
  //   return result;
  // };



  // #getColumn = (
  //   statementReferences: StatementReference[][],
  //   columns: string[],
  //   parents: TableDto[]
  // ): { [key: string]: string }[] => {
  //   const column: { [key: string]: string }[] = [];

    

  //   columnDependencies.map((dependency) => {
  //         const dependencyTable = this.#findDependencyTable(
  //           dependency,
  //           statement,
  //           parents
  //         );

  //         const result = this.#analyzeColumnDependency(
  //           dependency,
  //           dependencyTable.name,
  //           statement
  //         );

  //         if (!result)
  //           throw new ReferenceError(
  //             'No information for column reference found'
  //           );

  //         column.push(result);
  //       });
  //   });

  //   return column;
  // };

  // #findColumn = (name: string, statementReferences: StatementReference[][]) =>{

  // }

  constructor(createTable: CreateTable) {
    this.#createTable = createTable;
  }

  async execute(
    request: CreateColumnRequestDto,
    auth: CreateColumnAuthDto
  ): Promise<CreateColumnResponseDto> {
    try {
      
      const statementDependencies = request.statementReferences.map((statement) =>
      statement
        .filter((dependency) => this.#isStatementDependency(dependency[0], request.reference[0]))).flat();

      


      Column.create({
        id: new ObjectId().toHexString(),
        name: request.name,
        tableId: request.tableId,
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
