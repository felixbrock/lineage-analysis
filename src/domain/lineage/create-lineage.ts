import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
// todo - Clean Code dependency violation. Fix
import fs from 'fs';
import { LineageDto } from './lineage-dto';
import { CreateTable, CreateTableResponseDto } from '../table/create-table';
import { Lineage } from '../entities/lineage';
import { Table } from '../entities/table';
import { TableDto } from '../table/table-dto';
import { SQLElement } from '../value-types/sql-element';

export interface CreateLineageRequestDto {
  id: string;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<LineageDto>;

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto
    >
{
  #createTable: CreateTable;

  #LineageElement = {
    TABLE_SELF: 'table_self',
    TABLE: 'table',

    COLUMN: 'column',

    DEPENDENCY_TYPE: 'dependency_type',

    TYPE_SELECT: 'select',
    TYPE_JOIN_CONDITION: 'join_condition',
    TYPE_ORDERBY_CLAUSE: 'oderby_clause',
  };

  #isColumnDependency = (key: string): boolean =>
    key.includes(SQLElement.COLUMN_REFERENCE);

  #analyzeColumnDependency = (
    key: string,
    value: string,
    statementDependencyObj: [string, string][]
  ) => {
    if (!this.#isColumnDependency(key)) return;

    const result: { [key: string]: string } = {};
    let tableRef = '';
    let valueRef = value;

    if (value.includes('.')) {
      const valuePathElements = value.split('.');
      tableRef = valuePathElements[0];
      valueRef = valuePathElements[1];
    }

    if (key.includes(SQLElement.SELECT_CLAUSE_ELEMENT)) {
      if (!tableRef) {
        const tableRefs = statementDependencyObj.filter((element) =>
          [
            SQLElement.FROM_EXPRESSION_ELEMENT,
            SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring))
        );
        tableRef = tableRefs[0][1];
      }

      if (!tableRef)
        throw ReferenceError(`No table for SELECT statement found`);

      result[this.#LineageElement.TABLE] = tableRef;
      result[this.#LineageElement.DEPENDENCY_TYPE] =
        this.#LineageElement.TYPE_SELECT;
    } else if (key.includes(SQLElement.JOIN_ON_CONDITION)) {
      if (!tableRef) {
        Object.entries(statementDependencyObj).forEach((element) => {
          const isJoinTable = [
            SQLElement.JOIN_CLAUSE,
            SQLElement.FROM_EXPRESSION_ELEMENT,
            SQLElement.TABLE_REFERENCE,
          ].every((substring) => element[0].includes(substring));
          if (isJoinTable) {
            tableRef = value;
            return;
          }
        });

        if (!tableRef)
          throw ReferenceError(`No table for JOIN statement found`);
      }

      result[this.#LineageElement.TABLE] = tableRef;
      result[this.#LineageElement.DEPENDENCY_TYPE] =
        this.#LineageElement.TYPE_JOIN_CONDITION;
    } else if (key.includes(SQLElement.ODERBY_CLAUSE))
      result[this.#LineageElement.DEPENDENCY_TYPE] =
        this.#LineageElement.TYPE_ORDERBY_CLAUSE;

    result[this.#LineageElement.COLUMN] = valueRef;
    return result;
  };

  #getLineage = (statementDependencies: [string, string][][]) : { [key: string]: string }[] => {
    const lineage: { [key: string]: string }[] = []

    statementDependencies.forEach((element) => {
      element
        .filter((dependency) => this.#isColumnDependency(dependency[0]))
        .forEach((dependency) => {
          const result = this.#analyzeColumnDependency(
            dependency[0],
            dependency[1],
            element
          );

          if (!result)
            throw new ReferenceError(
              'No information for column reference found'
            );

          lineage.push(result);
        });
    });

    return lineage
  };

  constructor(createTable: CreateTable) {
    this.#createTable = createTable;
  }

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    try {
      const createTableResult: CreateTableResponseDto =
        await this.#createTable.execute(
          { id: request.id },
          { organizationId: 'todo' }
        );

      if (!createTableResult.success) throw new Error(createTableResult.error);
      if (!createTableResult.value)
        throw new Error(`Creation of table ${request.id} failed`);

      const createTableResults = await Promise.all(
        createTableResult.value.parentNames.map(
          async (element) =>
            await this.#createTable.execute(
              { id: 'table1' },
              { organizationId: 'todo' }
            )
        )
      );

      const parents: TableDto[] = [];
      createTableResults.forEach((result) => {
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new Error(`Creation of parent table failed`);

        parents.push(result.value);

        // todo is async ok here?
        this.execute(
          { id: result.value.name },
          { organizationId: 'todo' }
        );
      });

      const lineage = this.#getLineage(createTableResult.value.statementDependencies)

      const lineageObj = Lineage.create({lineage});

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok({
        lineage: lineageObj.lineage,
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  // #runChildProcess = () => {
  //   const childProcess = spawn('python', [
  //     '../value-types/sql-parser.py',
  //     // id,
  //     'C://Users/felix-pc/Desktop/Test/table2.sql',
  //     'snowflake',
  //   ]);

  //       const processResults: any[] = [];

  //       childProcess.stdout.on('data', (data) =>
  //         processResults.push(data.toString())
  //       );

  //       childProcess.on('close', (code) => {
  // });}
}
