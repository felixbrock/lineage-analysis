import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
// todo - Clean Code dependency violation. Fix
import { Table } from '../entities/table';
import { SQLElement } from '../value-types/sql-element';
// todo cleancode violation
import { ObjectId } from 'mongodb';
import { StatementReference } from '../entities/model';
import { CreateColumn } from '../column/create-column';
import { CreateTable } from '../table/create-table';
import { LineageDto } from './lineage-dto';
import { CreateModel, CreateModelResponse } from '../table/create-model';

export interface CreateLineageRequestDto {
  tableId?: string;
  columnId?: string;
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
  #createModel: CreateModel;
  #createTable: CreateTable;
  #createColumn: CreateColumn;

  constructor(
    createModel: CreateModel,
    createTable: CreateTable,
    createColumn: CreateColumn
  ) {
    this.#createModel = createModel;
    this.#createTable = createTable;
    this.#createColumn = createColumn;
  }

  #getTableName = (statementReferences: StatementReference[][]): string => {
    const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;

    const tableSelfSearchRes: string[] = [];
    statementReferences.flat().forEach((element) => {
      if (element.includes(tableSelfRef)) tableSelfSearchRes.push(element[1]);
    });

    if (tableSelfSearchRes.length > 1)
      throw new ReferenceError(`Multiple instances of ${tableSelfRef} found`);
    if (tableSelfSearchRes.length < 1)
      throw new ReferenceError(`${tableSelfRef} not found`);

    return tableSelfSearchRes[0];
  };

  #getTableColumnNames = (
    statementReferences: StatementReference[][]
  ): string[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
    ];

    const tableColumnNames: string[] = [];

    statementReferences.flat().forEach((element) => {
      if (columnSelfRefs.some((ref) => element[0].includes(ref)))
        tableColumnNames.push(element[1]);
    });

    return tableColumnNames;
  };

  #getParentTableNames = (
    statementReferences: StatementReference[][]
  ): string[] => {
    const tableRef = `${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
    const tableSelfRefs = [
      `${SQLElement.CREATE_TABLE_STATEMENT}.${tableRef}`,
      `${SQLElement.INSERT_STATEMENT}.${tableRef}`,
    ];

    const parentTableNames: string[] = [];
    statementReferences.flat().forEach((element) => {
      if (
        !tableSelfRefs.some((ref) => element.includes(ref)) &&
        element[0].includes(SQLElement.TABLE_REFERENCE)
      )
        parentTableNames.push(element[1]);
    });

    return parentTableNames;
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    try {
      if (!request.tableId) throw new TypeError('No tableId provided');

      const createModelResult: CreateModelResponse =
        await this.#createModel.execute(
          { id: request.tableId },
          { organizationId: 'todo' }
        );

      if (!createModelResult.success) throw new Error(createModelResult.error);
      if (!createModelResult.value)
        throw new Error(
          `Creation of model for table ${request.tableId} failed`
        );

      const model = createModelResult.value;

      const name = this.#getTableName(model.statementReferences);

      const table = await this.#createTable.execute(
        {
          name,
          modelId: model.id,
        },
        { organizationId: 'todo' }
      );

      const parentNames = this.#getParentTableNames(model.statementReferences);

      // todo - can this be async?
      const parentTables = await Promise.all(
        parentNames.map(async (name) => {
          // const createModelResult = await this.#createModel.execute(
          //   { id: name },
          //   { organizationId: 'todo' }
          // );

          // if (!createModelResult.success)
          //   throw new Error(createModelResult.error);
          // if (!createModelResult.value)
          //   throw new Error(
          //     `Creation of model for table ${request.tableId} failed`
          //   );

          // const table = await this.#createTable.execute(
          //   { name, modelId: createModelResult.value.id },
          //   { organizationId: 'todo' }
          // );

          // todo - Last point of changek
          await this.execute({tableId: name}, {organizationId: 'todo'})

        })
      );

      const columnNames = this.#getTableColumnNames(model.statementReferences);
      const columns = await Promise.all(columnNames.map(async (name) => await this.#createColumn.execute({ name, statementReferences, tableId }, {organizationId: 'todo'}));


      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      // todo return table
      return Result.ok(buildTableDto(table));
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
