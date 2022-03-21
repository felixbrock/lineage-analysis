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
import { buildLineageDto, LineageDto } from './lineage-dto';
import { CreateModel, CreateModelResponse } from '../table/create-model';
import { Lineage } from '../value-types/transient-types/lineage';
import { Column } from '../entities/column';

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

  #getTableColumnReferences = (
    statementReferences: StatementReference[][]
  ): StatementReference[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
    ];

    const tableColumnNames: StatementReference[] = [];

    statementReferences.flat().forEach((element) => {
      if (columnSelfRefs.some((ref) => element[0].includes(ref)))
        tableColumnNames.push(element);
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

      const createTableResult = await this.#createTable.execute(
        {
          name,
          modelId: model.id,
        },
        { organizationId: 'todo' }
      );

      if (!createTableResult.success) throw new Error(createTableResult.error);
      if (!createTableResult.value)
        throw new Error(`Creation of table ${name} failed`);

      const table = createTableResult.value;

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
          await this.execute({ tableId: name }, { organizationId: 'todo' });
        })
      );

      const columnNames = this.#getTableColumnReferences(model.statementReferences);
      const createColumnResults = await Promise.all(
        columnNames.map(
          async (reference) =>
            await this.#createColumn.execute(
              {
                reference,
                statementReferences: model.statementReferences,
                tableId: table.id,
                parentTableNames: parentNames
              },
              { organizationId: 'todo' }
            )
        )
      );

      if (!createColumnResults.some((result) => !result.success || !result.value))
        throw new Error(createTableResult.error);

      const columns = createColumnResults
        .map((result) => result.value)
        .filter(
          (value): value is Column =>
            value !== undefined && value.tableId === table.id
        );

      const lineage = Lineage.create({
        table,
        columns,
      });

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      return Result.ok(buildLineageDto(lineage));
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
