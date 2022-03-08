import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { buildTableDto, TableDto } from './table-dto';
// todo - Clean Code dependency violation. Fix
import fs from 'fs';
import { Table } from '../entities/table';
import { ParseSQL } from '../sql-parser-api/parse-sql';
import { SQLElement } from '../value-types/sql-element';
// todo cleancode violation
import { ObjectId } from 'mongodb';
import { CreateModel, CreateModelResponse } from './create-model';
import { Model } from '../value-types/model';
import { buildModelDto } from './model-dto';

export interface CreateTableRequestDto {
  name: string;
}

export interface CreateTableAuthDto {
  organizationId: string;
}

export type CreateTableResponseDto = Result<TableDto>;

export class CreateTable
  implements
    IUseCase<CreateTableRequestDto, CreateTableResponseDto, CreateTableAuthDto>
{
  #createModel: CreateModel;

  constructor(createModel: CreateModel) {
    this.#createModel = createModel;
  }

  #getTableName = (statementReferences: [string, string][][]): string => {
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

  #getTableColumns = (
    statementReferences: [string, string][][]
  ): string[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
    ];

    const columnSelfSearchRes: string[] = [];

    statementReferences.flat().forEach((element) => {
      if (columnSelfRefs.some((ref) => element[0].includes(ref)))
        columnSelfSearchRes.push(element[1]);
    });

    return columnSelfSearchRes;
  };

  #getParentTableNames = (
    statementReferences: [string, string][][]
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
    request: CreateTableRequestDto,
    auth: CreateTableAuthDto
  ): Promise<CreateTableResponseDto> {
    try {
      const createModelResult: CreateModelResponse =
        await this.#createModel.execute(
          { name: request.name },
          { organizationId: 'XXX' }
        );

      if (!createModelResult.success) throw new Error(createModelResult.error);
      if (!createModelResult.value)
        throw new Error(`Creation of model for table ${request.name} failed`);

      const model = createModelResult.value;

      const name = this.#getTableName(model.statementReferences);

      const parentNames = this.#getParentTableNames(
        model.statementReferences
      );
      // create Parent tables

      const columns = this.#getTableColumns(model.statementReferences);
      columns.for

      

      const table = Table.create({
        id: new ObjectId().toHexString(),
        name,
        model,
      });

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
