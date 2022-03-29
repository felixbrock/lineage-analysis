// todo - Clean Architecture dependency violation. Fix
import fs from 'fs';
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import { CreateTable } from '../table/create-table';
import { buildLineageDto, LineageDto } from './lineage-dto';
import { CreateModel, CreateModelResponse } from '../model/create-model';
import { ReadTables } from '../table/read-tables';
import { StatementReference } from '../value-types/logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import LineageRepo from '../../infrastructure/persistence/lineage-repo';
import { ReadColumns } from '../column/read-columns';

export interface CreateLineageRequestDto {
  tableId: string;
  lineageId?: string;
  lineageCreatedAt?: number;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<LineageDto>;

interface TableColumnReference {
  selfColumnReference: StatementReference;
  statementIndex: number;
}

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

  #readTables: ReadTables;

  #readColumns: ReadColumns;

  #parseSQL: ParseSQL;

  #lineageRepo: LineageRepo;

  constructor(
    createModel: CreateModel,
    createTable: CreateTable,
    createColumn: CreateColumn,
    readTables: ReadTables,
    readColumns: ReadColumns,
    parseSQL: ParseSQL,
    lineageRepo: LineageRepo
  ) {
    this.#createModel = createModel;
    this.#createTable = createTable;
    this.#createColumn = createColumn;
    this.#readTables = readTables;
    this.#readColumns = readColumns;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
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
  ): TableColumnReference[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
    ];

    let statementIndex = 0;
    const selfColumnRefs: TableColumnReference[] = [];
    statementReferences.forEach((statement) => {
      statement.forEach((element) => {
        if (columnSelfRefs.some((ref) => element[0].includes(ref)))
          selfColumnRefs.push({ selfColumnReference: element, statementIndex });
      });

      statementIndex += 1;
    });

    return selfColumnRefs;
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
      if (tableSelfRefs.some((ref) => element.includes(ref))) return;

      if (element[0].includes(SQLElement.TABLE_REFERENCE)) {
        if (!parentTableNames.includes(element[1]))
          parentTableNames.push(element[1]);
      } else if (
        element[0].includes(SQLElement.COLUMN_REFERENCE) &&
        element[1].includes('.')
      ) {
        const tableName = element[1].split('.').slice(0)[0];
        if (!parentTableNames.includes(tableName))
          parentTableNames.push(tableName);
      }
    });

    return parentTableNames;
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    try {
      // todo-replace
      console.log(auth);

      if (!request.tableId) throw new TypeError('No tableId provided');

      const lineage = request.lineageId && request.lineageCreatedAt
        ? Lineage.create({
            id: request.lineageId,
            createdAt: request.lineageCreatedAt,
          })
        : Lineage.create({ id: new ObjectId().toHexString() });

      if (!(request.lineageId && request.lineageCreatedAt)) await this.#lineageRepo.insertOne(lineage);

      const location = `C://Users/felix-pc/Desktop/Test/${request.tableId}.sql`;

      const data = fs.readFileSync(location, 'utf-8');

      const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute(
        { dialect: 'snowflake', sql: data },
        { jwt: 'todo' }
      );

      if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
      if (!parseSQLResult.value)
        throw new SyntaxError(`Parsing of SQL logic failed`);

      const newParsedLogic = JSON.stringify(parseSQLResult.value);

      // const readModelsResult = await this.#readModels.execute(
      //   {
      //     location,
      //   },
      //   { organizationId: auth.organizationId }
      // );

      // if (!readModelsResult.success) throw new Error(readModelsResult.error);
      // if (!readModelsResult.value) throw new Error('Reading models failed');

      // let model: Model;
      // if (readModelsResult.value.length) {
      //   const currentModel = readModelsResult.value[0];

      //   if (newParsedLogic !== currentModel.logic.parsedLogic) {
      //     const updateModelResult = await this.#updateModel.execute(
      //       { id: readModelsResult.value[0].id, parsedLogic: newParsedLogic },
      //       { organizationId: 'todo' }
      //     );

      //     if (!updateModelResult.success)
      //       throw new Error(readModelsResult.error);
      //     if (!updateModelResult.value)
      //       throw new Error('Update of model failed');

      //     model = updateModelResult.value;
      //   } else model = currentModel;
      // } else {
      const createModelResult: CreateModelResponse =
        await this.#createModel.execute(
          {
            id: request.tableId,
            location,
            parsedLogic: newParsedLogic,
            lineageId: lineage.id,
          },
          { organizationId: 'todo' }
        );

      if (!createModelResult.success) throw new Error(createModelResult.error);
      if (!createModelResult.value) throw new Error('Creation of model failed');

      const model = createModelResult.value;
      // }

      const name = this.#getTableName(model.logic.statementReferences);

      // todo-Update logic only gets relevant once we provide real-time by checking dbt/Snowflake changelogs

      // const readTablesResult = await this.#readTables.execute(
      //   { modelId: model.id },
      //   { organizationId: 'todo' }
      // );

      // if (!readTablesResult.success) throw new Error(readModelsResult.error);
      // if (!readTablesResult.value) throw new Error('Reading tables failed');

      const createTableResult = await this.#createTable.execute(
        {
          name,
          modelId: model.id,
          lineageId: lineage.id,
        },
        { organizationId: 'todo' }
      );

      if (!createTableResult.success) throw new Error(createTableResult.error);
      if (!createTableResult.value)
        throw new SyntaxError(`Creation of table ${name} failed`);

      const table = createTableResult.value;

      const parentNames = this.#getParentTableNames(
        model.logic.statementReferences
      );

      // todo - can this be async?
      // todo - lineage nowhere stored and not returned for display. But in the end it is only columns and table object
      await Promise.all(
        parentNames.map(async (element) =>
          this.execute(
            {
              tableId: element,
              lineageId: lineage.id,
              lineageCreatedAt: lineage.createdAt,
            },
            { organizationId: 'todo' }
          )
        )
      );

      const parentTableIds: string[] = [];
      if (parentNames.length) {
        const readParentsResult = await this.#readTables.execute(
          { name: parentNames, lineageId: lineage.id },
          { organizationId: 'todo' }
        );

        if (!readParentsResult.success)
          throw new Error(readParentsResult.error);
        if (!readParentsResult.value)
          throw new SyntaxError(`Creation of table ${name} failed`);

        const parentTables = readParentsResult.value;

        const tableMatches = parentNames.map(
          (nameElement) =>
            parentTables.filter(
              (tableElement) => tableElement.name === nameElement
            ).length
        );

        if (tableMatches.some((matches) => matches > 1))
          throw new ReferenceError('Multiple tables for parent name found');
        if (tableMatches.some((matches) => matches === 0))
          throw new ReferenceError('No table for parent name found');

        const ids = parentTables.map((tableElement) => tableElement.id);
        parentTableIds.push(...ids);
      }

      const tableColumnReferences = this.#getTableColumnReferences(
        model.logic.statementReferences
      );



      const createColumnResults = await Promise.all(
        tableColumnReferences.map(async (reference) =>{
        if(reference.selfColumnReference[0].includes(SQLElement.WILDCARD_IDENTIFIER)){
          if(parentTableIds.length !== 1) throw new ReferenceError('Wildcard - parent table mismatch');
          const readColumnsResult = await this.#readColumns.execute({tableId: parentTableIds[0], lineageId: lineage.id}, {organizationId: 'todo'});

          if (!readColumnsResult.success)
          throw new Error(readColumnsResult.error);
        if (!readColumnsResult.value)
          throw new SyntaxError(`Creation of table ${name} failed`);

          return Promise.all(readColumnsResult.value.map(column => this.#createColumn.execute(
            {
              selfReference: reference.selfColumnReference,
              statementSourceReferences:
                model.logic.statementReferences[reference.statementIndex],
              tableId: table.id,
              parentTableIds,
              lineageId: lineage.id,
            },
            { organizationId: 'todo' }
          )));

        }
        
        return this.#createColumn.execute(
          {
            selfReference: reference.selfColumnReference,
            statementSourceReferences:
              model.logic.statementReferences[reference.statementIndex],
            tableId: table.id,
            parentTableIds,
            lineageId: lineage.id,
          },
          { organizationId: 'todo' }
        );
        }
        )
      );

      createColumnResults.forEach((result) => {
        if (!result.success) throw new Error(result.error);
        if (!result.value) throw new SyntaxError(`Creation of column failed`);
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
