// todo - Clean Architecture dependency violation. Fix
import fs from 'fs';
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn, CreateColumnResponseDto } from '../column/create-column';
import { CreateTable } from '../table/create-table';
import { buildLineageDto, LineageDto } from './lineage-dto';
import { CreateModel, CreateModelResponse } from '../model/create-model';
import { ReadTables } from '../table/read-tables';
import { ReferenceType, StatementReference } from '../value-types/logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import LineageRepo from '../../infrastructure/persistence/lineage-repo';
import { ReadColumns } from '../column/read-columns';
import { Model } from '../entities/model';
import { Table } from '../entities/table';
import { Column } from '../entities/column';

export interface CreateLineageRequestDto {
  tableId: string;
  lineageId?: string;
  lineageCreatedAt?: number;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<LineageDto>;

interface ContextualStatementReference extends StatementReference {
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
  readonly #createModel: CreateModel;

  readonly #createTable: CreateTable;

  readonly #createColumn: CreateColumn;

  readonly #readTables: ReadTables;

  readonly #readColumns: ReadColumns;

  readonly #parseSQL: ParseSQL;

  readonly #lineageRepo: LineageRepo;

  #lineage?: Lineage;

  #model?: Model;

  #table?: Table;

  #columns: Column[] = [];

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

  #getTableName = (): string => {
    const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
    const selectRef = `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
    const withRef = `${SQLElement.COMMON_TABLE_EXPRESSION}`;
    
    const tableSelfSearchRes: string[] = [];
    const tableSelectRes: string[] = [];
    const withTableRes: string[] = [];
    if (!this.#model) throw new ReferenceError('Model property is undefined');

    this.#model.logic.statementReferences.flat().forEach((element) => {

      if (element.path.includes(tableSelfRef)) {
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );
        tableSelfSearchRes.push(element.tableName);
      }

      else if(element.path.includes(selectRef) && element.path.includes(withRef)){
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );
          withTableRes.push(`${element.tableName}`);
      }

      else if(element.path.includes(selectRef)){
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );
          tableSelectRes.push(`${element.tableName}_view`);
          
      }
    });

    if (tableSelfSearchRes.length > 1)
      throw new ReferenceError(`Multiple instances of ${tableSelfRef} found`);
    if (tableSelfSearchRes.length < 1){
      if (tableSelectRes.length < 1){
        if (withTableRes.length < 1)
          throw new ReferenceError(`${tableSelfRef} or ${selectRef} or ${withRef} not found`);
        return withTableRes[0];  
      }
      if (tableSelectRes.length > 1)
          throw new ReferenceError(`Multiple instances of ${selectRef} found`);
      return tableSelectRes[0];
    }
    return tableSelfSearchRes[0];
  };

  #getSelfColumnDefinitions = (
    statementReferences: StatementReference[][]
  ): ContextualStatementReference[] => {
    const columnSelfRefs = [
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
      `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
    ];

    let statementIndex = 0;
    const selfColumnRefs: ContextualStatementReference[] = [];
    statementReferences.forEach((statement) => {
      statement.forEach((element) => {
        if (columnSelfRefs.some((ref) => element.path.includes(ref))) {
          const selfColumnRef: ContextualStatementReference = {
            ...element,
            statementIndex,
          };
          selfColumnRefs.push(selfColumnRef);
        }
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
    const withRef = `${SQLElement.COMMON_TABLE_EXPRESSION}`;
    const withComp = `${SQLElement.WITH_COMPOUND_STATEMENT}`;
    const identifier = `${SQLElement.IDENTIFIER}`;

    const parentTableNames: string[] = [];

    statementReferences.flat().forEach((element) => {
      if (tableSelfRefs.some((ref) => element.path.includes(ref))) return;

      const withStatement = element.path.includes(withComp);

      if (element.type === ReferenceType.TABLE) {
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );

        if (withStatement){
          if(element.path.includes(withRef)){
            if (!parentTableNames.includes(element.tableName))
              parentTableNames.push(element.tableName);
            }
        }else if(!parentTableNames.includes(element.tableName))
            parentTableNames.push(element.tableName);

      } else if (element.type === ReferenceType.COLUMN) {
        if (!element.columnName)
          throw new ReferenceError(
            'columnName of COLUMN references does not exist'
          );

        if (withStatement){
          if(!element.path.includes(`${withRef}.${identifier}`)){
            if (element.tableName && !parentTableNames.includes(element.tableName))
              parentTableNames.push(element.tableName);
          }
        }else if (element.tableName && !parentTableNames.includes(element.tableName))
            parentTableNames.push(element.tableName);
      }
    });

    return parentTableNames;
  };

  #setLineage = async (
    lineageId?: string,
    lineageCreatedAt?: number
  ): Promise<void> => {
    this.#lineage =
      lineageId && lineageCreatedAt
        ? Lineage.create({
            id: lineageId,
            createdAt: lineageCreatedAt,
          })
        : Lineage.create({ id: new ObjectId().toHexString() });

    if (!(lineageId && lineageCreatedAt))
      await this.#lineageRepo.insertOne(this.#lineage);
  };

  #parseLogic = async (location: string): Promise<string> => {
    const data = fs.readFileSync(location, 'utf-8');

    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute(
      { dialect: 'snowflake', sql: data },
      { jwt: 'todo' }
    );

    if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
    if (!parseSQLResult.value)
      throw new SyntaxError(`Parsing of SQL logic failed`);

    return JSON.stringify(parseSQLResult.value);
  };

  #setModel = async (tableId: string): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');

    // const location = `C://Users/felix-pc/Desktop/Test/${tableId}.sql`;
    const location = `C://Users/nasir/OneDrive/Desktop/sql_files/${tableId}.sql`;

    const parsedLogic = await this.#parseLogic(location);

    const createModelResult: CreateModelResponse =
      await this.#createModel.execute(
        {
          id: tableId,
          location,
          parsedLogic,
          lineageId: this.#lineage.id,
        },
        { organizationId: 'todo' }
      );

    if (!createModelResult.success) throw new Error(createModelResult.error);
    if (!createModelResult.value) throw new Error('Creation of model failed');

    this.#model = createModelResult.value;
  };

  #setTable = async (): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    if (!this.#model) throw new ReferenceError('Model property is undefined');

    const name = this.#getTableName();
   
    const createTableResult = await this.#createTable.execute(
        {
          name,
          modelId: this.#model.id,
          lineageId: this.#lineage.id,
        },
        { organizationId: 'todo' }
      );

      if (!createTableResult.success) throw new Error(createTableResult.error);
      if (!createTableResult.value)
        throw new SyntaxError(`Creation of table ${name} failed`);
      this.#table = createTableResult.value;

  };

  #createParents = async (parentNames: string[]): Promise<void> => {
    // todo - can this be async?
    // todo - lineage nowhere stored and not returned for display. But in the end it is only columns and table object
    await Promise.all(
      parentNames.map(async (element) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');

        // todo - Check if best practice for ioc registry / awilix
        return new CreateLineage(
          this.#createModel,
          this.#createTable,
          this.#createColumn,
          this.#readTables,
          this.#readColumns,
          this.#parseSQL,
          this.#lineageRepo
        ).execute(
          {
            tableId: element,
            lineageId: this.#lineage.id,
            lineageCreatedAt: this.#lineage.createdAt,
          },
          { organizationId: 'todo' }
        );
      })
    );
  };

  #getParentTables = async (parentNames: string[]): Promise<Table[]> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');

    const readParentsResult = await this.#readTables.execute(
      { name: parentNames, lineageId: this.#lineage.id },
      { organizationId: 'todo' }
    );

    if (!readParentsResult.success) throw new Error(readParentsResult.error);
    if (!readParentsResult.value)
      throw new SyntaxError(`Reading parent tables failed`);

    return readParentsResult.value;
  };

  #createSelfColumns = async (
    reference: ContextualStatementReference,
    parentTableIds: string[]
  ): Promise<CreateColumnResponseDto | CreateColumnResponseDto[]> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    if (!this.#model) throw new ReferenceError('Model property is undefined');
    if (!this.#table) throw new ReferenceError('Table property is undefined');

    if (!reference.path.includes(SQLElement.WILDCARD_IDENTIFIER))
      return this.#createColumn.execute(
        {
          selfRef: {
            columnName: reference.columnName,
            tableName: reference.tableName,
            path: reference.path,
            type: reference.type,
          },
          statementSourceReferences:
            this.#model.logic.statementReferences[reference.statementIndex],
          tableId: this.#table.id,
          parentTableIds,
          lineageId: this.#lineage.id,
        },
        { organizationId: 'todo' }
      );

    if (parentTableIds.length !== 1)
      throw new ReferenceError('Wildcard - parent-table mismatch');
    const readColumnsResult = await this.#readColumns.execute(
      { tableId: parentTableIds[0], lineageId: this.#lineage.id },
      { organizationId: 'todo' }
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new SyntaxError(`Readinng of parent-table columns failed`);

    const createColumnResults = await Promise.all(
      readColumnsResult.value.map((column) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');
        if (!this.#model)
          throw new ReferenceError('Model property is undefined');
        if (!this.#table)
          throw new ReferenceError('Table property is undefined');

        return this.#createColumn.execute(
          {
            selfRef: {
              columnName: column.name,
              tableName: reference.tableName,
              path: reference.path,
              type: ReferenceType.COLUMN,
            },
            statementSourceReferences:
              this.#model.logic.statementReferences[reference.statementIndex],
            tableId: this.#table.id,
            parentTableIds,
            lineageId: this.#lineage.id,
          },
          { organizationId: 'todo' }
        );
      })
    );

    return createColumnResults;
  };

  #setColumns = async (parentTableIds: string[]): Promise<void> => {
    if (!this.#model) throw new ReferenceError('Model property is undefined');

    let selfColumnReferences = this.#getSelfColumnDefinitions(
      this.#model.logic.statementReferences
    );

    const setColumnRefs = selfColumnReferences.filter((ref) => ref.path.includes(SQLElement.SET_EXPRESSION));
    let columnRefs = selfColumnReferences.filter((ref) => !ref.path.includes(SQLElement.SET_EXPRESSION));
    
    const uniqueSetColumnRefs = setColumnRefs.filter((value, index, self) =>
    index === self.findIndex((t) => (
    t.columnName === value.columnName && t.path === value.path && t.type === value.type))
    );

    selfColumnReferences = uniqueSetColumnRefs.concat(columnRefs);
    
    const withColumnRefs = selfColumnReferences.filter((ref) => ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT));
    columnRefs = selfColumnReferences.filter((ref) => !ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT));
    const innerWithColumnRefs = withColumnRefs.filter((ref) => !ref.path.includes(SQLElement.COMMON_TABLE_EXPRESSION));
    
    selfColumnReferences = innerWithColumnRefs.concat(columnRefs);

    const createColumnResults = (
      await Promise.all(
        selfColumnReferences.map(async (reference) =>
          this.#createSelfColumns(reference, parentTableIds)
        )
      )
    ).flat();

    createColumnResults.forEach((result) => {
      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new SyntaxError(`Creation of column failed`);

      this.#columns.push(result.value);
    });
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    try {
      // todo-replace
      console.log(auth);

      if (!request.tableId) throw new TypeError('No tableId provided');

      await this.#setLineage(request.lineageId, request.lineageCreatedAt);

      await this.#setModel(request.tableId);

      // todo-Update logic only gets relevant once we provide real-time by checking dbt/Snowflake changelogs

      // const readTablesResult = await this.#readTables.execute(
      //   { modelId: model.id },
      //   { organizationId: 'todo' }
      // );

      // if (!readTablesResult.success) throw new Error(readModelsResult.error);
      // if (!readTablesResult.value) throw new Error('Reading tables failed');

      await this.#setTable();

      if (!this.#model) throw new ReferenceError('Model property is undefined');

      const parentNames = this.#getParentTableNames(
        this.#model.logic.statementReferences
      );

      const parentTableIds: string[] = [];
      if (parentNames.length) {
        await this.#createParents(parentNames);

        const parentTables = await this.#getParentTables(parentNames);

        const ids = parentTables.map((tableElement) => tableElement.id);
        parentTableIds.push(...ids);
      }

      await this.#setColumns(parentTableIds);

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      // todo - how to avoid checking if property exists. A sub-method created the property
      if (!this.#lineage)
        throw new ReferenceError('Model property is undefined');

      return Result.ok(buildLineageDto(this.#lineage));
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
