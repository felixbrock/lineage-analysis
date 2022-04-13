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

    const tableSelfSearchRes: string[] = [];
    const tableSelectRes: string[] = [];
    if (!this.#model) throw new ReferenceError('Model property is undefined');

    this.#model.logic.statementReferences.flat().forEach((element) => {

      if (element.path.includes(tableSelfRef)) {
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );
        tableSelfSearchRes.push(element.tableName);
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
      if (tableSelectRes.length < 1)
        throw new ReferenceError(`${tableSelfRef} or ${selectRef} not found`);
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

    const parentTableNames: string[] = [];

    statementReferences.flat().forEach((element) => {
      if (tableSelfRefs.some((ref) => element.path.includes(ref))) return;

      if (element.type === ReferenceType.TABLE) {
        if (!element.tableName)
          throw new ReferenceError(
            'tableName of TABLE references does not exist'
          );
        if (!parentTableNames.includes(element.tableName))
          parentTableNames.push(element.tableName);
      } else if (element.type === ReferenceType.COLUMN) {
        if (!element.columnName)
          throw new ReferenceError(
            'columnName of COLUMN references does not exist'
          );
        if (element.tableName && !parentTableNames.includes(element.tableName))
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
    const columnRefs = selfColumnReferences.filter((ref) => !ref.path.includes(SQLElement.SET_EXPRESSION));
    
    const uniqueSetColumnRefs = setColumnRefs.filter((value, index, self) =>
    index === self.findIndex((t) => (
    t.columnName === value.columnName && t.path === value.path && t.type === value.type))
    );

    selfColumnReferences = uniqueSetColumnRefs.concat(columnRefs); 

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
          // todo-replace
          console.log(auth);
    try {

      #getStatementRoot = (path: string): string => {
        const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
        if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
          // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
          throw new RangeError('Statement root not found for column reference');
    
        return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
      };
    
      #analyzeStatementReference = (
        potentialDependency: StatementReference,
        selfRef: StatementReference
      ): DependencyAnalysisResult => {
        const dependencyStatementRoot = this.#getStatementRoot(
          potentialDependency.path
        );
        const selfStatementRoot = this.#getStatementRoot(selfRef.path);
    
        const isStatementDependency =
          !potentialDependency.path.includes(SQLElement.INSERT_STATEMENT) &&
          !potentialDependency.path.includes(SQLElement.COLUMN_DEFINITION) &&
          dependencyStatementRoot === selfStatementRoot &&
          (potentialDependency.path.includes(SQLElement.COLUMN_REFERENCE) ||
            potentialDependency.path.includes(SQLElement.WILDCARD_IDENTIFIER));
    
        const resultObj: DependencyAnalysisResult = {
          ...potentialDependency,
          isDependency: false,
        };
    
        if (!isStatementDependency) return resultObj;
    
        if (!resultObj.tableName && selfRef.tableName)
          resultObj.tableName = selfRef.tableName;
    
        if (selfStatementRoot.includes(SQLElement.SELECT_STATEMENT)) {
          if (potentialDependency.type === ReferenceType.WILDCARD){
            resultObj.columnName = selfRef.columnName;
            resultObj.isDependency = true;
          }
          else if (
            selfRef.columnName &&
            selfRef.columnName === potentialDependency.columnName
          )
            resultObj.isDependency = true;
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
    
        if (dependencyReference.length === 0)
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
    
        if (!columnName) throw new ReferenceError('Name of column to be clarified');
    
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
          if (!fromTables[0].tableName)
            throw new ReferenceError(
              'table name of TABLE reference does not exist'
            );
    
          const dependencySource = await this.#getColumnSourceOfTable(
            fromTables[0].tableName,
            columnName,
            columnsToClarify,
            lineageId
          );
          if (dependencySource) return dependencySource;
        }
    
        throw new ReferenceError(`Table for column ${columnName} not found`);
      };



      #getDependencyPrototypes = async (
        selfRef: StatementReference,
        statementReferences: StatementReference[],
        parentTableIds: string[],
        lineageId: string
      ): Promise<DependencyProperties[]> => {
        const dependencyReferences = statementReferences
          .map((reference) => this.#analyzeStatementReference(reference, selfRef))
          .filter((result) => result.isDependency);
    
        if (!dependencyReferences.length) return [];
    
        const columnDependencyNames = dependencyReferences.map((reference) => {
          if (!reference.columnName)
            throw new ReferenceError('COLUMN reference is missing name');
          return reference.columnName;
        });
    
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
    
        // const matchesToClarify: Match[] = matches.filter(
        //   (match) => match.referenceColumnRatio > 1
        // );
        // const columnsToClarify = potentialColumnDependencies.filter(
        //   (column) =>
        //     matchesToClarify.filter(
        //       (match) => match.analysisResult.columnName === column.name
        //     ).length
        // );
    
        // const clarifiedColumns: Column[] = await Promise.all(
        //   matchesToClarify.map(async (match) =>
        //     this.#getClarifiedMatchedColumn(
        //       lineageId,
        //       match,
        //       columnsToClarify,
        //       statementReferences
        //     )
        //   )
        // );
    
        const finalMatches = matches.filter(
          (match) => match.referenceColumnRatio > 1
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
        // dependencyPropertyObjs.push(
        //   ...clarifiedColumns.map((column) =>
        //     this.#buildDependencyPrototype(dependencyReferences, column)
        //   )
        // );
    
        return dependencyPropertyObjs;
      };

      interface DependencyAnalysisResult extends StatementReference {
        isDependency: boolean;
      }
      
      interface Match {
        analysisResult: DependencyAnalysisResult;
        referenceColumnRatio: number;
      }


      const dependencyPrototypes = await this.#getDependencyPrototypes(
        request.selfRef,
        request.statementSourceReferences,
        request.parentTableIds,
        request.lineageId
      );

      if (!request.selfRef.columnName)
        throw new ReferenceError('Name of column to be created is undefined');

        export interface CreateColumnRequestDto {
          selfRef: StatementReference;
          tableId: string;
          statementSourceReferences: StatementReference[];
          parentTableIds: string[];
          lineageId: string;
        }








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
