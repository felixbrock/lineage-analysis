// todo - Clean Architecture dependency violation. Fix
import fs from 'fs';
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import { CreateTable } from '../table/create-table';
import { buildLineageDto, LineageDto } from './lineage-dto';
import { CreateModel } from '../model/create-model';
import { ReadTables } from '../table/read-tables';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import LineageRepo from '../../infrastructure/persistence/lineage-repo';
import { ReadColumns } from '../column/read-columns';
import { Model } from '../entities/model';
import { CreateDependency } from '../dependency/create-dependency';
import { ColumnRef } from '../value-types/logic';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: number;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<LineageDto>;

// interface ContextualStatementReference extends StatementReference {
//   statementIndex: number;
// }

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

  readonly #createDependency: CreateDependency;

  readonly #readTables: ReadTables;

  readonly #readColumns: ReadColumns;

  readonly #parseSQL: ParseSQL;

  readonly #lineageRepo: LineageRepo;

  #lineage?: Lineage;

  #models: Model[] = [];

  // #table?: Table;

  // #columns: Column[] = [];

  constructor(
    createModel: CreateModel,
    createTable: CreateTable,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    readTables: ReadTables,
    readColumns: ReadColumns,
    parseSQL: ParseSQL,
    lineageRepo: LineageRepo
  ) {
    this.#createModel = createModel;
    this.#createTable = createTable;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#readTables = readTables;
    this.#readColumns = readColumns;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
  }

  // #getTableName = (): string => {
  //   const tableSelfRef = `${SQLElement.CREATE_TABLE_STATEMENT}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
  //   const selectRef = `${SQLElement.FROM_EXPRESSION_ELEMENT}.${SQLElement.TABLE_EXPRESSION}.${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;

  //   const tableSelfSearchRes: string[] = [];
  //   const tableSelectRes: string[] = [];
  //   if (!this.#model) throw new ReferenceError('Model property is undefined');

  //   this.#model.logic.statementRefs.flat().forEach((element) => {

  //     if (element.path.includes(tableSelfRef)) {
  //       if (!element.tableName)
  //         throw new ReferenceError(
  //           'tableName of TABLE references does not exist'
  //         );
  //       tableSelfSearchRes.push(element.tableName);
  //     }

  //     else if(element.path.includes(selectRef)){
  //       if (!element.tableName)
  //         throw new ReferenceError(
  //           'tableName of TABLE references does not exist'
  //         );
  //         tableSelectRes.push(`${element.tableName}_view`);
  //     }
  //   });

  //   if (tableSelfSearchRes.length > 1)
  //     throw new ReferenceError(`Multiple instances of ${tableSelfRef} found`);
  //   if (tableSelfSearchRes.length < 1){
  //     if (tableSelectRes.length < 1)
  //       throw new ReferenceError(`${tableSelfRef} or ${selectRef} not found`);
  //     if (tableSelectRes.length > 1)
  //         throw new ReferenceError(`Multiple instances of ${selectRef} found`);
  //     return tableSelectRes[0];
  //   }
  //   return tableSelfSearchRes[0];
  // };

  // #getSelfColumnDefinitions = (
  //   statementRefs: StatementReference[][]
  // ): ContextualStatementReference[] => {
  //   const columnSelfRefs = [
  //     `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.COLUMN_REFERENCE}.${SQLElement.IDENTIFIER}`,
  //     `${SQLElement.COLUMN_DEFINITION}.${SQLElement.IDENTIFIER}`,
  //     `${SQLElement.SELECT_CLAUSE_ELEMENT}.${SQLElement.WILDCARD_EXPRESSION}.${SQLElement.WILDCARD_IDENTIFIER}`,
  //   ];

  //   let statementIndex = 0;
  //   const selfColumnRefs: ContextualStatementReference[] = [];
  //   statementRefs.forEach((statement) => {
  //     statement.forEach((element) => {
  //       if (columnSelfRefs.some((ref) => element.path.includes(ref))) {
  //         const selfColumnRef: ContextualStatementReference = {
  //           ...element,
  //           statementIndex,
  //         };
  //         selfColumnRefs.push(selfColumnRef);
  //       }
  //     });

  //     statementIndex += 1;
  //   });

  //   return selfColumnRefs;
  // };

  // #getParentTableNames = (
  //   statementRefs: StatementReference[][]
  // ): string[] => {
  //   const tableRef = `${SQLElement.TABLE_REFERENCE}.${SQLElement.IDENTIFIER}`;
  //   const tableSelfRefs = [
  //     `${SQLElement.CREATE_TABLE_STATEMENT}.${tableRef}`,
  //     `${SQLElement.INSERT_STATEMENT}.${tableRef}`,
  //   ];

  //   const parentTableNames: string[] = [];

  //   statementRefs.flat().forEach((element) => {
  //     if (tableSelfRefs.some((ref) => element.path.includes(ref))) return;

  //     if (element.type === ReferenceType.TABLE) {
  //       if (!element.tableName)
  //         throw new ReferenceError(
  //           'tableName of TABLE references does not exist'
  //         );
  //       if (!parentTableNames.includes(element.tableName))
  //         parentTableNames.push(element.tableName);
  //     } else if (element.type === ReferenceType.COLUMN) {
  //       if (!element.columnName)
  //         throw new ReferenceError(
  //           'columnName of COLUMN references does not exist'
  //         );
  //       if (element.tableName && !parentTableNames.includes(element.tableName))
  //         parentTableNames.push(element.tableName);
  //     }
  //   });

  //   return parentTableNames;
  // };

  #setLineage = async (
    lineageId?: string,
    lineageCreatedAt?: number
  ): Promise<void> => {
    // todo - enable lineage updating
    // this.#lineage =
    //   lineageId && lineageCreatedAt
    //     ? Lineage.create({
    //         id: lineageId,
    //         createdAt: lineageCreatedAt,
    //       })
    //     : Lineage.create({ id: new ObjectId().toHexString() });

    this.#lineage = Lineage.create({ id: new ObjectId().toHexString() });

    if (!(lineageId && lineageCreatedAt))
      await this.#lineageRepo.insertOne(this.#lineage);
  };

  #parseLogic = async (sql: string): Promise<string> => {
    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute(
      { dialect: 'snowflake', sql },
      { jwt: 'todo' }
    );

    if (!parseSQLResult.success) throw new Error(parseSQLResult.error);
    if (!parseSQLResult.value)
      throw new SyntaxError(`Parsing of SQL logic failed`);

    return JSON.stringify(parseSQLResult.value);
  };

  #getDbtNodes = (location: string): any => {
    const data = fs.readFileSync(location, 'utf-8');

    const catalog = JSON.parse(data);

    return catalog.nodes;
  };

  // #setModel = async (tableId: string): Promise<void> => {
  //   if (!this.#lineage)
  //     throw new ReferenceError('Lineage property is undefined');

  //   // const location = `C://Users/felix-pc/Desktop/Test/${tableId}.sql`;
  //   const location = `C://Users/nasir/OneDrive/Desktop/sql_files/${tableId}.sql`;

  //   const parsedLogic = await this.#parseLogic(location);

  //   const createModelResult: CreateModelResponse =
  //     await this.#createModel.execute(
  //       {
  //         id: tableId,
  //         location,
  //         parsedLogic,
  //         lineageId: this.#lineage.id,
  //       },
  //       { organizationId: 'todo' }
  //     );

  //   if (!createModelResult.success) throw new Error(createModelResult.error);
  //   if (!createModelResult.value) throw new Error('Creation of model failed');

  //   this.#model = createModelResult.value;
  // };

  #generateWarehouseResources = async (): Promise<void> => {
    const dbtCatalogNodes = this.#getDbtNodes(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/catalog.json`
    );
    const dbtManifestNodes = this.#getDbtNodes(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/manifest.json`
    );

    const dbtModelKeys = Object.keys(dbtCatalogNodes);

    if (!dbtModelKeys.length) throw new ReferenceError('No dbt models found');

    await Promise.all(
      dbtModelKeys.map(async (key) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');

        const dbtModel = dbtCatalogNodes[key];
        const manifest = dbtManifestNodes[key];

        const parsedLogic = await this.#parseLogic(manifest.compiled_sql);

        const createModelResult = await this.#createModel.execute(
          {
            dbtModelId: dbtModel.unique_id,
            lineageId: this.#lineage.id,
            parsedLogic,
          },
          { organizationId: 'todo' }
        );

        if (!createModelResult.success)
          throw new Error(createModelResult.error);
        if (!createModelResult.value)
          throw new SyntaxError(`Creation of model failed`);

        const model = createModelResult.value;

        this.#models.push(model);

        const createTableResult = await this.#createTable.execute(
          {
            name: dbtModel.metadata.name,
            dbtModelId: dbtModel.unique_id,
            modelId: model.id,
            lineageId: this.#lineage.id,
          },
          { organizationId: 'todo' }
        );

        if (!createTableResult.success)
          throw new Error(createTableResult.error);
        if (!createTableResult.value)
          throw new SyntaxError(`Creation of table failed`);

        const table = createTableResult.value;

        await Promise.all(
          dbtModel.columns.map(async (column: any) => {
            if (!this.#lineage)
              throw new ReferenceError('Lineage property is undefined');

            // todo - add additional properties like index
            const createColumnResult = await this.#createColumn.execute(
              {
                name: column.name,
                tableId: table.id,
                dbtModelId: table.dbtModelId,
                lineageId: this.#lineage.id,
              },
              { organizationId: 'todo' }
            );

            if (!createColumnResult.success)
              throw new Error(createColumnResult.error);
            if (!createColumnResult.value)
              throw new SyntaxError(`Creation of column failed`);
          })
        );
      })
    );
  };

  // #setTable = async (): Promise<void> => {
  //   if (!this.#lineage)
  //     throw new ReferenceError('Lineage property is undefined');
  //   if (!this.#model) throw new ReferenceError('Model property is undefined');

  //   const name = this.#getTableName();

  //   const createTableResult = await this.#createTable.execute(
  //     {
  //       name,
  //       modelId: this.#model.id,
  //       lineageId: this.#lineage.id,
  //     },
  //     { organizationId: 'todo' }
  //   );

  //   if (!createTableResult.success) throw new Error(createTableResult.error);
  //   if (!createTableResult.value)
  //     throw new SyntaxError(`Creation of table ${name} failed`);
  //   this.#table = createTableResult.value;
  // };

  // #createParents = async (parentNames: string[]): Promise<void> => {
  //   // todo - can this be async?
  //   // todo - lineage nowhere stored and not returned for display. But in the end it is only columns and table object
  //   await Promise.all(
  //     parentNames.map(async (element) => {
  //       if (!this.#lineage)
  //         throw new ReferenceError('Lineage property is undefined');

  //       // todo - Check if best practice for ioc registry / awilix
  //       return new CreateLineage(
  //         this.#createModel,
  //         this.#createTable,
  //         this.#createColumn,
  //         this.#readTables,
  //         this.#readColumns,
  //         this.#parseSQL,
  //         this.#lineageRepo
  //       ).execute(
  //         {
  //           tableId: element,
  //           lineageId: this.#lineage.id,
  //           lineageCreatedAt: this.#lineage.createdAt,
  //         },
  //         { organizationId: 'todo' }
  //       );
  //     })
  //   );
  // };

  // #getParentTables = async (parentNames: string[]): Promise<Table[]> => {
  //   if (!this.#lineage)
  //     throw new ReferenceError('Lineage property is undefined');

  //   const readParentsResult = await this.#readTables.execute(
  //     { name: parentNames, lineageId: this.#lineage.id },
  //     { organizationId: 'todo' }
  //   );

  //   if (!readParentsResult.success) throw new Error(readParentsResult.error);
  //   if (!readParentsResult.value)
  //     throw new SyntaxError(`Reading parent tables failed`);

  //   return readParentsResult.value;
  // };

  // #createSelfColumns = async (
  //   reference: ContextualStatementReference,
  //   parentTableIds: string[]
  // ): Promise<CreateColumnResponseDto | CreateColumnResponseDto[]> => {
  //   if (!this.#lineage)
  //     throw new ReferenceError('Lineage property is undefined');
  //   if (!this.#model) throw new ReferenceError('Model property is undefined');
  //   if (!this.#table) throw new ReferenceError('Table property is undefined');

  //   if (!reference.path.includes(SQLElement.WILDCARD_IDENTIFIER))
  //     return this.#createColumn.execute(
  //       {
  //         selfRef: {
  //           columnName: reference.columnName,
  //           tableName: reference.tableName,
  //           path: reference.path,
  //           type: reference.type,
  //         },
  //         statementSourceReferences:
  //           this.#model.logic.statementRefs[reference.statementIndex],
  //         tableId: this.#table.id,
  //         parentTableIds,
  //         lineageId: this.#lineage.id,
  //       },
  //       { organizationId: 'todo' }
  //     );

  //   if (parentTableIds.length !== 1)
  //     throw new ReferenceError('Wildcard - parent-table mismatch');
  //   const readColumnsResult = await this.#readColumns.execute(
  //     { tableId: parentTableIds[0], lineageId: this.#lineage.id },
  //     { organizationId: 'todo' }
  //   );

  //   if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
  //   if (!readColumnsResult.value)
  //     throw new SyntaxError(`Readinng of parent-table columns failed`);

  //   const createColumnResults = await Promise.all(
  //     readColumnsResult.value.map((column) => {
  //       if (!this.#lineage)
  //         throw new ReferenceError('Lineage property is undefined');
  //       if (!this.#model)
  //         throw new ReferenceError('Model property is undefined');
  //       if (!this.#table)
  //         throw new ReferenceError('Table property is undefined');

  //       return this.#createColumn.execute(
  //         {
  //           selfRef: {
  //             columnName: column.name,
  //             tableName: reference.tableName,
  //             path: reference.path,
  //             type: ReferenceType.COLUMN,
  //           },
  //           statementSourceReferences:
  //             this.#model.logic.statementRefs[reference.statementIndex],
  //           tableId: this.#table.id,
  //           parentTableIds,
  //           lineageId: this.#lineage.id,
  //         },
  //         { organizationId: 'todo' }
  //       );
  //     })
  //   );

  //   return createColumnResults;
  // };

  // #setColumns = async (parentTableIds: string[]): Promise<void> => {
  //   if (!this.#model) throw new ReferenceError('Model property is undefined');

  //   let selfColumnReferences = this.#getSelfColumnDefinitions(
  //     this.#model.logic.statementRefs
  //   );

  //   const setColumnRefs = selfColumnReferences.filter((ref) =>
  //     ref.path.includes(SQLElement.SET_EXPRESSION)
  //   );
  //   const columnRefs = selfColumnReferences.filter(
  //     (ref) => !ref.path.includes(SQLElement.SET_EXPRESSION)
  //   );

  //   const uniqueSetColumnRefs = setColumnRefs.filter(
  //     (value, index, self) =>
  //       index ===
  //       self.findIndex(
  //         (t) =>
  //           t.columnName === value.columnName &&
  //           t.path === value.path &&
  //           t.type === value.type
  //       )
  //   );

  //   selfColumnReferences = uniqueSetColumnRefs.concat(columnRefs);

  //   const createColumnResults = (
  //     await Promise.all(
  //       selfColumnReferences.map(async (reference) =>
  //         this.#createSelfColumns(reference, parentTableIds)
  //       )
  //     )
  //   ).flat();

  //   createColumnResults.forEach((result) => {
  //     if (!result.success) throw new Error(result.error);
  //     if (!result.value) throw new SyntaxError(`Creation of column failed`);

  //     this.#columns.push(result.value);
  //   });
  // };

  // Identifies the statement root (e.g. create_table_statement.select_statement) of a specific reference path
  #getStatementRoot = (path: string): string => {
    const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
    if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
      // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
      throw new RangeError('Statement root not found for column reference');

    return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  };

  // Checks if parent dependency can be mapped on the provided self column or to another column of the self table.
  #isTargetRefDependency = (
    potentialDependency: ColumnRef,
    selfRef: ColumnRef
  ): boolean => {
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

    if (!isStatementDependency) return false;

    if (
      (selfStatementRoot.includes(SQLElement.SELECT_STATEMENT) &&
        potentialDependency.isWildcardRef) ||
      selfRef.name === potentialDependency.name
    )
      return true;

    throw new RangeError('Unhandled isTargetRefDependency');
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    // todo-replace
    console.log(auth);
    try {
      await this.#setLineage(request.lineageId, request.lineageCreatedAt);

      await this.#generateWarehouseResources();

      this.#models.forEach((model) => {
        model.logic.statementRefs.forEach((refs) => {
          let selfRefs = refs.columns.filter((column) => column.isSelfRef);

          const setColumnRefs = selfRefs.filter((ref) =>
            ref.path.includes(SQLElement.SET_EXPRESSION)
          );

          const uniqueSetColumnRefs = setColumnRefs.filter(
            (value, index, self) =>
              index ===
              self.findIndex(
                (ref) =>
                  ref.name === value.name &&
                  ref.path === value.path &&
                  ref.tableName === value.tableName
              )
          );

          const columnRefs = selfRefs.filter(
            (ref) => !ref.path.includes(SQLElement.SET_EXPRESSION)
          );

          selfRefs = uniqueSetColumnRefs.concat(columnRefs);

          const isColumnRef = (
            item: ColumnRef | undefined
          ): item is ColumnRef => !!item;

          selfRefs.forEach((selfRef) => {
            const dependencies: ColumnRef[] = refs.columns
              .map((columnRef) => {
                const isDependency = this.#isTargetRefDependency(
                  columnRef,
                  selfRef
                );
                if (isDependency) return columnRef;
                return undefined;
              })
              .filter(isColumnRef);

            dependencies.forEach((dependency) => {
              if (!this.#lineage)
                throw new ReferenceError('Lineage property is undefined');
              // this.#createDependency({selfRef, dependency, refs, model.id, parentTableIds} )
              this.#createDependency.execute(
                {
                  selfRef,
                  parentRef: dependency,
                  selfModelId: model.dbtModelId,
                  parentModelDbtIds: this.#models.map(
                    (element) => element.dbtModelId
                  ),
                  lineageId: this.#lineage.id,
                },
                { organizationId: 'todo' }
              );
            });
          });
        });
      });

      // await this.#setModel(request.tableId);

      // await this.#setTable();

      // if (!this.#model) throw new ReferenceError('Model property is undefined');

      // const parentNames = this.#getParentTableNames(
      //   this.#model.logic.statementRefs
      // );

      // const parentTableIds: string[] = [];
      // if (parentNames.length) {
      //   await this.#createParents(parentNames);

      //   const parentTables = await this.#getParentTables(parentNames);

      //   const ids = parentTables.map((tableElement) => tableElement.id);
      //   parentTableIds.push(...ids);
      // }

      // await this.#setColumns(parentTableIds);

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
