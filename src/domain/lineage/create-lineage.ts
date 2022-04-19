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
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import LineageRepo from '../../infrastructure/persistence/lineage-repo';
import { Model } from '../entities/model';
import { CreateDependency } from '../dependency/create-dependency';
import { ColumnRef } from '../value-types/logic';
import { DependencyType } from '../entities/dependency';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: number;
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
  readonly #createModel: CreateModel;

  readonly #createTable: CreateTable;

  readonly #createColumn: CreateColumn;

  readonly #createDependency: CreateDependency;

  readonly #parseSQL: ParseSQL;

  readonly #lineageRepo: LineageRepo;

  #lineage?: Lineage;

  #models: Model[] = [];

  constructor(
    createModel: CreateModel,
    createTable: CreateTable,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    parseSQL: ParseSQL,
    lineageRepo: LineageRepo
  ) {
    this.#createModel = createModel;
    this.#createTable = createTable;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
  }

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
          Object.keys(dbtModel.columns).map(async (columnKey) => {
            if (!this.#lineage)
              throw new ReferenceError('Lineage property is undefined');

            const column = dbtModel.columns[columnKey];

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

  // Identifies the statement root (e.g. create_table_statement.select_statement) of a specific reference path
  #getStatementRoot = (path: string): string => {
    const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
    if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
      // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
      throw new RangeError('Statement root not found for column reference');

    return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  };

  // Checks if parent dependency can be mapped on the provided self column or to another column of the self table.
  #isDependencyOfTarget = (
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

    const isSelfSelectStatement = selfStatementRoot.includes(
      SQLElement.SELECT_STATEMENT
    );

    const isWildcardRef =
      isSelfSelectStatement && potentialDependency.isWildcardRef;
    const isSameName =
      isSelfSelectStatement && selfRef.name === potentialDependency.name;
    const isGroupBy =
      potentialDependency.path.includes(SQLElement.GROUPBY_CLAUSE) &&
      selfRef.name !== potentialDependency.name;

    if (isWildcardRef || isSameName || isGroupBy) return true;

    throw new RangeError(
      'Unhandled case when checking if is dependency of target'
    );
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
          let dataDependencyRefs = refs.columns.filter(
            (column) => column.dependencyType === DependencyType.DATA
          );
          dataDependencyRefs.push(...refs.wildcards);

          const setColumnRefs = dataDependencyRefs.filter((ref) =>
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

          let columnRefs = dataDependencyRefs.filter(
            (ref) => !ref.path.includes(SQLElement.SET_EXPRESSION)
          );

          dataDependencyRefs = uniqueSetColumnRefs.concat(columnRefs);

          const withColumnRefs = dataDependencyRefs.filter(
            (ref) =>
              ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT) &&
              !ref.path.includes(SQLElement.COMMON_TABLE_EXPRESSION)
          );
          columnRefs = dataDependencyRefs.filter(
            (ref) => !ref.path.includes(SQLElement.WITH_COMPOUND_STATEMENT)
          );

          dataDependencyRefs = withColumnRefs.concat(columnRefs);

          const isColumnRef = (
            item: ColumnRef | undefined
          ): item is ColumnRef => !!item;

          dataDependencyRefs.forEach((selfRef) => {
            const dependencies: ColumnRef[] = refs.columns
              .map((columnRef) => {
                const isDependency = this.#isDependencyOfTarget(
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
