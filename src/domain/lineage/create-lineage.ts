// todo - Clean Architecture dependency violation. Fix
import fs from 'fs';
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import { CreateMaterialization } from '../materialization/create-materialization';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import { Logic, ColumnRef, Refs } from '../entities/logic';
import {
  CreateDependency,
  CreateDependencyResponse,
} from '../dependency/create-dependency';
import { Dependency, DependencyType } from '../entities/dependency';
import { ReadColumns } from '../column/read-columns';
import { Materialization } from '../entities/materialization';
import { Column } from '../entities/column';
import { ILineageRepo } from './i-lineage-repo';
import { IColumnRepo } from '../column/i-column-repo';
import { IMaterializationRepo } from '../materialization/i-materialization-repo';
import { IDependencyRepo } from '../dependency/i-dependency-repo';
import { ILogicRepo } from '../logic/i-logic-repo';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: number;
}

export interface CreateLineageAuthDto {
  organizationId: string;
}

export type CreateLineageResponseDto = Result<Lineage>;

interface DbtResources {
  nodes: any;
  sources: any;
}

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto
    >
{
  readonly #createLogic: CreateLogic;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createDependency: CreateDependency;

  readonly #parseSQL: ParseSQL;

  readonly #lineageRepo: ILineageRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #readColumns: ReadColumns;

  #lineage?: Lineage;

  #logics: Logic[];

  #materializations: Materialization[];

  #columns: Column[];

  #dependencies: Dependency[];

  #lastQueryDependency?: ColumnRef;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    parseSQL: ParseSQL,
    lineageRepo: ILineageRepo,
    logicRepo: ILogicRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo,
    readColumns: ReadColumns
  ) {
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#parseSQL = parseSQL;
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#readColumns = readColumns;
    this.#logics = [];
    this.#materializations = [];
    this.#columns = [];
    this.#dependencies = [];
    this.#lineage = undefined;
    this.#lastQueryDependency = undefined;
  }

  /* Building a new lineage object that is referenced by resources like columns and materializations */
  #buildLineage = async (
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

  /* Sends sql to parse SQL microservices and receives parsed SQL logic back */
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

  #generateColumn = async (
    columnDefinition: any,
    dbtModelId: string,
    materializationId: string
  ): Promise<Column> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    // todo - add additional properties like index
    const createColumnResult = await this.#createColumn.execute(
      {
        dbtModelId,
        name: columnDefinition.name,
        index: columnDefinition.index,
        type: columnDefinition.type,
        materializationId,
        lineageId: lineage.id,
        writeToPersistence: false,
      },
      { organizationId: 'todo' }
    );

    if (!createColumnResult.success) throw new Error(createColumnResult.error);
    if (!createColumnResult.value)
      throw new SyntaxError(`Creation of column failed`);

    return createColumnResult.value;
  };

  #generateWarehouseSource = async (source: any): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          materializationType: source.metadata.type,
          name: source.metadata.name,
          dbtModelId: source.unique_id,
          schemaName: source.metadata.schema,
          databaseName: source.metadata.database,
          logicId: 'todo - read from snowflake',
          lineageId: lineage.id,
          writeToPersistence: false,
        },
        { organizationId: 'todo' }
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    this.#materializations.push(materialization);

    const columns = await Promise.all(
      Object.keys(source.columns).map(async (columnKey) =>
        this.#generateColumn(
          source.columns[columnKey],
          materialization.dbtModelId,
          materialization.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  #generateDbtModel = async (model: any, modelManifest: any): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    const sql = modelManifest.compiled_sql;

    const parsedLogic = await this.#parseLogic(sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        dbtModelId: model.unique_id,
        sql,
        lineageId: lineage.id,
        parsedLogic,
        writeToPersistence: false,
      },
      { organizationId: 'todo' }
    );

    if (!createLogicResult.success) throw new Error(createLogicResult.error);
    if (!createLogicResult.value)
      throw new SyntaxError(`Creation of logic failed`);

    const logic = createLogicResult.value;

    this.#logics.push(logic);

    const createMaterializationResult =
      await this.#createMaterialization.execute(
        {
          materializationType: model.metadata.type,
          name: model.metadata.name,
          dbtModelId: model.unique_id,
          schemaName: model.metadata.schema,
          databaseName: model.metadata.database,
          logicId: logic.id,
          lineageId: lineage.id,
          writeToPersistence: false,
        },
        { organizationId: 'todo' }
      );

    if (!createMaterializationResult.success)
      throw new Error(createMaterializationResult.error);
    if (!createMaterializationResult.value)
      throw new SyntaxError(`Creation of materialization failed`);

    const materialization = createMaterializationResult.value;

    this.#materializations.push(materialization);

    const columns = await Promise.all(
      Object.keys(model.columns).map(async (columnKey) =>
        this.#generateColumn(
          model.columns[columnKey],
          materialization.dbtModelId,
          materialization.id
        )
      )
    );

    this.#columns.push(...columns);
  };

  /* Get dbt nodes from catalog.json or manifest.json */
  #getDbtResources = (location: string): DbtResources => {
    const data = fs.readFileSync(location, 'utf-8');

    const catalog = JSON.parse(data);

    const { nodes } = catalog;
    const { sources } = catalog;

    return { nodes, sources };
  };

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  #generateWarehouseResources = async (): Promise<void> => {
    const dbtCatalogResources = this.#getDbtResources(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/catalog/web-samples/sample-1-no-v_date_stg.json`
      // `C:/Users/nasir/OneDrive/Desktop/lineage-analysis/test/use-cases/dbt/catalog/web-samples/sample-1-no-v_date_stg.json`
    );
    const dbtManifestResources = this.#getDbtResources(
      `C:/Users/felix-pc/Documents/Repositories/lineage-analysis/test/use-cases/dbt/manifest/web-samples/sample-1-no-v_date_stg.json`
      // `C:/Users/nasir/OneDrive/Desktop/lineage-analysis/test/use-cases/dbt/manifest/web-samples/sample-1-no-v_date_stg.json`
    );

    const dbtSourceKeys = Object.keys(dbtCatalogResources.sources);

    await Promise.all(
      dbtSourceKeys.map(async (key) =>
        this.#generateWarehouseSource(dbtCatalogResources.sources[key])
      )
    );

    const dbtModelKeys = Object.keys(dbtCatalogResources.nodes);

    if (!dbtModelKeys.length) throw new ReferenceError('No dbt models found');

    await Promise.all(
      dbtModelKeys.map(async (key) =>
        this.#generateDbtModel(
          dbtCatalogResources.nodes[key],
          dbtManifestResources.nodes[key]
        )
      )
    );
  };

  /* Identifies the statement root (e.g. create_materialization_statement.select_statement) of a specific reference path */
  #getStatementRoot = (path: string): string => {
    const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
    if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
      // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
      throw new RangeError('Statement root not found for column reference');

    return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  };

  /* Checks if parent dependency can be mapped on the provided self column or to another column of the self materialization. */
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
    const isJoinOn =
      potentialDependency.path.includes(SQLElement.JOIN_ON_CONDITION) &&
      selfRef.name !== potentialDependency.name;

    if (isWildcardRef || isSameName || isGroupBy) return true;

    if (isJoinOn) return false;
    if (potentialDependency.name !== selfRef.name) return false;

    throw new RangeError(
      'Unhandled case when checking if is dependency of target'
    );
  };

  /* Get all relevant statement references that are a valid dependency to parent resources */
  #getDataDependencyRefs = (statementRefs: Refs): ColumnRef[] => {
    let dataDependencyRefs = statementRefs.columns.filter(
      (column) => column.dependencyType === DependencyType.DATA
    );
    dataDependencyRefs.push(...statementRefs.wildcards);

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
            ref.materializationName === value.materializationName
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

    return dataDependencyRefs;
  };

  /* Compares two ColumnRef objects if they are equal */
  #columnRefIsEqual = (fst: ColumnRef, snd: ColumnRef | undefined): boolean => {
    if (!fst || !snd) return false;

    return (
      fst.alias === snd.alias &&
      fst.databaseName === snd.databaseName &&
      fst.dependencyType === snd.dependencyType &&
      fst.isWildcardRef === snd.isWildcardRef &&
      fst.materializationName === snd.materializationName &&
      fst.name === snd.name &&
      fst.path === snd.path &&
      fst.schemaName === snd.schemaName &&
      fst.warehouseName === snd.warehouseName
    );
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildStatementRefDependencies = async (
    selfRef: ColumnRef,
    statementRefs: Refs,
    dbtModelId: string
  ): Promise<void> => {
    const isColumnRef = (item: ColumnRef | undefined): item is ColumnRef =>
      !!item;

    const dbtModelIdElements = dbtModelId.split('.');
    if (dbtModelIdElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const dbtModelIdProjectRoot = `${dbtModelIdElements[0]}.${dbtModelIdElements[1]}`;

    const wildcardDependencies = await this.#getDependenciesForWildcard(
      statementRefs,
      selfRef,
      dbtModelIdProjectRoot
    );

    const columnDependencies = statementRefs.columns
      .map((columnRef) => {
        const isDependency = this.#isDependencyOfTarget(columnRef, selfRef);
        if (isDependency) return columnRef;
        return undefined;
      })
      .filter(isColumnRef);

    const dependencies: ColumnRef[] =
      wildcardDependencies.concat(columnDependencies);

    const isCreateDependencyResponse = (
      item: CreateDependencyResponse | null
    ): item is CreateDependencyResponse => !!item;

    const createDependencyResults = await Promise.all(
      dependencies.map(
        async (dependency): Promise<CreateDependencyResponse | null> => {
          if (!this.#lineage)
            throw new ReferenceError('Lineage property is undefined');

          if (this.#columnRefIsEqual(dependency, this.#lastQueryDependency))
            return null;

          if (dependency.dependencyType === DependencyType.QUERY)
            this.#lastQueryDependency = dependency;

          const createDependencyResult = await this.#createDependency.execute(
            {
              selfRef,
              parentRef: dependency,
              selfDbtModelId: dbtModelId,
              parentDbtModelIds: this.#logics.map(
                (element) => element.dbtModelId
              ),
              lineageId: this.#lineage.id,
              writeToPersistence: false,
            },
            { organizationId: 'todo' }
          );

          return createDependencyResult;
        }
      )
    );

    const onlyCreateDependencyResults = createDependencyResults.filter(
      isCreateDependencyResponse
    );

    if (onlyCreateDependencyResults.some((result) => !result.success)) {
      const errorResults = onlyCreateDependencyResults.filter(
        (result) => result.error
      );
      throw new Error(errorResults[0].error);
    }

    if (onlyCreateDependencyResults.some((result) => !result.value))
      throw new SyntaxError(`Creation of dependencies failed`);

    const isValue = (item: Dependency | undefined): item is Dependency =>
      !!item;

    const values = onlyCreateDependencyResults
      .map((result) => result.value)
      .filter(isValue);

    this.#dependencies.push(...values);
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependencies = async (): Promise<void> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    await Promise.all(
      this.#logics.map(async (logic) => {
        await Promise.all(
          logic.statementRefs.map(async (refs) => {
            const dataDependencyRefs = this.#getDataDependencyRefs(refs);

            await Promise.all(
              dataDependencyRefs.map(async (selfRef) =>
                this.#buildStatementRefDependencies(
                  selfRef,
                  refs,
                  logic.dbtModelId
                )
              )
            );
          })
        );
      })
    );
  };

  #getDependenciesForWildcard = async (
    statementRefs: Refs,
    selfRef: ColumnRef,
    dbtModelIdProjectRoot: string
  ): Promise<ColumnRef[]> => {
    const dbtIdProjectRoot =
      dbtModelIdProjectRoot.slice(-1) !== '.'
        ? dbtModelIdProjectRoot
        : dbtModelIdProjectRoot.substring(0, dbtModelIdProjectRoot.length - 1);

    const dependencies: ColumnRef[] = [];
    await Promise.all(
      statementRefs.wildcards.map(async (wildcardRef) => {
        if (!this.#lineage)
          throw new ReferenceError('Lineage property is undefined');

        const readColumnsResult = await this.#readColumns.execute(
          {
            dbtModelId: `${dbtIdProjectRoot}.${wildcardRef.materializationName}`,
            lineageId: this.#lineage.id,
          },
          { organizationId: 'todo' }
        );

        if (!readColumnsResult.success)
          throw new Error(readColumnsResult.error);
        if (!readColumnsResult.value)
          throw new ReferenceError(`Reading of columns failed`);

        const colsFromWildcard = readColumnsResult.value;

        colsFromWildcard.forEach((column) => {
          const newColumnRef: ColumnRef = {
            materializationName: wildcardRef.materializationName,
            path: wildcardRef.path,
            dependencyType: wildcardRef.dependencyType,
            isWildcardRef: wildcardRef.isWildcardRef,
            name: column.name,
            alias: wildcardRef.alias,
            schemaName: wildcardRef.schemaName,
            databaseName: wildcardRef.databaseName,
            warehouseName: wildcardRef.warehouseName,
          };

          const isDependency = this.#isDependencyOfTarget(
            newColumnRef,
            selfRef
          );
          if (isDependency) dependencies.push(newColumnRef);
        });
      })
    );

    return dependencies;
  };

  #writeWhResourcesToPersistence = async (): Promise<void> => {
    await this.#logicRepo.insertMany(this.#logics);

    await this.#materializationRepo.insertMany(this.#materializations);

    await this.#columnRepo.insertMany(this.#columns);
  };

  #writeDependenciesToPersistence = async (): Promise<void> => {
    await this.#dependencyRepo.insertMany(this.#dependencies);
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto
  ): Promise<CreateLineageResponseDto> {
    // todo-replace
    console.log(auth);
    try {
      // todo - Workaround. Fix ioc container
      this.#lineage = undefined;
      this.#logics = [];
      this.#materializations = [];
      this.#columns = [];
      this.#dependencies = [];
      this.#lastQueryDependency = undefined;

      await this.#buildLineage(request.lineageId, request.lineageCreatedAt);

      await this.#generateWarehouseResources();

      await this.#writeWhResourcesToPersistence();

      await this.#buildDependencies();

      await this.#writeDependenciesToPersistence();

      // if (auth.organizationId !== 'TODO')
      //   throw new Error('Not authorized to perform action');

      // todo - how to avoid checking if property exists. A sub-method created the property
      if (!this.#lineage)
        throw new ReferenceError('Lineage property is undefined');

      return Result.ok(this.#lineage);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
