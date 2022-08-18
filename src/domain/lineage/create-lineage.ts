// todo - Clean Architecture dependency violation. Fix
import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import SQLElement from '../value-types/sql-element';
import { CreateColumn } from '../column/create-column';
import { CreateMaterialization } from '../materialization/create-materialization';
import { CreateLogic } from '../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../sql-parser-api/parse-sql';
import { Lineage } from '../entities/lineage';
import {
  Logic,
  ColumnRef,
  Refs,
  MaterializationDefinition,
  DashboardRef,
} from '../entities/logic';
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
import { DbConnection } from '../services/i-db';
import {
  QuerySnowflakeHistory,
  QueryHistoryResponseDto,
} from '../query-snowflake-history-api/query-snowflake-history';
import { Dashboard } from '../entities/dashboard';
import { CreateExternalDependency } from '../dependency/create-external-dependency';
import { IDashboardRepo } from '../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../dashboard/create-dashboard';
import  BiLayer from '../value-types/bilayer';

export interface CreateLineageRequestDto {
  lineageId?: string;
  lineageCreatedAt?: number;
  targetOrganizationId: string;
  catalog: string;
  manifest: string;
  biType?: string;
}

export interface CreateLineageAuthDto {
  jwt: string;
  isSystemInternal: boolean;
}

export type CreateLineageResponseDto = Result<Lineage>;

interface DbtResources {
  nodes: any;
  sources: any;

  parent_map: { [key: string]: string[] };
}

export class CreateLineage
  implements
    IUseCase<
      CreateLineageRequestDto,
      CreateLineageResponseDto,
      CreateLineageAuthDto,
      DbConnection
    >
{
  readonly #createLogic: CreateLogic;

  readonly #createMaterialization: CreateMaterialization;

  readonly #createColumn: CreateColumn;

  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #parseSQL: ParseSQL;

  readonly #querySnowflakeHistory: QuerySnowflakeHistory;

  readonly #lineageRepo: ILineageRepo;

  readonly #logicRepo: ILogicRepo;

  readonly #materializationRepo: IMaterializationRepo;

  readonly #columnRepo: IColumnRepo;

  readonly #dependencyRepo: IDependencyRepo;

  readonly #dashboardRepo: IDashboardRepo;

  readonly #readColumns: ReadColumns;

  #dbConnection: DbConnection;

  #lineage?: Lineage;

  #logics: Logic[];

  #materializations: Materialization[];

  #columns: Column[];

  #dependencies: Dependency[];

  #dashboards: Dashboard[];

  #lastQueryDependency?: ColumnRef;

  #matDefinitionCatalog: MaterializationDefinition[];

  #targetOrganizationId: string;

  #jwt: string;

  #isSystemInternal: boolean;

  constructor(
    createLogic: CreateLogic,
    createMaterialization: CreateMaterialization,
    createColumn: CreateColumn,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    parseSQL: ParseSQL,
    querySnowflakeHistory: QuerySnowflakeHistory,
    lineageRepo: ILineageRepo,
    logicRepo: ILogicRepo,
    materializationRepo: IMaterializationRepo,
    columnRepo: IColumnRepo,
    dependencyRepo: IDependencyRepo,
    dashboardRepo: IDashboardRepo,
    readColumns: ReadColumns,
    createDashboard: CreateDashboard
  ) {
    this.#createLogic = createLogic;
    this.#createMaterialization = createMaterialization;
    this.#createColumn = createColumn;
    this.#createDependency = createDependency;
    this.#createExternalDependency = createExternalDependency;
    this.#createDashboard = createDashboard;
    this.#parseSQL = parseSQL;
    this.#querySnowflakeHistory = querySnowflakeHistory;
    this.#lineageRepo = lineageRepo;
    this.#logicRepo = logicRepo;
    this.#materializationRepo = materializationRepo;
    this.#columnRepo = columnRepo;
    this.#dependencyRepo = dependencyRepo;
    this.#dashboardRepo = dashboardRepo;
    this.#readColumns = readColumns;
    this.#logics = [];
    this.#materializations = [];
    this.#columns = [];
    this.#dependencies = [];
    this.#dashboards = [];
    this.#matDefinitionCatalog = [];
    this.#lineage = undefined;
    this.#lastQueryDependency = undefined;
    this.#targetOrganizationId = '';
    this.#jwt = '';
    this.#isSystemInternal = false;
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

    this.#lineage = Lineage.create({
      id: new ObjectId().toHexString(),
      organizationId: this.#targetOrganizationId,
    });

    if (!(lineageId && lineageCreatedAt))
      await this.#lineageRepo.insertOne(this.#lineage, this.#dbConnection);
  };

  /* Sends sql to parse SQL microservices and receives parsed SQL logic back */
  #parseLogic = async (sql: string): Promise<string> => {
    const parseSQLResult: ParseSQLResponseDto = await this.#parseSQL.execute({
      dialect: 'snowflake',
      sql,
    });

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
        targetOrganizationId: this.#targetOrganizationId,
      },
      { isSystemInternal: this.#isSystemInternal },
      this.#dbConnection
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
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        { isSystemInternal: this.#isSystemInternal },
        this.#dbConnection
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

  #generateDbtModel = async (
    model: any,
    modelManifest: any,
    dependentOn: MaterializationDefinition[],
    catalogFile: string
  ): Promise<void> => {
    if (!this.#lineage)
      throw new ReferenceError('Lineage property is undefined');
    const lineage = this.#lineage;

    const sql = modelManifest.compiled_sql;

    const parsedLogic = await this.#parseLogic(sql);

    const createLogicResult = await this.#createLogic.execute(
      {
        dbtModelId: model.unique_id,
        sql,
        modelName: model.metadata.name,
        dependentOn,
        lineageId: lineage.id,
        parsedLogic,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
        catalogFile,
      },
      { isSystemInternal: this.#isSystemInternal },
      this.#dbConnection
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
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        { isSystemInternal: this.#isSystemInternal },
        this.#dbConnection
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
  #getDbtResources = (file: string): DbtResources => {
    const data = file;

    const catalog = JSON.parse(data);

    const { nodes } = catalog;
    const { sources } = catalog;
    // eslint-disable-next-line @typescript-eslint/naming-convention
    const { parent_map } = catalog;

    return { nodes, sources, parent_map };
  };

  /* Runs through dbt nodes and creates objects like logic, materializations and columns */
  #generateWarehouseResources = async (
    catalog: any,
    manifest: any
  ): Promise<void> => {
    const dbtCatalogResources = this.#getDbtResources(catalog);
    const dbtManifestResources = this.#getDbtResources(manifest);

    const dbtSourceKeys = Object.keys(dbtCatalogResources.sources);

    dbtSourceKeys.forEach((key) => {
      const source = dbtCatalogResources.sources[key];

      const matCatalogElement = {
        dbtModelId: key,
        materializationName: source.metadata.name,
        schemaName: source.metadata.schema,
        databaseName: source.metadata.database,
      };

      this.#matDefinitionCatalog.push(matCatalogElement);
    });

    await Promise.all(
      dbtSourceKeys.map(async (key) =>
        this.#generateWarehouseSource(dbtCatalogResources.sources[key])
      )
    );

    const dbtModelKeys = Object.keys(dbtCatalogResources.nodes);

    if (!dbtModelKeys.length) throw new ReferenceError('No dbt models found');

    dbtModelKeys.forEach((key) => {
      const model = dbtCatalogResources.nodes[key];

      const matCatalogElement = {
        dbtModelId: key,
        materializationName: model.metadata.name,
        schemaName: model.metadata.schema,
        databaseName: model.metadata.database,
      };

      this.#matDefinitionCatalog.push(matCatalogElement);
    });

    await Promise.all(
      dbtModelKeys.map(async (key) => {
        const dependsOn: string[] = [
          ...new Set(dbtManifestResources.parent_map[key]),
        ];

        const dependentOn = this.#matDefinitionCatalog.filter((element) =>
          dependsOn.includes(element.dbtModelId)
        );

        if (dependsOn.length !== dependentOn.length)
          throw new RangeError('materialization dependency mismatch');

        return this.#generateDbtModel(
          dbtCatalogResources.nodes[key],
          dbtManifestResources.nodes[key],
          dependentOn,
          catalog
        );
      })
    );
  };

  // /* Identifies the statement root (e.g. create_materialization_statement.select_statement) of a specific reference path */
  // #getStatementRoot = (path: string): string => {
  //   const lastIndexStatementRoot = path.lastIndexOf(SQLElement.STATEMENT);
  //   if (lastIndexStatementRoot === -1 || !lastIndexStatementRoot)
  //     // todo - inconsistent usage of Error types. Sometimes Range and sometimes Reference
  //     throw new RangeError('Statement root not found for column reference');

  //   return path.slice(0, lastIndexStatementRoot + SQLElement.STATEMENT.length);
  // };

  /* Checks if parent dependency can be mapped on the provided self column or to another column of the self materialization. */
  // #isDependencyOfTarget = (
  //   potentialDependency: ColumnRef,
  //   selfRef: ColumnRef
  // ): boolean => {
  //   const dependencyStatementRoot = this.#getStatementRoot(
  //     potentialDependency.context.path
  //   );
  //   const selfStatementRoot = this.#getStatementRoot(selfRef.context.path);

  //   const isStatementDependency =
  //     !potentialDependency.context.path.includes(SQLElement.INSERT_STATEMENT) &&
  //     !potentialDependency.context.path.includes(
  //       SQLElement.COLUMN_DEFINITION
  //     ) &&
  //     dependencyStatementRoot === selfStatementRoot &&
  //     (potentialDependency.context.path.includes(SQLElement.COLUMN_REFERENCE) ||
  //       potentialDependency.context.path.includes(
  //         SQLElement.WILDCARD_IDENTIFIER
  //       ));

  //   if (!isStatementDependency) return false;

  //   const isSelfSelectStatement = selfStatementRoot.includes(
  //     SQLElement.SELECT_STATEMENT
  //   );

  //   const isWildcardRef =
  //     isSelfSelectStatement && potentialDependency.isWildcardRef;
  //   const isSameName =
  //     isSelfSelectStatement && selfRef.name === potentialDependency.name;
  //   const isGroupBy =
  //     potentialDependency.context.path.includes(SQLElement.GROUPBY_CLAUSE) &&
  //     selfRef.name !== potentialDependency.name;
  //   const isJoinOn =
  //     potentialDependency.context.path.includes(SQLElement.JOIN_ON_CONDITION) &&
  //     selfRef.name !== potentialDependency.name;

  //   if (isWildcardRef || isSameName || isGroupBy) return true;

  //   if (isJoinOn) return false;
  //   if (potentialDependency.name !== selfRef.name) return false;

  //   throw new RangeError(
  //     'Unhandled case when checking if is dependency of target'
  //   );
  // };

  #retrieveQueryHistory = async (biLayer: BiLayer): Promise<any> => {
    const queryHistoryResult: QueryHistoryResponseDto =
      await this.#querySnowflakeHistory.execute(
        { biLayer, limit: 10 },
        { jwt: this.#jwt }
      );
    
    if (!queryHistoryResult.success) throw new Error(queryHistoryResult.error);
    if (!queryHistoryResult.value)
      throw new SyntaxError(`Retrival of query history failed`);

    return queryHistoryResult.value;
  };

  /* Get all relevant dashboards that are data dependency to self materialization */
  #getDashboardDataDependencyRefs = async (
    statementRefs: Refs,
    queryHistory: any,
    biLayer: BiLayer
  ): Promise<DashboardRef[]> => {
    const dependentDashboards: DashboardRef[] = [];

    statementRefs.columns.forEach((column) => {
      queryHistory.forEach((entry: any) => {
        const sqlText: string = entry.QUERY_TEXT;

        const testUrl = sqlText.match(/"(https?:[^\s]+),/);
        const dashboardUrl = testUrl ? testUrl[1] : `${biLayer} dashboard: ${new ObjectId().toHexString()}`;

        const matName = column.materializationName.toUpperCase();
        const colName = column.alias
          ? column.alias.toUpperCase()
          : column.name.toUpperCase();

        if (sqlText.includes(matName) && sqlText.includes(colName)) {
          dependentDashboards.push({
            url: dashboardUrl,
            materializationName: matName,
            columnName: colName,
          });
        }
      });
    });
    return dependentDashboards;
  };

  /* Get all relevant wildcard statement references that are data dependency to self materialization */
  #getWildcardDataDependencyRefs = (statementRefs: Refs): ColumnRef[] =>
    statementRefs.wildcards.filter(
      (wildcard) => wildcard.dependencyType === DependencyType.DATA
    );

  /* Get all relevant column statement references that are data dependency to self materialization */
  #getColDataDependencyRefs = (statementRefs: Refs): ColumnRef[] => {
    let dataDependencyRefs = statementRefs.columns.filter(
      (column) =>
        column.dependencyType === DependencyType.DATA &&
        !column.isCompoundValueRef
    );

    const setColumnRefs = dataDependencyRefs.filter((ref) =>
      ref.context.path.includes(SQLElement.SET_EXPRESSION)
    );

    const uniqueSetColumnRefs = setColumnRefs.filter(
      (value, index, self) =>
        index ===
        self.findIndex(
          (ref) =>
            ref.name === value.name &&
            ref.context.path === value.context.path &&
            ref.materializationName === value.materializationName
        )
    );

    const columnRefs = dataDependencyRefs.filter(
      (ref) => !ref.context.path.includes(SQLElement.SET_EXPRESSION)
    );

    dataDependencyRefs = uniqueSetColumnRefs.concat(columnRefs);

    // const withColumnRefs = dataDependencyRefs.filter(
    //   (ref) =>
    //     ref.context.path.includes(SQLElement.WITH_COMPOUND_STATEMENT) &&
    //     !ref.context.path.includes(SQLElement.COMMON_TABLE_EXPRESSION)
    // );
    // columnRefs = dataDependencyRefs.filter(
    //   (ref) => !ref.context.path.includes(SQLElement.WITH_COMPOUND_STATEMENT)
    // );

    // dataDependencyRefs = withColumnRefs.concat(columnRefs);

    return dataDependencyRefs;
  };

  #buildDashboardRefDependency = async (
    dashboardRef: DashboardRef,
    dbtModelId: string,
    parentDbtModelIds: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;
    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const lineageId = lineage.id;
    const dbtModelIdElements = dbtModelId.split('.');
    if (dbtModelIdElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const materialization = await this.#materializationRepo.findBy(
      {
        name: dashboardRef.materializationName,
        dbtModelId: parentDbtModelIds[0],
        lineageId,
        organizationId: this.#targetOrganizationId,
      },
      this.#dbConnection
    );
    const materializationId = materialization[0].id;

    const column = await this.#columnRepo.findBy(
      {
        name: dashboardRef.columnName,
        materializationId,
        lineageId,
        organizationId: this.#targetOrganizationId,
      },
      this.#dbConnection
    );
    const columnId = column[0].id;

    const createDashboardResult = await this.#createDashboard.execute(
      {
        columnId,
        columnName: dashboardRef.columnName,
        lineageId,
        materializationId,
        materializationName: dashboardRef.materializationName,
        url: dashboardRef.url,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      { isSystemInternal: this.#isSystemInternal },
      this.#dbConnection
    );

    if (!createDashboardResult.success)
      throw new Error(createDashboardResult.error);
    if (!createDashboardResult.value)
      throw new Error('Creating dashboard failed');

    const dashboard = createDashboardResult.value;

    this.#dashboards.push(dashboard);

    const createExternalDependencyResult =
      await this.#createExternalDependency.execute(
        {
          dashboard,
          lineageId: lineage.id,
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        { isSystemInternal: this.#isSystemInternal },
        this.#dbConnection
      );

    if (!createExternalDependencyResult.success)
      throw new Error(createExternalDependencyResult.error);
    if (!createExternalDependencyResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependency = createExternalDependencyResult.value;
    this.#dependencies.push(dependency);
  };

  /* Creates dependency for specific wildcard ref */
  #buildWildcardRefDependency = async (
    dependencyRef: ColumnRef,
    dbtModelId: string,
    parentDbtModelIds: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const dbtModelIdElements = dbtModelId.split('.');
    if (dbtModelIdElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const columnDependencyRefs = await this.#getDependenciesForWildcard(
      dependencyRef
    );

    // const isCreateDependencyResponse = (
    //   item: CreateDependencyResponse | null
    // ): item is CreateDependencyResponse => !!item;

    const createDependencyResults = await Promise.all(
      columnDependencyRefs.map(
        async (dependency): Promise<CreateDependencyResponse> => {
          // if (this.#columnRefIsEqual(dependency, this.#lastQueryDependency))
          //   return null;

          // if (dependency.dependencyType === DependencyType.QUERY)
          //   this.#lastQueryDependency = dependency;

          const createDependencyResult = await this.#createDependency.execute(
            {
              dependencyRef: dependency,
              selfDbtModelId: dbtModelId,
              parentDbtModelIds,
              lineageId: lineage.id,
              targetOrganizationId: this.#targetOrganizationId,
              writeToPersistence: false,
            },
            { isSystemInternal: this.#isSystemInternal },
            this.#dbConnection
          );

          return createDependencyResult;
        }
      )
    );

    // const onlyCreateDependencyResults = createDependencyResults.filter(
    //   isCreateDependencyResponse
    // );

    if (createDependencyResults.some((result) => !result.success)) {
      const errorResults = createDependencyResults.filter(
        (result) => result.error
      );
      throw new Error(errorResults[0].error);
    }

    if (createDependencyResults.some((result) => !result.value))
      throw new SyntaxError(`Creation of dependencies failed`);

    const isValue = (item: Dependency | undefined): item is Dependency =>
      !!item;

    const values = createDependencyResults
      .map((result) => result.value)
      .filter(isValue);

    this.#dependencies.push(...values);
  };

  /* Creates dependency for specific column ref */
  #buildColumnRefDependency = async (
    dependencyRef: ColumnRef,
    dbtModelId: string,
    parentDbtModelIds: string[]
  ): Promise<void> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const dbtModelIdElements = dbtModelId.split('.');
    if (dbtModelIdElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const createDependencyResult = await this.#createDependency.execute(
      {
        dependencyRef,
        selfDbtModelId: dbtModelId,
        parentDbtModelIds,
        lineageId: lineage.id,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      { isSystemInternal: this.#isSystemInternal },
      this.#dbConnection
    );

    if (!createDependencyResult.success)
      throw new Error(createDependencyResult.error);
    if (!createDependencyResult.value)
      throw new ReferenceError(`Creating dependency failed`);

    const dependency = createDependencyResult.value;

    this.#dependencies.push(dependency);
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependencies = async (biType?: string): Promise<void> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    const biLayer = biType as BiLayer || BiLayer.mode;
    const queryHistory = await this.#retrieveQueryHistory(biLayer);
    await Promise.all(
      this.#logics.map(async (logic) => {
        const colDataDependencyRefs = this.#getColDataDependencyRefs(
          logic.statementRefs
        );

        await Promise.all(
          colDataDependencyRefs.map(async (dependencyRef) =>
            this.#buildColumnRefDependency(
              dependencyRef,
              logic.dbtModelId,
              logic.dependentOn.map((element) => element.dbtModelId)
            )
          )
        );

        const wildcardDataDependencyRefs = this.#getWildcardDataDependencyRefs(
          logic.statementRefs
        );

        await Promise.all(
          wildcardDataDependencyRefs.map(async (dependencyRef) =>
            this.#buildWildcardRefDependency(
              dependencyRef,
              logic.dbtModelId,
              logic.dependentOn.map((element) => element.dbtModelId)
            )
          )
        );

        const dashboardDataDependencyRefs =
          await this.#getDashboardDataDependencyRefs(
            logic.statementRefs,
            queryHistory,
            biLayer
          );

        const uniqueDashboardRefs = dashboardDataDependencyRefs.filter(
          (value, index, self) =>
            index ===
            self.findIndex(
              (dashboard) =>
                dashboard.name === value.name &&
                dashboard.columnName === value.columnName &&
                dashboard.materializationName === value.materializationName
            )
        );

        await Promise.all(
          uniqueDashboardRefs.map(async (dashboardRef) =>
            this.#buildDashboardRefDependency(
              dashboardRef,
              logic.dbtModelId,
              logic.dependentOn.map((element) => element.dbtModelId)
            )
          )
        );
      })
    );
  };

  #getDependenciesForWildcard = async (
    dependencyRef: ColumnRef
  ): Promise<ColumnRef[]> => {
    const lineage = this.#lineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const catalogMatches = this.#matDefinitionCatalog.filter((dependency) => {
      const nameIsEqual =
        dependencyRef.materializationName === dependency.materializationName;

      const schemaNameIsEqual =
        !dependencyRef.schemaName ||
        dependencyRef.schemaName === dependency.schemaName;

      const databaseNameIsEqual =
        !dependencyRef.databaseName ||
        dependencyRef.databaseName === dependency.databaseName;

      return nameIsEqual && schemaNameIsEqual && databaseNameIsEqual;
    });

    if (catalogMatches.length !== 1)
      throw new RangeError(
        'Inconsistencies in materialization dependency catalog'
      );

    const { dbtModelId } = catalogMatches[0];

    const readColumnsResult = await this.#readColumns.execute(
      {
        dbtModelId,
        lineageId: lineage.id,
        targetOrganizationId: this.#targetOrganizationId,
      },
      { isSystemInternal: this.#isSystemInternal },
      this.#dbConnection
    );

    if (!readColumnsResult.success) throw new Error(readColumnsResult.error);
    if (!readColumnsResult.value)
      throw new ReferenceError(`Reading of columns failed`);

    const colsFromWildcard = readColumnsResult.value;

    const dependencies = colsFromWildcard.map((column) => ({
      ...dependencyRef,
      name: column.name,
    }));

    return dependencies;
  };

  #writeWhResourcesToPersistence = async (): Promise<void> => {
    await this.#logicRepo.insertMany(this.#logics, this.#dbConnection);

    await this.#materializationRepo.insertMany(
      this.#materializations,
      this.#dbConnection
    );

    await this.#columnRepo.insertMany(this.#columns, this.#dbConnection);
  };

  #writeDashboardsToPersistence = async (): Promise<void> => {
    if (this.#dashboards.length > 0)
      await this.#dashboardRepo.insertMany(
        this.#dashboards,
        this.#dbConnection
      );
  };

  #writeDependenciesToPersistence = async (): Promise<void> => {
    if (this.#dependencies.length > 0)
      await this.#dependencyRepo.insertMany(
        this.#dependencies,
        this.#dbConnection
      );
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateLineageResponseDto> {
    try {
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      this.#targetOrganizationId = request.targetOrganizationId;
      this.#jwt = auth.jwt;
      this.#isSystemInternal = auth.isSystemInternal;

      // todo - Workaround. Fix ioc container
      this.#lineage = undefined;
      this.#logics = [];
      this.#materializations = [];
      this.#columns = [];
      this.#dependencies = [];
      this.#matDefinitionCatalog = [];
      this.#lastQueryDependency = undefined;

      await this.#buildLineage(request.lineageId, request.lineageCreatedAt);

      await this.#generateWarehouseResources(request.catalog, request.manifest);

      await this.#writeWhResourcesToPersistence();

      await this.#buildDependencies(request.biType);

      await this.#writeDashboardsToPersistence();

      await this.#writeDependenciesToPersistence();

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
