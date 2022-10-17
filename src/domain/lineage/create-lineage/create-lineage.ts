// todo - Clean Architecture dependency violation. Fix
import { ObjectId } from 'mongodb';
import Result from '../../value-types/transient-types/result';
import IUseCase from '../../services/use-case';
import SQLElement from '../../value-types/sql-element';
import { CreateColumn } from '../../column/create-column';
import {
  CreateMaterialization,
  CreateMaterializationRequestDto,
} from '../../materialization/create-materialization';
import { CreateLogic } from '../../logic/create-logic';
import { ParseSQL, ParseSQLResponseDto } from '../../sql-parser-api/parse-sql';
import { Lineage } from '../../entities/lineage';
import {
  Logic,
  ColumnRef,
  Refs,
  MaterializationDefinition,
  DashboardRef,
} from '../../entities/logic';
import {
  CreateDependency,
  CreateDependencyResponse,
} from '../../dependency/create-dependency';
import { Dependency, DependencyType } from '../../entities/dependency';
import { ReadColumns } from '../../column/read-columns';
import {
  Materialization,
  MaterializationType,
} from '../../entities/materialization';
import { Column } from '../../entities/column';
import { ILineageRepo } from '../i-lineage-repo';
import { IColumnRepo } from '../../column/i-column-repo';
import { IMaterializationRepo } from '../../materialization/i-materialization-repo';
import { IDependencyRepo } from '../../dependency/i-dependency-repo';
import { ILogicRepo } from '../../logic/i-logic-repo';
import { DbConnection } from '../../services/i-db';
import {
  QuerySnowflakeHistory,
  QueryHistoryResponseDto,
} from '../../query-snowflake-history-api/query-snowflake-history';
import { Dashboard } from '../../entities/dashboard';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { IDashboardRepo } from '../../dashboard/i-dashboard-repo';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import { BiLayer, parseBiLayer } from '../../value-types/bilayer';
import { buildLineage } from './build-lineage';

export interface CreateLineageRequestDto {
  targetOrganizationId?: string;
  catalog: string;
  manifest: string;
  biType?: string;
}

export interface CreateLineageAuthDto {
  jwt: string;
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateLineageResponseDto = Result<Lineage>;



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

  #newLineage?: Lineage;

  #newLogics: Logic[];

  #oldLogics: { [key: string]: Logic[] };

  #logicsToUpdate: Logic[];

  #logicsToCreate: Logic[];

  #newMaterializations: Materialization[];

  #matsToUpdate: Materialization[];

  #matsToCreate: Materialization[];

  #newColumns: Column[];

  #oldColumns: { [key: string]: Column[] };

  #columnsToUpdate: Column[];

  #columnsToCreate: Column[];

  #newDependencies: Dependency[];

  #oldDependencies: Dependency[];

  #depencenciesToUpdate: Dependency[];

  #depencenciesToCreate: Dependency[];

  #newDashboards: Dashboard[];

  #oldDashboards: { [key: string]: Dashboard[] };

  #dashboardsToUpdate: Dashboard[];

  #dashboardsToCreate: Dashboard[];

  #lastQueryDependency?: ColumnRef;

  #newMatDefinitionCatalog: MaterializationDefinition[];

  #targetOrganizationId?: string;

  #callerOrganizationId?: string;

  #organizationId: string;

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
    this.#newLogics = [];
    this.#newMaterializations = [];
    this.#newColumns = [];
    this.#newDependencies = [];
    this.#newDashboards = [];
    this.#newMatDefinitionCatalog = [];
    this.#newLineage = undefined;
    this.#lastQueryDependency = undefined;
    this.#targetOrganizationId = '';
    this.#callerOrganizationId = '';
    this.#organizationId = '';
    this.#jwt = '';
    this.#isSystemInternal = false;
  }



  

  



 

  

 



  



  #updateMatRelatedResources = (props: {
    oldMatProps: { matId: string; relationName: string; lineageId: string };
    newMat: Materialization;
  }) => {
    const createMatResult = await this.#createMaterialization.execute(
      {
        ...props.newMat.toDto(),
        id: props.oldMatProps.matId,
        relationName: props.oldMatProps.relationName,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createMatResult.success) throw new Error(createMatResult.error);
    if (!createMatResult.value)
      throw new Error('Create Mat failed - Unknown error');

    this.#matsToUpdate.push(createMatResult.value);
  };

  #groupByMatId = <T extends { materializationId: string }>(
    accumulation: { [key: string]: T[] },
    element: T
  ): { [key: string]: T[] } => {
    const localAcc = accumulation;

    const key = element.materializationId;
    if (!(key in accumulation)) {
      localAcc[key] = [];
    }
    localAcc[key].push(element);
    return localAcc;
  };

  #mergeWithLatestSnapshot = async (): Promise<void> => {
    // todo- needs to retrieve latest compeleted lineage
    const latestLineage = await this.#lineageRepo.findLatest(
      this.#dbConnection,
      this.#organizationId
    );

    if (!latestLineage) return;

    const latestMats = await this.#materializationRepo.findBy(
      { lineageIds: [latestLineage.id], organizationId: this.#organizationId },
      this.#dbConnection
    );

    await Promise.all(
      this.#newMaterializations.map(async (newMat) => {
        const matchingMat = latestMats.find(
          (oldMat) => newMat.relationName === oldMat.relationName
        );

        if (matchingMat) {
          if (!Object.keys(this.#oldColumns).length)
            this.#oldColumns = (
              await this.#columnRepo.findBy(
                {
                  lineageIds: [latestLineage.id],
                  organizationId: this.#organizationId,
                },
                this.#dbConnection
              )
            ).reduce(this.#groupByMatId, {});

          if (!Object.keys(this.#oldLogics).length)
            this.#oldColumns = (
              await this.#columnRepo.findBy(
                {
                  lineageIds: [latestLineage.id],
                  organizationId: this.#organizationId,
                },
                this.#dbConnection
              )
            ).reduce(this.#groupByMatId, {});

          await this.#updateMatRelatedResources;
        } else {
          // todo - also logic, ...
          this.#matsToCreate.push(newMat);
        }
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
        {
          biLayer,
          limit: 10,
          targetOrganizationId: this.#targetOrganizationId,
        },
        { jwt: this.#jwt, callerOrganizationId: this.#callerOrganizationId }
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
      queryHistory[Object.keys(queryHistory)[0]].forEach((entry: any) => {
        const sqlText: string = entry.QUERY_TEXT;

        const testUrl = sqlText.match(/"(https?:[^\s]+),/);
        const dashboardUrl = testUrl
          ? testUrl[1]
          : `${biLayer} dashboard: ${new ObjectId().toHexString()}`;

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
            this.#insensitiveEquality(ref.name, value.name) &&
            this.#insensitiveEquality(ref.context.path, value.context.path) &&
            this.#insensitiveEquality(
              ref.materializationName,
              value.materializationName
            )
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
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#newLineage;
    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const lineageId = lineage.id;
    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const materialization = await this.#materializationRepo.findBy(
      {
        name: dashboardRef.materializationName,
        relationName: parentRelationNames[0],
        lineageIds: [lineageId],
        organizationId: this.#organizationId,
      },
      this.#dbConnection
    );
    const materializationId = materialization[0].id;

    const column = await this.#columnRepo.findBy(
      {
        name: dashboardRef.columnName,
        materializationId,
        lineageIds: [lineageId],
        organizationId: this.#organizationId,
      },
      this.#dbConnection
    );
    const columnId = column[0].id;

    const createDashboardResult = await this.#createDashboard.execute(
      {
        columnId,
        columnName: dashboardRef.columnName,
        lineageIds: [lineageId],
        materializationId,
        materializationName: dashboardRef.materializationName,
        url: dashboardRef.url,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createDashboardResult.success)
      throw new Error(createDashboardResult.error);
    if (!createDashboardResult.value)
      throw new Error('Creating dashboard failed');

    const dashboard = createDashboardResult.value;

    this.#newDashboards.push(dashboard);

    const createExternalDependencyResult =
      await this.#createExternalDependency.execute(
        {
          dashboard,
          lineageIds: [lineage.id],
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        {
          isSystemInternal: this.#isSystemInternal,
          callerOrganizationId: this.#callerOrganizationId,
        },
        this.#dbConnection
      );

    if (!createExternalDependencyResult.success)
      throw new Error(createExternalDependencyResult.error);
    if (!createExternalDependencyResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependency = createExternalDependencyResult.value;
    this.#newDependencies.push(dependency);
  };

  /* Creates dependency for specific wildcard ref */
  #buildWildcardRefDependency = async (
    dependencyRef: ColumnRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#newLineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
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
              selfRelationName: relationName,
              parentRelationNames,
              lineageIds: [lineage.id],
              targetOrganizationId: this.#targetOrganizationId,
              writeToPersistence: false,
            },
            {
              isSystemInternal: this.#isSystemInternal,
              callerOrganizationId: this.#callerOrganizationId,
            },
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
      console.warn(`Fix. Creation of dependencies failed. Skipped for now.`);
    // throw new SyntaxError(`Creation of dependencies failed`);

    const isValue = (item: Dependency | undefined): item is Dependency =>
      !!item;

    const values = createDependencyResults
      .map((result) => result.value)
      .filter(isValue);

    this.#newDependencies.push(...values);
  };

  /* Creates dependency for specific column ref */
  #buildColumnRefDependency = async (
    dependencyRef: ColumnRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const lineage = this.#newLineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const createDependencyResult = await this.#createDependency.execute(
      {
        dependencyRef,
        selfRelationName: relationName,
        parentRelationNames,
        lineageIds: [lineage.id],
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
      this.#dbConnection
    );

    if (!createDependencyResult.success)
      throw new Error(createDependencyResult.error);
    if (!createDependencyResult.value) {
      console.warn(`Creating dependency failed`);
      return;
    }
    // throw new ReferenceError(`Creating dependency failed`);

    const dependency = createDependencyResult.value;

    this.#newDependencies.push(dependency);
  };

  /* Creates all dependencies that exist between DWH resources */
  #buildDependencies = async (biType?: string): Promise<void> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    let biLayer: BiLayer | undefined;
    let queryHistory: any | undefined;

    if (biType) {
      biLayer = parseBiLayer(biType);
      queryHistory = await this.#retrieveQueryHistory(biLayer);
    }

    await Promise.all(
      this.#newLogics.map(async (logic) => {
        const colDataDependencyRefs = this.#getColDataDependencyRefs(
          logic.statementRefs
        );
        await Promise.all(
          colDataDependencyRefs.map(async (dependencyRef) =>
            this.#buildColumnRefDependency(
              dependencyRef,
              logic.relationName,
              logic.dependentOn.dbtDependencyDefinitions
                .concat(logic.dependentOn.dwDependencyDefinitions)
                .map((element) => element.relationName)
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
              logic.relationName,
              logic.dependentOn.dbtDependencyDefinitions
                .concat(logic.dependentOn.dwDependencyDefinitions)
                .map((element) => element.relationName)
            )
          )
        );

        if (biLayer && queryHistory) {
          const dashboardDataDependencyRefs =
            await this.#getDashboardDataDependencyRefs(
              logic.statementRefs,
              queryHistory,
              biLayer
            );

          const uniqueDashboardRefs = dashboardDataDependencyRefs.filter(
            (value, index, self) =>
              index ===
              self.findIndex((dashboard) =>
                typeof dashboard.name === 'string' &&
                typeof value.name === 'string'
                  ? this.#insensitiveEquality(dashboard.name, value.name)
                  : dashboard.name === value.name &&
                    this.#insensitiveEquality(
                      dashboard.columnName,
                      value.columnName
                    ) &&
                    this.#insensitiveEquality(
                      dashboard.materializationName,
                      value.materializationName
                    )
              )
          );

          await Promise.all(
            uniqueDashboardRefs.map(async (dashboardRef) =>
              this.#buildDashboardRefDependency(
                dashboardRef,
                logic.relationName,
                logic.dependentOn.dbtDependencyDefinitions.map(
                  (element) => element.relationName
                )
              )
            )
          );
        }
      })
    );
  };

  #getDependenciesForWildcard = async (
    dependencyRef: ColumnRef
  ): Promise<ColumnRef[]> => {
    const lineage = this.#newLineage;

    if (!lineage) throw new ReferenceError('Lineage property is undefined');

    const catalogMatches = this.#newMatDefinitionCatalog.filter(
      (dependency) => {
        const nameIsEqual = this.#insensitiveEquality(
          dependencyRef.materializationName,
          dependency.materializationName
        );

        const schemaNameIsEqual =
          !dependencyRef.schemaName ||
          (typeof dependencyRef.schemaName === 'string' &&
          typeof dependency.schemaName === 'string'
            ? this.#insensitiveEquality(
                dependencyRef.schemaName,
                dependency.schemaName
              )
            : dependencyRef.schemaName === dependency.schemaName);

        const databaseNameIsEqual =
          !dependencyRef.databaseName ||
          (typeof dependencyRef.databaseName === 'string' &&
          typeof dependency.databaseName === 'string'
            ? this.#insensitiveEquality(
                dependencyRef.databaseName,
                dependency.databaseName
              )
            : dependencyRef.databaseName === dependency.databaseName);

        return nameIsEqual && schemaNameIsEqual && databaseNameIsEqual;
      }
    );

    if (catalogMatches.length !== 1) {
      console.warn(
        'todo - fix. Error in wildcard dependency generation. Skipped for now'
      );
      return [];
      //   throw new RangeError(
      //   'Inconsistencies in materialization dependency catalog'
      // );
    }

    const { relationName } = catalogMatches[0];

    const readColumnsResult = await this.#readColumns.execute(
      {
        relationName,
        lineageIds: [lineage.id],
        targetOrganizationId: this.#targetOrganizationId,
      },
      {
        isSystemInternal: this.#isSystemInternal,
        callerOrganizationId: this.#callerOrganizationId,
      },
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
    if (!this.#newLineage)
      throw new ReferenceError(
        'Lineage object does not exist. Cannot write to persistence'
      );

    await this.#logicRepo.insertMany(this.#newLogics, this.#dbConnection);

    await this.#materializationRepo.insertMany(
      this.#newMaterializations,
      this.#dbConnection
    );

    await this.#columnRepo.insertMany(this.#newColumns, this.#dbConnection);
  };

  // todo - updateDashboards

  #writeDashboardsToPersistence = async (): Promise<void> => {
    if (this.#newDashboards.length > 0)
      await this.#dashboardRepo.insertMany(
        this.#newDashboards,
        this.#dbConnection
      );
  };

  #writeDependenciesToPersistence = async (): Promise<void> => {
    if (this.#newDependencies.length > 0)
      await this.#dependencyRepo.insertMany(
        this.#newDependencies,
        this.#dbConnection
      );
  };

  async execute(
    request: CreateLineageRequestDto,
    auth: CreateLineageAuthDto,
    dbConnection: DbConnection
  ): Promise<CreateLineageResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#dbConnection = dbConnection;

      this.#targetOrganizationId = request.targetOrganizationId;

      this.#callerOrganizationId = auth.callerOrganizationId;

      if (auth.callerOrganizationId)
        this.#organizationId = auth.callerOrganizationId;
      else if (request.targetOrganizationId)
        this.#organizationId = request.targetOrganizationId;
      else throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      this.#jwt = auth.jwt;
      this.#isSystemInternal = auth.isSystemInternal;

      // todo - Workaround. Fix ioc container
      this.#newLineage = undefined;

      this.#newLogics = [];
      this.#logicsToUpdate = [];
      this.#logicsToCreate = [];

      this.#newMaterializations = [];
      this.#matsToUpdate = [];
      this.#matsToCreate = [];

      this.#newColumns = [];
      this.#columnsToUpdate = [];
      this.#columnsToCreate = [];

      this.#newDependencies = [];
      this.#depencenciesToUpdate = [];
      this.#depencenciesToCreate = [];

      this.#dashboardsToUpdate = [];
      this.#dashboardsToCreate = [];

      this.#newMatDefinitionCatalog = [];
      this.#lastQueryDependency = undefined;

      console.log('starting lineage creation...');

      console.log('...building lineage object');
      this.#newLineage = buildLineage(this.#organizationId);

      console.log('...generating warehouse resources');
      await this.#generateWarehouseResources(request.catalog, request.manifest);

      console.log('...merging new lineage snapshot with last one');
      await this.#mergeWithLatestSnapshot();

      console.log('...writing dw resources to persistence');
      await this.#writeWhResourcesToPersistence(
      );

      console.log('...building dependencies');
      await this.#buildDependencies(request.biType);

      console.log('...writing dashboards to persistence');
      await this.#writeDashboardsToPersistence();

      console.log('...writing dependencies to persistence');
      await this.#writeDependenciesToPersistence();

      // todo - updateLineage;

      if (!this.#newLineage)
        throw new ReferenceError('Lineage property is undefined');

      console.log('finished lineage creation.');

      return Result.ok(this.#newLineage);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  
}
