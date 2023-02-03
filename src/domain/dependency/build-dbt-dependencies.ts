// todo clean architecture violation
import { v4 as uuidv4 } from 'uuid';
import { ReadColumns } from '../column/read-columns';
import { CreateDashboard } from '../dashboard/create-dashboard';
import {
  CreateDependency,
  CreateDependencyResponse,
} from './create-dependency';
import { CreateExternalDependency } from './create-external-dependency';
import { Column } from '../entities/column';
import { Dashboard } from '../entities/dashboard';
import { Dependency } from '../entities/dependency';
import {
  ColumnRef,
  DashboardRef,
  Logic,
  ModelRepresentation,
  Refs,
} from '../entities/logic';
import { Materialization } from '../entities/materialization';
import {
  QuerySfQueryHistory,
  QuerySfQueryHistoryResponseDto,
} from '../snowflake-api/query-snowflake-history';
import { BiToolType } from '../value-types/bi-tool';
import SQLElement from '../value-types/sql-element';
import {
  IConnectionPool,
  SnowflakeQueryResult,
} from '../snowflake-api/i-snowflake-api-repo';
import BaseAuth from '../services/base-auth';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';

export type Auth = BaseAuth;

export interface BuildResult {
  dashboards: Dashboard[];
  dependencies: Dependency[];
}

export interface BuildDbtDependenciesRequestDto {
  targetOrgId?: string;
  logics: Logic[];
  mats: Materialization[];
  columns: Column[];
  catalog: ModelRepresentation[];
  biToolType?: BiToolType;
}

export type BuildDbtDependenciesAuthDto = BaseAuth;

export type BuildDbtDependenciesResponse = Result<BuildResult>;

export class BuildDbtDependencies
  implements
    IUseCase<
      BuildDbtDependenciesRequestDto,
      BuildDbtDependenciesResponse,
      BuildDbtDependenciesAuthDto,
      IConnectionPool
    >
{
  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #readColumns: ReadColumns;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  #auth?: Auth;

  #targetOrgId?: string;

  #logics?: Logic[];

  #mats?: Materialization[];

  #columns?: Column[];

  #catalog?: ModelRepresentation[];

  #dependencies: Dependency[] = [];

  #dashboards: Dashboard[] = [];

  #connPool?: IConnectionPool;

  constructor(
    createDashboard: CreateDashboard,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    readColumns: ReadColumns,
    querySfQueryHistory: QuerySfQueryHistory
  ) {
    this.#createDashboard = createDashboard;
    this.#createDependency = createDependency;
    this.#createExternalDependency = createExternalDependency;
    this.#readColumns = readColumns;
    this.#querySfQueryHistory = querySfQueryHistory;
  }

  #retrieveQuerySfQueryHistory = async (
    biType: BiToolType
  ): Promise<SnowflakeQueryResult> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('connection pool or auth missing');

    const querySfQueryHistoryResult: QuerySfQueryHistoryResponseDto =
      await this.#querySfQueryHistory.execute(
        {
          biType,
          limit: 10,
          targetOrgId: this.#targetOrgId,
        },
        this.#auth,
        this.#connPool
      );

    if (!querySfQueryHistoryResult.success)
      throw new Error(querySfQueryHistoryResult.error);
    if (!querySfQueryHistoryResult.value)
      throw new SyntaxError(`Retrival of query history failed`);

    return querySfQueryHistoryResult.value;
  };

  /* Get all relevant dashboards that are data dependency to self materialization */
  static #getDashboardDataDependencyRefs = async (
    statementRefs: Refs,
    querySfQueryHistoryResult: SnowflakeQueryResult,
    biTool: BiToolType
  ): Promise<DashboardRef[]> => {
    const dependentDashboards: DashboardRef[] = [];

    statementRefs.columns.forEach((column) => {
      querySfQueryHistoryResult.forEach((entry) => {
        const queryText = entry.QUERY_TEXT;
        if (typeof queryText !== 'string')
          throw new Error('Retrieved bi layer query text not in string format');

        const testUrl = queryText.match(/"(https?:[^\s]+),/);
        const dashboardUrl = testUrl
          ? testUrl[1]
          : `${biTool} dashboard: ${uuidv4()}`;

        const matName = column.materializationName.toUpperCase();
        const colName = column.alias
          ? column.alias.toUpperCase()
          : column.name.toUpperCase();

        if (queryText.includes(matName) && queryText.includes(colName)) {
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
  static #getWildcardDataDependencyRefs = (statementRefs: Refs): ColumnRef[] =>
    statementRefs.wildcards.filter(
      (wildcard) => wildcard.dependencyType === 'data'
    );

  /* Get all relevant column statement references that are data dependency to self materialization */
  static #getColDataDependencyRefs = (statementRefs: Refs): ColumnRef[] => {
    let dataDependencyRefs = statementRefs.columns.filter(
      (column) => column.dependencyType === 'data' && !column.isCompoundValueRef
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
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    if (!this.#connPool || !this.#mats || !this.#columns || !this.#auth)
      throw new Error('Build dependency field values missing');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const materialization = this.#mats.find(
      (el) =>
        el.name === dashboardRef.materializationName &&
        el.relationName === parentRelationNames[0]
    );

    if (!materialization)
      throw new Error(
        'Dashboard ref dependency built failed; Error: Materialization not found'
      );

    const column = this.#columns.find(
      (el) =>
        el.name === dashboardRef.columnName &&
        el.materializationId === materialization.id
    );

    if (!column)
      throw new Error(
        'Dashboard ref dependency built failed; Error: Column not found'
      );

    const createDashboardResult = await this.#createDashboard.execute(
      {
        columnId: column.id,
        columnName: dashboardRef.columnName,
        materializationId: materialization.id,
        materializationName: dashboardRef.materializationName,
        url: dashboardRef.url,
        targetOrgId: this.#targetOrgId,
        writeToPersistence: false,
      },
      this.#auth,
      this.#connPool
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
          targetOrgId: this.#targetOrgId,
          writeToPersistence: false,
        },
        this.#auth,
        this.#connPool
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
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    const connPool = this.#connPool;
    const auth = this.#auth;

    if (!connPool || !auth) throw new Error('Connection pool or auth missing');

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
              targetOrgId: this.#targetOrgId,
              writeToPersistence: false,
            },
            auth,
            connPool
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

    this.#dependencies.push(...values);
  };

  /* Creates dependency for specific column ref */
  #buildColumnRefDependency = async (
    dependencyRef: ColumnRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Connection pool or auth missing');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const createDependencyResult = await this.#createDependency.execute(
      {
        dependencyRef,
        selfRelationName: relationName,
        parentRelationNames,
        targetOrgId: this.#targetOrgId,
        writeToPersistence: false,
      },
      this.#auth,
      this.#connPool
    );

    if (!createDependencyResult.success)
      throw new Error(createDependencyResult.error);
    if (!createDependencyResult.value) {
      console.warn(`Creating dependency failed`);
      return;
    }
    // throw new ReferenceError(`Creating dependency failed`);

    const dependency = createDependencyResult.value;

    this.#dependencies.push(dependency);
  };

  #getDependenciesForWildcard = async (
    dependencyRef: ColumnRef
  ): Promise<ColumnRef[]> => {
    if (!this.#connPool || !this.#catalog || !this.#auth)
      throw new Error('Connection pool or catalog missing');

    const catalogMatches = this.#catalog.filter((catalogEl) => {
      const nameIsEqual =
        dependencyRef.materializationName === catalogEl.materializationName;
      const schemaNameIsEqual =
        !dependencyRef.schemaName ||
        dependencyRef.schemaName === catalogEl.schemaName;

      const databaseNameIsEqual =
        !dependencyRef.databaseName ||
        dependencyRef.databaseName === catalogEl.databaseName;

      return nameIsEqual && schemaNameIsEqual && databaseNameIsEqual;
    });

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
        relationNames: [relationName],
        targetOrgId: this.#targetOrgId,
      },
      this.#auth,
      this.#connPool
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

  /* Creates all dependencies that exist between DWH resources */
  async execute(
    req: BuildDbtDependenciesRequestDto,
    auth: BuildDbtDependenciesAuthDto,
    connPool: IConnectionPool
  ): Promise<BuildDbtDependenciesResponse> {
    try {
      this.#connPool = connPool;
      this.#auth = auth;
      this.#mats = req.mats;
      this.#columns = req.columns;
      this.#logics = req.logics;
      this.#catalog = req.catalog;
      this.#targetOrgId = req.targetOrgId;

      const querySfQueryHistory: SnowflakeQueryResult | undefined =
        req.biToolType
          ? await this.#retrieveQuerySfQueryHistory(req.biToolType)
          : undefined;

      await Promise.all(
        this.#logics.map(async (logic) => {
          const colDataDependencyRefs =
            BuildDbtDependencies.#getColDataDependencyRefs(logic.statementRefs);
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

          const wildcardDataDependencyRefs =
            BuildDbtDependencies.#getWildcardDataDependencyRefs(
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

          if (req.biToolType && querySfQueryHistory) {
            const dashboardDataDependencyRefs =
              await BuildDbtDependencies.#getDashboardDataDependencyRefs(
                logic.statementRefs,
                querySfQueryHistory,
                req.biToolType
              );

            const uniqueDashboardRefs = dashboardDataDependencyRefs.filter(
              (value, index, self) =>
                index ===
                self.findIndex((dashboard) =>
                  typeof dashboard.name === 'string' &&
                  typeof value.name === 'string'
                    ? dashboard.name === value.name
                    : dashboard.name === value.name &&
                      dashboard.columnName === value.columnName &&
                      dashboard.materializationName ===
                        value.materializationName
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

      return Result.ok({
        dashboards: this.#dashboards,
        dependencies: this.#dependencies,
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.ok();
    }
  }
}
