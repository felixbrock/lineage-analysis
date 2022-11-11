import { v4 as uuidv4 } from 'uuid';
import { ReadColumns } from '../../column/read-columns';
import { CreateDashboard } from '../../dashboard/create-dashboard';
import {
  CreateDependency,
  CreateDependencyResponse,
} from '../../dependency/create-dependency';
import { CreateExternalDependency } from '../../dependency/create-external-dependency';
import { Column } from '../../entities/column';
import { Dashboard } from '../../entities/dashboard';
import { Dependency } from '../../entities/dependency';
import {
  ColumnRef,
  DashboardRef,
  Logic,
  ModelRepresentation,
  Refs,
} from '../../entities/logic';
import { Materialization } from '../../entities/materialization';
import {
  QuerySfQueryHistory,
  QuerySfQueryHistoryResponseDto,
} from '../../snowflake-api/query-snowflake-history';
import {  } from '../../services/i-db';
import { BiType } from '../../value-types/bilayer';
import SQLElement from '../../value-types/sql-element';
import { SnowflakeQueryResult } from '../../snowflake-api/i-snowflake-api-repo';

interface Auth {
  jwt: string;
  callerOrganizationId?: string;
  isSystemInternal: boolean;
}

export interface BuildResult {
  dashboards: Dashboard[];
  dependencies: Dependency[];
}

export default class DependenciesBuilder {
  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #readColumns: ReadColumns;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  readonly #auth: Auth;

  readonly #lineageId: string;

  readonly #targetOrganizationId?: string;

  readonly #logics: Logic[];

  readonly #mats: Materialization[];

  readonly #columns: Column[];

  readonly #catalog: ModelRepresentation[];

  #dependencies: Dependency[] = [];

  get dependencies(): Dependency[] {
    return this.#dependencies;
  }

  #dashboards: Dashboard[] = [];

  get dashboards(): Dashboard[] {
    return this.#dashboards;
  }

  constructor(
    props: {
      lineageId: string;
      targetOrganizationId?: string;
      organizationId: string;
      logics: Logic[];
      mats: Materialization[];
      columns: Column[];
      catalog: ModelRepresentation[];
    },
    auth: Auth,
    dependencies: {
      createDashboard: CreateDashboard;
      createDependency: CreateDependency;
      createExternalDependency: CreateExternalDependency;
      readColumns: ReadColumns;
      querySfQueryHistory: QuerySfQueryHistory;
    }
  ) {
    this.#createDashboard = dependencies.createDashboard;
    this.#createDependency = dependencies.createDependency;
    this.#createExternalDependency = dependencies.createExternalDependency;
    this.#readColumns = dependencies.readColumns;
    this.#querySfQueryHistory = dependencies.querySfQueryHistory;

    this.#auth = auth;

    this.#lineageId = props.lineageId;
    this.#targetOrganizationId = props.targetOrganizationId;
    this.#logics = props.logics;
    this.#mats = props.mats;
    this.#columns = props.columns;
    this.#catalog = props.catalog;
  }

  #retrieveQuerySfQueryHistory = async (
    biType: BiType
  ): Promise<SnowflakeQueryResult> => {
    const querySfQueryHistoryResult: QuerySfQueryHistoryResponseDto =
      await this.#querySfQueryHistory.execute(
        {
          biType,
          limit: 10,
          targetOrganizationId: this.#targetOrganizationId,
        },
        this.#auth
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
    biLayer: BiType
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
          : `${biLayer} dashboard: ${uuidv4()}`;

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
            DependenciesBuilder.#insensitiveEquality(ref.name, value.name) &&
            DependenciesBuilder.#insensitiveEquality(
              ref.context.path,
              value.context.path
            ) &&
            DependenciesBuilder.#insensitiveEquality(
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
    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const materialization = this.#mats.find(
      (el) =>
        el.name === dashboardRef.materializationName &&
        el.relationName === parentRelationNames[0] &&
        el.lineageIds.includes(this.#lineageId)
    );

    if (!materialization)
      throw new Error(
        'Dashboard ref dependency built failed; Error: Materialization not found'
      );

    const column = this.#columns.find(
      (el) =>
        el.name === dashboardRef.columnName &&
        el.materializationId === materialization.id &&
        el.lineageIds.includes(this.#lineageId)
    );

    if (!column)
      throw new Error(
        'Dashboard ref dependency built failed; Error: Column not found'
      );

    const createDashboardResult = await this.#createDashboard.execute(
      {
        columnId: column.id,
        columnName: dashboardRef.columnName,
        lineageId: this.#lineageId,
        materializationId: materialization.id,
        materializationName: dashboardRef.materializationName,
        url: dashboardRef.url,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      this.#auth,
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
          lineageId: this.#lineageId,
          targetOrganizationId: this.#targetOrganizationId,
          writeToPersistence: false,
        },
        this.#auth,
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
    const lineage = this.#lineageId;

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
              lineageId: this.#lineageId,
              targetOrganizationId: this.#targetOrganizationId,
              writeToPersistence: false,
            },
            this.#auth,
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
    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of dbt model id elements');

    const createDependencyResult = await this.#createDependency.execute(
      {
        dependencyRef,
        selfRelationName: relationName,
        parentRelationNames,
        lineageId: this.#lineageId,
        targetOrganizationId: this.#targetOrganizationId,
        writeToPersistence: false,
      },
      this.#auth
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
    const catalogMatches = this.#catalog.filter((catalogEl) => {
      const nameIsEqual = DependenciesBuilder.#insensitiveEquality(
        dependencyRef.materializationName,
        catalogEl.materializationName
      );

      const schemaNameIsEqual =
        !dependencyRef.schemaName ||
        (typeof dependencyRef.schemaName === 'string' &&
        typeof catalogEl.schemaName === 'string'
          ? DependenciesBuilder.#insensitiveEquality(
              dependencyRef.schemaName,
              catalogEl.schemaName
            )
          : dependencyRef.schemaName === catalogEl.schemaName);

      const databaseNameIsEqual =
        !dependencyRef.databaseName ||
        (typeof dependencyRef.databaseName === 'string' &&
        typeof catalogEl.databaseName === 'string'
          ? DependenciesBuilder.#insensitiveEquality(
              dependencyRef.databaseName,
              catalogEl.databaseName
            )
          : dependencyRef.databaseName === catalogEl.databaseName);

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
        relationName,
        lineageId: this.#lineageId,
        targetOrganizationId: this.#targetOrganizationId,
      },
      this.#auth
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
  build = async (biType?: BiType): Promise<BuildResult> => {
    // todo - should method be completely sync? Probably resolves once transformed into batch job.

    const querySfQueryHistory: SnowflakeQueryResult | undefined = biType
      ? await this.#retrieveQuerySfQueryHistory(biType)
      : undefined;

    await Promise.all(
      this.#logics.map(async (logic) => {
        const colDataDependencyRefs =
          DependenciesBuilder.#getColDataDependencyRefs(logic.statementRefs);
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
          DependenciesBuilder.#getWildcardDataDependencyRefs(
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

        if (biType && querySfQueryHistory) {
          const dashboardDataDependencyRefs =
            await DependenciesBuilder.#getDashboardDataDependencyRefs(
              logic.statementRefs,
              querySfQueryHistory,
              biType
            );

          const uniqueDashboardRefs = dashboardDataDependencyRefs.filter(
            (value, index, self) =>
              index ===
              self.findIndex((dashboard) =>
                typeof dashboard.name === 'string' &&
                typeof value.name === 'string'
                  ? DependenciesBuilder.#insensitiveEquality(
                      dashboard.name,
                      value.name
                    )
                  : dashboard.name === value.name &&
                    DependenciesBuilder.#insensitiveEquality(
                      dashboard.columnName,
                      value.columnName
                    ) &&
                    DependenciesBuilder.#insensitiveEquality(
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

    return { dashboards: this.#dashboards, dependencies: this.#dependencies };
  };

  static #insensitiveEquality = (str1: string, str2: string): boolean =>
    str1.toLowerCase() === str2.toLowerCase();
}
