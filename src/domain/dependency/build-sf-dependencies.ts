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
import {
  IConnectionPool,
  SnowflakeQueryResult,
} from '../snowflake-api/i-snowflake-api-repo';
import BaseAuth from '../services/base-auth';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';

export type Auth = BaseAuth;

export interface BuildResult {
  dashboards: Dashboard[];
  dependencies: Dependency[];
}

export interface BuildSfDependenciesRequestDto {
  targetOrgId?: string;
  logics: Logic[];
  mats: Materialization[];
  columns: Column[];
  catalog: ModelRepresentation[];
  biToolType?: BiToolType;
}

interface SfObjectRef 
{
  id:string,
  databaseName: string, 
  schemaName: string,
  matName:string, 
  type: 'TABLE' | 'VIEW'
}
interface SfObjectDependency {
  head: SfObjectRef 
  tail: SfObjectRef 
  type: 'BY_NAME' | 'BY_ID' | 'BY_NAME_AND_ID'
}

export type BuildSfDependenciesAuthDto = BaseAuth;

export type BuildSfDependenciesResponse = Result<BuildResult>;

export class BuildSfDependencies
  implements
    IUseCase<
      BuildSfDependenciesRequestDto,
      BuildSfDependenciesResponse,
      BuildSfDependenciesAuthDto,
      IConnectionPool
    >
{
  readonly #createDashboard: CreateDashboard;

  readonly #createDependency: CreateDependency;

  readonly #createExternalDependency: CreateExternalDependency;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  readonly #querySnowflake: QuerySnowflake;

  #auth?: Auth;

  #targetOrgId?: string;

  #mats?: Materialization[];

  #columns?: Column[];

  #dependencies: Dependency[] = [];

  #dashboards: Dashboard[] = [];

  #connPool?: IConnectionPool;

  constructor(
    createDashboard: CreateDashboard,
    createDependency: CreateDependency,
    createExternalDependency: CreateExternalDependency,
    querySfQueryHistory: QuerySfQueryHistory,
    querySnowflake: QuerySnowflake
  ) {
    this.#createDashboard = createDashboard;
    this.#createDependency = createDependency;
    this.#createExternalDependency = createExternalDependency;
    this.#querySfQueryHistory = querySfQueryHistory;
    this.#querySnowflake = querySnowflake;
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


  #buildDashboardRefDependency = async (
    dashboardRef: DashboardRef,
    relationName: string,
    parentRelationNames: string[]
  ): Promise<void> => {
    if (!this.#connPool || !this.#mats || !this.#columns || !this.#auth)
      throw new Error('Build dependency field values missing');

    const relationNameElements = relationName.split('.');
    if (relationNameElements.length !== 3)
      throw new RangeError('Unexpected number of sf model id elements');

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

  #getSfObjectDependencies = async (
  ): Promise<Dependency[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select * from snowflake.account_usage.object_dependencies;`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds:[] },
      this.#auth,
      this.#connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dependencies: SfObjectDependency[] = results.map(
      (el) => {
        const {
          REFERENCED_DATABASE: headDbName,
          REFERENCED_SCHEMA:headSchemaName,
          REFERENCED_OBJECT_NAME:headMatName,
          REFERENCED_OBJECT_ID:headObjId,
          REFERENCED_OBJECT_DOMAIN:headObjType,
          REFERENCING_DATABASE:headDbName,
          REFERENCING_SCHEMA:headSchemaName,
          REFERENCING_OBJECT_NAME:headMatName,
          REFERENCING_OBJECT_ID:headObjId,
          REFERENCING_OBJECT_DOMAIN:headObjType,
          DEPENDENCY_TYPE:objDependencyType,
        } = el;

        if (
          typeof headDbName === 'string' || 
          typeof headSchemaName === 'string' || 
          typeof headMatName === 'string' || 
          typeof headObjId === 'number' || 
        )

        const isComment = (val: unknown): val is string | undefined =>
          !val || typeof val === 'string';
        const isOwnerId = (val: unknown): val is string | undefined =>
          !val || typeof val === 'string';
        const isIsTransientVal = (val: unknown): val is string | undefined =>
          !val ||
          (typeof val === 'string' &&
            ['yes', 'no'].includes(val.toLowerCase()));

        if (
          typeof databaseName !== 'string' ||
          typeof schemaName !== 'string' ||
          typeof name !== 'string' ||
          typeof type !== 'string' ||
          !isIsTransientVal(isTransient) ||
          !isComment(comment) ||
          !isOwnerId(ownerId)
        )
          throw new Error(
            'Received mat representation field value in unexpected format'
          );

        return {
          databaseName,
          schemaName,
          name,
          relationName: `${databaseName}.${schemaName}.${name}`,
          type: parseMaterializationType(type.toLowerCase()),
          ownerId: ownerId || undefined,
          isTransient: isTransient
            ? isTransient.toLowerCase() !== 'no'
            : undefined,
          comment: comment || undefined,
        };
      }
    );

    return matRepresentations;
  };

  /* Creates all dependencies that exist between DWH resources */
  async execute(
    req: BuildSfDependenciesRequestDto,
    auth: BuildSfDependenciesAuthDto,
    connPool: IConnectionPool
  ): Promise<BuildSfDependenciesResponse> {
    try {
      this.#connPool = connPool;
      this.#auth = auth;
      this.#mats = req.mats;
      this.#columns = req.columns;
      this.#targetOrgId = req.targetOrgId;

      const querySfQueryHistory: SnowflakeQueryResult | undefined =
        req.biToolType
          ? await this.#retrieveQuerySfQueryHistory(req.biToolType)
          : undefined;

          await Promise.all(
            if (req.biToolType && querySfQueryHistory) {
          const dashboardDataDependencyRefs =
            await BuildSfDependencies.#getDashboardDataDependencyRefs(
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
                    ? 
                        dashboard.name ===
                        value.name
                      
                    : dashboard.name === value.name &&
                      
                        dashboard.columnName ===
                        value.columnName
                      &&
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
