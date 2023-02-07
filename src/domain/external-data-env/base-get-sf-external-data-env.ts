import { v4 as uuidv4 } from 'uuid';
import { CreateDashboards } from '../dashboard/create-dashboards';
import { CreateDependencies } from '../dependency/create-dependencies';
import { Dashboard } from '../entities/dashboard';
import { Dependency, DependencyType } from '../entities/dependency';
import BaseAuth from '../services/base-auth';
import {
  IConnectionPool,
  SnowflakeQueryResult,
} from '../snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import {
  QuerySfQueryHistory,
  QuerySfQueryHistoryResponseDto,
} from '../snowflake-api/query-snowflake-history';
import { BiToolType } from '../value-types/bi-tool';

export type GenerateSfExternalDataEnvRequestDto = {
  targetOrgId?: string;
  biToolType?: BiToolType;
};

interface CitoMatRepresentation {
  id: string;
  relationName: string;
}

export interface DashboardRef {
  url: string;
  name?: string;
  materializationName: string;
  materializationId: string;
  columnName?: string;
  columnId?: string;
}

export type Auth = BaseAuth;

type BuildBiResourcesResult = {
  dashboards: Dashboard[];
  dependencies: Dependency[];
};

export default abstract class BaseGetSfExternalDataEnv {
  readonly #createDashboards: CreateDashboards;

  readonly #createDependencies: CreateDependencies;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  protected readonly querySnowflake: QuerySnowflake;

  protected auth?: Auth;

  protected targetOrgId?: string;

  protected connPool?: IConnectionPool;

  constructor(
    querySnowflake: QuerySnowflake,
    querySfQueryHistory: QuerySfQueryHistory,
    createDashboards: CreateDashboards,
    createDependencies: CreateDependencies
  ) {
    this.querySnowflake = querySnowflake;
    this.#querySfQueryHistory = querySfQueryHistory;
    this.#createDashboards = createDashboards;
    this.#createDependencies = createDependencies;
  }

  protected retrieveQuerySfQueryHistory = async (
    biType: BiToolType
  ): Promise<SnowflakeQueryResult> => {
    if (!this.connPool || !this.auth)
      throw new Error('connection pool or auth missing');

    const querySfQueryHistoryResult: QuerySfQueryHistoryResponseDto =
      await this.#querySfQueryHistory.execute(
        {
          biType,
          limit: 10,
          targetOrgId: this.targetOrgId,
        },
        this.auth,
        this.connPool
      );

    if (!querySfQueryHistoryResult.success)
      throw new Error(querySfQueryHistoryResult.error);
    if (!querySfQueryHistoryResult.value)
      throw new SyntaxError(`Retrival of query history failed`);

    return querySfQueryHistoryResult.value;
  };

  /* Get all relevant dashboards that are data dependency to self materialization */
  protected readDashboardRefs = async (
    matRepresentations: CitoMatRepresentation[],
    querySfQueryHistoryResult: SnowflakeQueryResult,
    biTool: BiToolType
  ): Promise<DashboardRef[]> => {
    const dependentDashboards: DashboardRef[] = [];

    matRepresentations.forEach((matRep) => {
      querySfQueryHistoryResult.forEach((entry) => {
        const queryText = entry.QUERY_TEXT;
        if (typeof queryText !== 'string')
          throw new Error('Retrieved bi layer query text not in string format');

        const testUrl = queryText.match(/"(https?:[^\s]+),/);
        const dashboardUrl = testUrl
          ? testUrl[1]
          : `${biTool} dashboard: ${uuidv4()}`;

        if (
          queryText.includes(matRep.relationName) ||
          (matRep.relationName.toLowerCase() === matRep.relationName &&
            queryText.includes(matRep.relationName.toUpperCase()))
        )
          dependentDashboards.push({
            url: dashboardUrl,
            materializationName: matRep.relationName,
            materializationId: matRep.id,
          });
      });
    });
    return dependentDashboards;
  };

  protected buildDashboardRefDependencies = async (
    dashboardRefs: DashboardRef[]
  ): Promise<BuildBiResourcesResult> => {
    if (!this.connPool || !this.auth)
      throw new Error('Build dependency field values missing');

    const uniqueDashboardRefs = dashboardRefs.filter(
      (dashboardRef, i, self) =>
        i === self.findIndex((el) => el.url === dashboardRef.url)
    );

    const createDashboardResult = await this.#createDashboards.execute(
      {
        toCreate: uniqueDashboardRefs.map((dashboardRef) => ({
          url: dashboardRef.url,
        })),
        targetOrgId: this.targetOrgId,
        writeToPersistence: false,
      },
      this.auth,
      this.connPool
    );

    if (!createDashboardResult.success)
      throw new Error(createDashboardResult.error);
    if (!createDashboardResult.value)
      throw new Error('Creating dashboard failed');

    const dashboards = createDashboardResult.value;

    const toCreate = dashboardRefs.map(
      (
        dashboardRef
      ): { headId: string; tailId: string; type: DependencyType } => {
        const dashboard = dashboards.find((el) => el.url === dashboardRef.url);

        if (!dashboard) throw new Error('Dashboard not found');

        return {
          headId: dashboard.id,
          tailId: dashboardRef.materializationId,
          type: 'external',
        };
      }
    );

    const createDependeciesResult = await this.#createDependencies.execute(
      {
        toCreate,
        targetOrgId: this.targetOrgId,
        writeToPersistence: false,
      },
      this.auth,
      this.connPool
    );

    if (!createDependeciesResult.success)
      throw new Error(createDependeciesResult.error);
    if (!createDependeciesResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependencies = createDependeciesResult.value;
    return { dashboards, dependencies };
  };

  protected buildBiResources = async (
    biToolType: BiToolType
  ): Promise<BuildBiResourcesResult> => {
    const querySfQueryHistory = await this.retrieveQuerySfQueryHistory(
      biToolType
    );

    const matReps = await this.getAllCitoMatReps();

    const dashboardRefs = await this.readDashboardRefs(
      matReps,
      querySfQueryHistory,
      biToolType
    );

    const result = await this.buildDashboardRefDependencies(dashboardRefs);

    return result;
  };

  protected getAllCitoMatReps = async (): Promise<CitoMatRepresentation[]> => {
    if (!this.connPool || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select id, relation_name * from cito.lineage.materializations;`;
    const queryResult = await this.querySnowflake.execute(
      { queryText, binds: [] },
      this.auth,
      this.connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const mats = queryResult.value;

    return mats.map((el): CitoMatRepresentation => {
      const { ID: id, RELATION_NAME: relationName } = el;

      if (typeof id !== 'string' || typeof relationName !== 'string')
        throw new Error(
          'Received unexpected mat representation from Snowflake'
        );

      return { id, relationName };
    });
  };
}
