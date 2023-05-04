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
import { BiToolType, biToolTypes } from '../value-types/bi-tool';
import { IDbConnection } from '../services/i-db';
import GetSfExternalDataEnvRepo from '../../infrastructure/persistence/get-sf-external-data-env-repo';

export type GenerateSfExternalDataEnvRequestDto = {
  targetOrgId?: string;
  biToolType?: BiToolType;
};

interface CitoMatRepresentation {
  id: string;
  relationName: string;
}

interface SfQueryHistoryResult {
  type: BiToolType;
  res: SnowflakeQueryResult;
}

export interface DashboardRef {
  url: string;
  name: string;
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

  readonly repo: GetSfExternalDataEnvRepo;

  protected auth?: Auth;

  protected targetOrgId?: string;

  protected connPool?: IConnectionPool;

  protected dbConnection?: IDbConnection;

  constructor(
    querySnowflake: QuerySnowflake,
    querySfQueryHistory: QuerySfQueryHistory,
    createDashboards: CreateDashboards,
    createDependencies: CreateDependencies,
    getSfExternalDataEnvRepo: GetSfExternalDataEnvRepo
  ) {
    this.querySnowflake = querySnowflake;
    this.#querySfQueryHistory = querySfQueryHistory;
    this.#createDashboards = createDashboards;
    this.#createDependencies = createDependencies;
    this.repo = getSfExternalDataEnvRepo;
  }

  protected retrieveQuerySfQueryHistory = async (
    biType?: BiToolType
  ): Promise<SfQueryHistoryResult[]> => {
    const { auth, connPool } = this;

    if (!connPool || !auth) throw new Error('connection pool or auth missing');

    const sfQuerHistoryResults = await Promise.all(
      (biType ? [biType] : biToolTypes).map(
        async (el): Promise<SfQueryHistoryResult | undefined> => {
          const querySfQueryHistoryResult: QuerySfQueryHistoryResponseDto =
            await this.#querySfQueryHistory.execute(
              {
                biType: el,
                targetOrgId: this.targetOrgId,
              },
              auth,
              connPool
            );

          if (!querySfQueryHistoryResult.success)
            throw new Error(querySfQueryHistoryResult.error);
          if (!querySfQueryHistoryResult.value)
            throw new SyntaxError(`Retrival of query history failed`);

          return querySfQueryHistoryResult.value
            ? { type: el, res: querySfQueryHistoryResult.value }
            : undefined;
        }
      )
    );

    const isSfQueryHistoryResult = (
      obj: unknown
    ): obj is SfQueryHistoryResult =>
      !!obj && typeof obj === 'object' && 'type' in obj && 'res' in obj;

    const referencingDashboards: SfQueryHistoryResult[] =
      sfQuerHistoryResults.filter(isSfQueryHistoryResult);

    return referencingDashboards;
  };

  /* Get all relevant dashboards that are data dependency to self materialization */
  protected readDashboardRefs = async (
    matRepresentations: CitoMatRepresentation[],
    querySfQueryHistoryResult: SnowflakeQueryResult,
    biTool: BiToolType
  ): Promise<DashboardRef[]> => {
    const dependentDashboards: DashboardRef[] = [];

    querySfQueryHistoryResult.forEach((entry) => {
      const { QUERY_TEXT: text, QUERY_TAG: tag } = entry;
      if (typeof text !== 'string' || typeof tag !== 'string')
        throw new Error(
          'Retrieved bi layer query text or tag not in string format'
        );

      const testUrl = text.match(/"(https?:[^\s]+),/);

      const dbMatch = tag.match(/(?<=dashboard-luid":\s")[\w-]+/);
      const wbMatch = tag.match(/(?<=workbook-luid":\s")[\w-]+/);
      const wsMatch = tag.match(/(?<=worksheet-luid":\s")[\w-]+/);

      let id: string;
      if (dbMatch && dbMatch.length) {
        if (dbMatch.length > 1)
          console.warn(`Multiple dashboard ids in ${tag} found`);
        id = `Dashb. id: ${dbMatch[0]}`;
      } else if (wbMatch && wbMatch.length) {
        if (wbMatch.length > 1)
          console.warn(`Multiple workbook ids in ${tag} found`);
        id = `Workb. id: ${wbMatch[0]}`;
      } else if (wsMatch && wsMatch.length) {
        if (wsMatch.length > 1)
          console.warn(`Multiple worksheet ids in ${tag} found`);
        id = `Worksh. id: ${wsMatch[0]}`;
      } else id = `unknown - random id: ${uuidv4()}`;

      const dashboardUrl = testUrl ? testUrl[1] : `${biTool} Dashboard`;

      const matchedRelationNames = text.match(/"?\S+"?\."?\S+"?\."?\S+"?/gm);

      if (!matchedRelationNames || !matchedRelationNames.length) return;

      const references = [
        ...new Set(
          matchedRelationNames.map((relName) =>
            relName
              .split('.')
              .map((el) =>
                el.includes('"') ? el.replace(/"/g, '') : el.toUpperCase()
              )
              .join('.')
          )
        ),
      ];

      matRepresentations.forEach((matRep) => {
        if (references.includes(matRep.relationName)) {
          const newDashboard = {
            url: dashboardUrl,
            name: id,
            materializationName: matRep.relationName,
            materializationId: matRep.id,
          };

          if (
            dependentDashboards.some(
              (el) =>
                el.url === newDashboard.url &&
                el.name === newDashboard.name &&
                el.materializationName === newDashboard.materializationName &&
                el.materializationId === newDashboard.materializationId
            )
          )
            return;

          dependentDashboards.push(newDashboard);
        }
      });
    });
    return dependentDashboards;
  };

  protected buildDashboardRefDependencies = async (
    dashboardRefs: DashboardRef[]
  ): Promise<BuildBiResourcesResult> => {
    if (!this.dbConnection || !this.auth)
      throw new Error('Build dependency field values missing');

    const uniqueDashboardRefs = dashboardRefs.filter(
      (dashboardRef, i, self) =>
        i === self.findIndex((el) => el.name === dashboardRef.name)
    );

    const createDashboardResult = await this.#createDashboards.execute(
      {
        toCreate: uniqueDashboardRefs.map((dashboardRef) => ({
          url: dashboardRef.url,
          name: dashboardRef.name,
        })),
        targetOrgId: this.targetOrgId,
        writeToPersistence: false,
      },
      this.auth,
      this.dbConnection
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
        const dashboard = dashboards.find(
          (el) => el.name === dashboardRef.name
        );

        if (!dashboard) {
          throw new Error('Dashboard not found');
        }

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
      this.dbConnection
    );

    if (!createDependeciesResult.success)
      throw new Error(createDependeciesResult.error);
    if (!createDependeciesResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependencies = createDependeciesResult.value;
    return { dashboards, dependencies };
  };

  protected buildBiResources = async (
    biToolType?: BiToolType
  ): Promise<BuildBiResourcesResult> => {
    const querySfQueryHistoryResults = await this.retrieveQuerySfQueryHistory(
      biToolType
    );

    if (!querySfQueryHistoryResults.length)
      return { dashboards: [], dependencies: [] };

    const matReps = await this.getAllCitoMatReps();

    const dashboardRefs = await Promise.all(
      querySfQueryHistoryResults.map(async (el) => {
        const identifiedRefs = await this.readDashboardRefs(
          matReps,
          el.res,
          el.type
        );

        return identifiedRefs;
      })
    );

    const result = await this.buildDashboardRefDependencies(
      dashboardRefs.flat()
    );

    return result;
  };

  protected getAllCitoMatReps = async (): Promise<CitoMatRepresentation[]> => {
    if (!this.dbConnection || !this.auth || !this.auth.callerOrgId)
      throw new Error('Missing properties for generating sf data env');

    const mats = await this.repo.allRelations(this.dbConnection, this.auth.callerOrgId);

    if (!mats) return [];

    return mats.map((el: any): CitoMatRepresentation => {
      const { id, relation_name: relationName } = el;

      if (typeof id !== 'string' || typeof relationName !== 'string')
        throw new Error(
          'Received unexpected mat representation from Snowflake'
        );

      return { id, relationName };
    });
  };
}
