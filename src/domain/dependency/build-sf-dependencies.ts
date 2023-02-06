// todo clean architecture violation
import { v4 as uuidv4 } from 'uuid';
import { CreateDashboards } from '../dashboard/create-dashboards';
import { CreateDependencies } from './create-dependencies';
import { Dashboard } from '../entities/dashboard';
import { Dependency, DependencyType } from '../entities/dependency';
import { DashboardRef } from '../entities/logic';
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
import { DependencyToDeleteRef } from '../data-env/data-env';

export interface BuildSfDependenciesRequestDto {
  targetOrgId?: string;
  biToolType?: BiToolType;
}

export const sfObjRefTypes = ['TABLE', 'VIEW'] as const;
export type SfObjRefType = typeof sfObjRefTypes[number];

export const parseSfObjRefType = (type: unknown): SfObjRefType => {
  if (typeof type !== 'string')
    throw new Error('Provision of type in non-string format');

  const identifiedElement = sfObjRefTypes.find(
    (element) => element.toLowerCase() === type.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

interface SfObjectRef {
  id: number;
  dbName: string;
  schemaName: string;
  matName: string;
  type: SfObjRefType;
}

export const sfObjDependencyTypes = [
  'BY_NAME',
  'BY_ID',
  'BY_NAME_AND_ID',
] as const;
export type SfObjDependencyType = typeof sfObjDependencyTypes[number];

export const parseSfObjDependencyType = (
  type: unknown
): SfObjDependencyType => {
  if (typeof type !== 'string')
    throw new Error('Provision of type in non-string format');

  const identifiedElement = sfObjDependencyTypes.find(
    (element) => element.toLowerCase() === type.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

interface SfObjectDependency {
  head: SfObjectRef;
  tail: SfObjectRef;
  type: SfObjDependencyType;
}

interface MatRepresentation {
  id: string;
  relationName: string;
}

export type BuildSfDependenciesAuthDto = BaseAuth;

export type BuildSfDependenciesResponse = Result<SfBuildResult>;

export class BuildSfDependencies
  implements
    IUseCase<
      BuildSfDependenciesRequestDto,
      BuildSfDependenciesResponse,
      BuildSfDependenciesAuthDto,
      IConnectionPool
    >
{
  readonly #createDashboards: CreateDashboards;

  readonly #createDependencies: CreateDependencies;

  readonly #querySfQueryHistory: QuerySfQueryHistory;

  readonly #querySnowflake: QuerySnowflake;

  #auth?: Auth;

  #targetOrgId?: string;

  #dependencies: Dependency[] = [];

  #dashboards: Dashboard[] = [];

  #connPool?: IConnectionPool;

  constructor(
    createDashboards: CreateDashboards,
    createDependencies: CreateDependencies,
    querySfQueryHistory: QuerySfQueryHistory,
    querySnowflake: QuerySnowflake
  ) {
    this.#createDashboards = createDashboards;
    this.#createDependencies = createDependencies;
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
  static #readDashboardRefs = async (
    matRepresentations: MatRepresentation[],
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

  #buildDashboardRefDependencies = async (
    dashboardRefs: DashboardRef[]
  ): Promise<void> => {
    if (!this.#connPool || !this.#auth)
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

    const dashboards = createDashboardResult.value;

    this.#dashboards.push(...dashboards);

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
        targetOrgId: this.#targetOrgId,
        writeToPersistence: false,
      },
      this.#auth,
      this.#connPool
    );

    if (!createDependeciesResult.success)
      throw new Error(createDependeciesResult.error);
    if (!createDependeciesResult.value)
      throw new ReferenceError(`Creating external dependency failed`);

    const dependencies = createDependeciesResult.value;
    this.#dependencies.push(...dependencies);
  };

  #buildBiDependencies = async (
    biToolType: BiToolType,
    matReps: MatRepresentation[]
  ): Promise<void> => {
    const querySfQueryHistory = await this.#retrieveQuerySfQueryHistory(
      biToolType
    );

    const dashboardRefs = await BuildSfDependencies.#readDashboardRefs(
      matReps,
      querySfQueryHistory,
      biToolType
    );

    await this.#buildDashboardRefDependencies(dashboardRefs);
  };

  #getAllMatReps = async (): Promise<MatRepresentation[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select id, relation_name * from cito.lineage.materializations;`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const mats = queryResult.value;

    return mats.map((el): MatRepresentation => {
      const { ID: id, RELATION_NAME: relationName } = el;

      if (typeof id !== 'string' || typeof relationName !== 'string')
        throw new Error(
          'Received unexpected mat representation from Snowflake'
        );

      return { id, relationName };
    });
  };

  #getReferencedMatRepresentations = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: MatRepresentation[]
  ): Promise<MatRepresentation[]> => {
    const distinctRelationNames = sfObjDependencies.reduce(
      (accumulation: string[], val: SfObjectDependency) => {
        const localAcc = accumulation;

        const refNameHead = `${val.head.dbName}.${val.head.schemaName}.${val.head.matName}`;
        const refNameTail = `${val.tail.dbName}.${val.tail.schemaName}.${val.tail.matName}`;

        if (localAcc.includes(refNameHead)) localAcc.push(refNameHead);

        if (localAcc.includes(refNameTail)) localAcc.push(refNameTail);

        return localAcc;
      },
      []
    );

    const referencedMats = matReps.filter((el) =>
      distinctRelationNames.includes(el.relationName)
    );

    return referencedMats;
  };

  #buildDataDependencies = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: MatRepresentation[]
  ): Promise<void> => {
    const referencedMatReps = await this.#getReferencedMatRepresentations(
      sfObjDependencies,
      matReps
    );

    const dependencies = sfObjDependencies.map((el): Dependency => {
      const headRelationName = `${el.head.dbName}.${el.head.schemaName}.${el.head.matName}`;
      const headMat = referencedMatReps.find(
        (entry) => entry.relationName === headRelationName
      );
      if (!headMat) throw new Error('Mat representation for head not found ');

      const tailRelationName = `${el.tail.dbName}.${el.tail.schemaName}.${el.tail.matName}`;
      const tailMat = referencedMatReps.find(
        (entry) => entry.relationName === tailRelationName
      );
      if (!tailMat) throw new Error('Mat representation for tail not found ');

      return Dependency.create({
        id: uuidv4(),
        headId: headMat.id,
        tailId: tailMat.id,
        type: 'data',
      });
    });

    this.#dependencies.push(...dependencies);
  };

  #getSfObjectDependencies = async (): Promise<SfObjectDependency[]> => {
    if (!this.#connPool || !this.#auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select * from snowflake.account_usage.object_dependencies;`;
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      this.#auth,
      this.#connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dependencies: SfObjectDependency[] = results.map((el) => {
      const {
        REFERENCED_DATABASE: headDbName,
        REFERENCED_SCHEMA: headSchemaName,
        REFERENCED_OBJECT_NAME: headMatName,
        REFERENCED_OBJECT_ID: headObjId,
        REFERENCED_OBJECT_DOMAIN: headObjType,
        REFERENCING_DATABASE: tailDbName,
        REFERENCING_SCHEMA: tailSchemaName,
        REFERENCING_OBJECT_NAME: tailMatName,
        REFERENCING_OBJECT_ID: tailObjId,
        REFERENCING_OBJECT_DOMAIN: tailObjType,
        DEPENDENCY_TYPE: objDependencyType,
      } = el;

      if (
        typeof headDbName !== 'string' ||
        typeof headSchemaName !== 'string' ||
        typeof headMatName !== 'string' ||
        typeof headObjId !== 'number' ||
        typeof tailDbName !== 'string' ||
        typeof tailSchemaName !== 'string' ||
        typeof tailMatName !== 'string' ||
        typeof tailObjId !== 'number'
      )
        throw new Error('Received sf obj representation in unexpected format');

      const head: SfObjectRef = {
        id: headObjId,
        type: parseSfObjRefType(headObjType),
        dbName: headDbName,
        schemaName: headSchemaName,
        matName: headMatName,
      };
      const tail: SfObjectRef = {
        id: tailObjId,
        type: parseSfObjRefType(tailObjType),
        dbName: tailDbName,
        schemaName: tailSchemaName,
        matName: tailMatName,
      };

      return {
        head,
        tail,
        type: parseSfObjDependencyType(objDependencyType),
      };
    });

    return dependencies;
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
      this.#targetOrgId = req.targetOrgId;

      const sfObjDependencies = await this.#getSfObjectDependencies();

      const matReps = await this.#getAllMatReps();

      await this.#buildDataDependencies(sfObjDependencies, matReps);

      if (req.biToolType)
        await this.#buildBiDependencies(req.biToolType, matReps);

      return Result.ok({});
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.ok();
    }
  }
}
