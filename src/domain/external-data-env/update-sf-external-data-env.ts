import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import {
  DashboardToDeleteRef,
  ExternalDataEnv,
  ExternalDataEnvProps,
} from './external-data-env';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import BaseGetSfExternalDataEnv from './base-get-sf-external-data-env';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';
import { BiToolType } from '../value-types/bi-tool';
import { Dashboard, DashboardProps } from '../entities/dashboard';
import { QuerySfQueryHistory } from '../snowflake-api/query-snowflake-history';
import { CreateDashboards } from '../dashboard/create-dashboards';
import { CreateDependencies } from '../dependency/create-dependencies';
import { IDashboardRepo } from '../dashboard/i-dashboard-repo';
import { Dependency } from '../entities/dependency';

export interface UpdateSfExternalDataEnvRequestDto {
  latestCompletedLineage: {
    completedAt: string;
  };
  biToolType?: BiToolType;
  targetOrgId?: string;
}

export interface UpdateSfExternalDataEnvAuthDto
  extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export type UpdateSfExternalDataEnvResponse = Result<ExternalDataEnvProps>;

export class UpdateSfExternalDataEnv
  extends BaseGetSfExternalDataEnv
  implements
    IUseCase<
      UpdateSfExternalDataEnvRequestDto,
      UpdateSfExternalDataEnvResponse,
      UpdateSfExternalDataEnvAuthDto,
      IConnectionPool
    >
{
  readonly #dasboardRepo: IDashboardRepo;

  constructor(
    querySnowflake: QuerySnowflake,
    querySfQueryHistory: QuerySfQueryHistory,
    createDashboards: CreateDashboards,
    createDependencies: CreateDependencies,
    dashboardRepo: IDashboardRepo
  ) {
    super(
      querySnowflake,
      querySfQueryHistory,
      createDashboards,
      createDependencies
    );
    this.#dasboardRepo = dashboardRepo;
  }

  #buildDashboardReplacement = (
    oldProps: { id: string },
    newProps: DashboardProps
  ): Dashboard =>
    Dashboard.build({
      ...newProps,
      id: oldProps.id,
    });

  // compare dashboards and return dashboards to replace, dashboards to delete and dashboards to create

  #mergeExternalDataEnv = async (
    newDashboards: Dashboard[],
    dependencies: Dependency[]
  ): Promise<ExternalDataEnv> => {
    if (!this.auth || !this.connPool)
      throw new Error('Auth or connPool not set');

    const oldDashboards = await this.#dasboardRepo.all(
      this.auth,
      this.connPool
    );

    if (!oldDashboards.length)
      return {
        dashboardsToCreate: newDashboards,
        dashboardsToReplace: [],
        dashboardToDeleteRefs: [],
        dependenciesToCreate: dependencies,
        deleteAllOldDependencies: false,
      };

    const dashboardsToReplace: Dashboard[] = [];
    const dashboardsToCreate: Dashboard[] = [];
    const dashboardToDeleteRefs: DashboardToDeleteRef[] = [];
    const dependenciesToCreate: Dependency[] = [];

    newDashboards.forEach((newDashb) => {
      const oldDashbIdx = oldDashboards.findIndex(
        (oldD) => newDashb.name === oldD.name
      );

      const relevantDeps = dependencies.filter(
        (el) => el.headId === newDashb.id || el.tailId === newDashb.id
      );

      if (oldDashbIdx === -1) {
        dashboardsToCreate.push(newDashb);
        dependenciesToCreate.push(...relevantDeps);
        return;
      }

      dashboardsToReplace.push(
        this.#buildDashboardReplacement(
          { id: oldDashboards[oldDashbIdx].id },
          newDashb.toDto()
        )
      );

      dependenciesToCreate.push(
        ...relevantDeps.map((el): Dependency => {
          const headId =
            el.headId === newDashb.id
              ? oldDashboards[oldDashbIdx].id
              : el.headId;
          const tailId =
            el.tailId === newDashb.id
              ? oldDashboards[oldDashbIdx].id
              : el.tailId;

          return Dependency.build({ ...el.toDto(), headId, tailId });
        })
      );
    });

    oldDashboards.forEach((oldDashb) => {
      const newDashbIdx = newDashboards.findIndex(
        (newD) => oldDashb.name === newD.name
      );

      if (newDashbIdx === -1) dashboardToDeleteRefs.push({ id: oldDashb.id });
    });

    return {
      dashboardsToCreate,
      dashboardsToReplace,
      dashboardToDeleteRefs,
      dependenciesToCreate,
      deleteAllOldDependencies: false,
    };
  };

  /* Checks Snowflake resources for changes and returns partial data env to merge with existing snapshot */
  async execute(
    req: UpdateSfExternalDataEnvRequestDto,
    auth: UpdateSfExternalDataEnvAuthDto,
    connPool: IConnectionPool
  ): Promise<UpdateSfExternalDataEnvResponse> {
    try {
      /* 
        1. Get all dashboards from sf
        2. Delete all external dependencies in sf
        3. Generate new dashboards and dependencies
        identify dashboards to replace and dashboards to delete
*/
      this.connPool = connPool;
      this.auth = auth;
      this.targetOrgId = req.targetOrgId;

      const buildBiResourcesResult = await this.buildBiResources(
        req.biToolType
      );

      const mergedDataEnv = await this.#mergeExternalDataEnv(
        buildBiResourcesResult.dashboards,
        buildBiResourcesResult.dependencies
      );

      return Result.ok({
        dataEnv: mergedDataEnv,
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
