import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import BaseAuth from '../services/base-auth';
import {
  DashboardDataEnv,
  DashboardToDeleteRef,
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

export interface UpdateSfExternalDataEnvRequestDto {
  latestLineage: {
    completedAt: string;
  };
  biToolType: BiToolType;
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

  #buildDashboardToReplace = (
    oldProps: { id: string },
    newProps: DashboardProps
  ): Dashboard =>
    Dashboard.build({
      ...newProps,
      id: oldProps.id,
    });

  // compare dashboards and return dashboards to replace, dashboards to delete and dashboards to create

  #compareDashboards = async (
    newDashboards: Dashboard[]
  ): Promise<DashboardDataEnv> => {
    if (!this.auth || !this.connPool)
      throw new Error('Auth or connPool not set');

    const oldDashboards = await this.#dasboardRepo.all(
      this.auth,
      this.connPool
    );

    const dashboardsToReplace: Dashboard[] = [];
    const dashboardsToCreate: Dashboard[] = [];
    const dashboardToDeleteRefs: DashboardToDeleteRef[] = [];

    oldDashboards.forEach((oldDashboard) => {
      const newDashboardIndex = newDashboards.findIndex(
        (newDashboard) => newDashboard.url === oldDashboard.url
      );
      if (newDashboardIndex === -1) {
        dashboardToDeleteRefs.push({ id: oldDashboard.id });
        return;
      }
      const newDashboard = newDashboards[newDashboardIndex];

      if (oldDashboard.name !== newDashboard.name) {
        dashboardsToReplace.push(
          this.#buildDashboardToReplace(
            { id: oldDashboard.id },
            newDashboard.toDto()
          )
        );
        return;
      }

      dashboardsToCreate.push(newDashboard);
    });

    return { dashboardsToCreate, dashboardsToReplace, dashboardToDeleteRefs };
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

      const compareDashboardsResult = await this.#compareDashboards(
        buildBiResourcesResult.dashboards
      );

      return Result.ok({
        dataEnv: {
          ...compareDashboardsResult,
          dependenciesToCreate: buildBiResourcesResult.dependencies,
          deleteAllOldDependencies: true,
        },
      });
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
