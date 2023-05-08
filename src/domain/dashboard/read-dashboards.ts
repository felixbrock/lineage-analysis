import { Dashboard } from '../entities/dashboard';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDashboardRepo, DashboardQueryDto } from './i-dashboard-repo';
import { IDbConnection } from '../services/i-db';

export interface ReadDashboardsRequestDto {
  url?: string;
  name?: string;
  materializationName?: string;
  columnName?: string;
  id?: string;
  columnId?: string;
  materializationId?: string;
  targetOrgId?: string;
}

export type ReadDashboardsAuthDto = BaseAuth;

export type ReadDashboardsResponseDto = Result<Dashboard[]>;

export class ReadDashboards
  implements
    IUseCase<
      ReadDashboardsRequestDto,
      ReadDashboardsResponseDto,
      ReadDashboardsAuthDto,
      IDbConnection
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  constructor(dashboardRepo: IDashboardRepo) {
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    req: ReadDashboardsRequestDto,
    auth: ReadDashboardsAuthDto,
    dbConnection: IDbConnection
  ): Promise<ReadDashboardsResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dashboards: Dashboard[] = await this.#dashboardRepo.findBy(
        this.#buildDashboardQueryDto(req),
        auth,
        dbConnection
      );
      if (!dashboards)
        throw new ReferenceError(`Queried dashboards do not exist`);

      return Result.ok(dashboards);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildDashboardQueryDto = (
    request: ReadDashboardsRequestDto
  ): DashboardQueryDto => {
    const queryDto: DashboardQueryDto = {};

    if (request.url) queryDto.url = request.url;
    if (request.name) queryDto.name = request.name;
    if (request.id) queryDto.id = request.id;

    return queryDto;
  };
}
