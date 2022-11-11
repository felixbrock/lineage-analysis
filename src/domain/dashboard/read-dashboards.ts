import { Dashboard } from '../entities/dashboard';
import {} from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDashboardRepo, DashboardQueryDto } from './i-dashboard-repo';

export interface ReadDashboardsRequestDto {
  url?: string;
  name?: string;
  materializationName?: string;
  columnName?: string;
  id?: string;
  columnId?: string;
  materializationId?: string;
  lineageId: string;
  targetOrganizationId?: string;
}

export interface ReadDashboardsAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
  jwt:string;
}

export type ReadDashboardsResponseDto = Result<Dashboard[]>;

export class ReadDashboards
  implements
    IUseCase<
      ReadDashboardsRequestDto,
      ReadDashboardsResponseDto,
      ReadDashboardsAuthDto
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  constructor(dashboardRepo: IDashboardRepo) {
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    request: ReadDashboardsRequestDto,
    auth: ReadDashboardsAuthDto
  ): Promise<ReadDashboardsResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dashboards: Dashboard[] = await this.#dashboardRepo.findBy(
        this.#buildDashboardQueryDto(request),
        auth,
        request.targetOrganizationId
      );
      if (!dashboards)
        throw new ReferenceError(`Queried dashboards do not exist`);

      return Result.ok(dashboards);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildDashboardQueryDto = (
    request: ReadDashboardsRequestDto,
  ): DashboardQueryDto => {
    const queryDto: DashboardQueryDto = {
      lineageId: request.lineageId,
    };

    if (request.url) queryDto.url = request.url;
    if (request.name) queryDto.name = request.name;
    if (request.materializationName)
      queryDto.materializationName = request.materializationName;
    if (request.columnName) queryDto.columnName = request.columnName;
    if (request.id) queryDto.id = request.id;
    if (request.columnId) queryDto.columnId = request.columnId;
    if (request.materializationId)
      queryDto.materializationId = request.materializationId;

    return queryDto;
  };
}
