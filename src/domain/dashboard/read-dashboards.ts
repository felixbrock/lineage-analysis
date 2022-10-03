import { Dashboard } from '../entities/dashboard';
import { DbConnection } from '../services/i-db';
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
}

export type ReadDashboardsResponseDto = Result<Dashboard[]>;

export class ReadDashboards
  implements
    IUseCase<
      ReadDashboardsRequestDto,
      ReadDashboardsResponseDto,
      ReadDashboardsAuthDto,
      DbConnection
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  #dbConnection: DbConnection;

  constructor(dashboardRepo: IDashboardRepo) {
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    request: ReadDashboardsRequestDto,
    auth: ReadDashboardsAuthDto,
    dbConnection: DbConnection
  ): Promise<ReadDashboardsResponseDto> {
    try {
      this.#dbConnection = dbConnection;

      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if(!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
        if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed'); 

      let organizationId;
      if(auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if(auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else
        throw new Error('Unhandled organizationId allocation');

        
      const dashboards: Dashboard[] = await this.#dashboardRepo.findBy(
        this.#buildDashboardQueryDto(request, organizationId),
        dbConnection
      );
      if (!dashboards)
        throw new ReferenceError(`Queried dashboards do not exist`);

      return Result.ok(dashboards);
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }

  #buildDashboardQueryDto = (
    request: ReadDashboardsRequestDto,
    organizationId: string
  ): DashboardQueryDto => {
    const queryDto: DashboardQueryDto = {
      lineageId: request.lineageId,
      organizationId,
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
