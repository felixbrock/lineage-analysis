import { Dashboard } from '../entities/dashboard';
import { DbConnection } from '../services/i-db';
import IUseCase from '../services/use-case';
import Result from '../value-types/transient-types/result';
import { IDashboardRepo, DashboardQueryDto } from './i-dashboard-repo';

export interface ReadDashboardsRequestDto {
    url?: string;
    name?: string;
    materialisation?: string;
    column?: string; 
    id?: string;
    columnId?: string,
    matId?: string;
    lineageId: string;
}

export interface ReadDashboardsAuthDto {
  organizationId: string;
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

      const dashboards: Dashboard[] = await this.#dashboardRepo.findBy(
        this.#buildDashboardQueryDto(request, auth.organizationId),
        dbConnection
      );
      if (!dashboards)
        throw new ReferenceError(`Queried dashboards do not exist`);

      return Result.ok(dashboards);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  #buildDashboardQueryDto = (
    request: ReadDashboardsRequestDto,
    organizationId: string
  ): DashboardQueryDto => {
    console.log(organizationId);

    const queryDto: DashboardQueryDto = { lineageId: request.lineageId };

    // todo - add organizationId
    // queryDto.organizationId = organizationId;
    if (request.url) queryDto.url = request.url; 
    if (request.name) queryDto.name = request.name;
    if (request.materialisation) queryDto.materialisation = request.materialisation;
    if (request.column) queryDto.column = request.column; 
    if (request.id) queryDto.id = request.id;
    if (request.columnId) queryDto.columnId = request.columnId;
    if (request.matId) queryDto.matId = request.matId;

    return queryDto;
  };
}
