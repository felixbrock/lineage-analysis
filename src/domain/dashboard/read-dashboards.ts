import { Dashboard } from '../entities/dashboard';
import { GetSnowflakeProfile } from '../integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../integration-api/i-integration-api-repo';
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
  targetOrgId?: string;
  profile?: SnowflakeProfileDto;
}

export interface ReadDashboardsAuthDto {
  isSystemInternal: boolean;
  callerOrgId?: string;
  jwt: string;
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

  readonly #getSnowflakeProfile: GetSnowflakeProfile;

  constructor(
    dashboardRepo: IDashboardRepo,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    this.#dashboardRepo = dashboardRepo;
    this.#getSnowflakeProfile = getSnowflakeProfile;
  }

  #getProfile = async (
    jwt: string,
    targetOrgId?: string
  ): Promise<SnowflakeProfileDto> => {
    const readSnowflakeProfileResult = await this.#getSnowflakeProfile.execute(
      { targetOrgId },
      {
        jwt,
      }
    );

    if (!readSnowflakeProfileResult.success)
      throw new Error(readSnowflakeProfileResult.error);
    if (!readSnowflakeProfileResult.value)
      throw new Error('SnowflakeProfile does not exist');

    return readSnowflakeProfileResult.value;
  };

  async execute(
    request: ReadDashboardsRequestDto,
    auth: ReadDashboardsAuthDto
  ): Promise<ReadDashboardsResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (request.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const profile =
        request.profile ||
        (await this.#getProfile(
          auth.jwt,
          auth.isSystemInternal ? request.targetOrgId : undefined
        ));

      const dashboards: Dashboard[] = await this.#dashboardRepo.findBy(
        this.#buildDashboardQueryDto(request),
        profile,
        auth,
        request.targetOrgId
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
    request: ReadDashboardsRequestDto
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
