import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dashboard } from '../entities/dashboard';
import { IDashboardRepo } from './i-dashboard-repo';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';

export interface CreateDashboardsRequestDto {
  toCreate: { url: string; name?: string }[];
  targetOrgId?: string;
  writeToPersistence: boolean;
}

export interface CreateDashboardsAuthDto {
  isSystemInternal: boolean;
  callerOrgId?: string;
  jwt: string;
}

export type CreateDashboardsResponseDto = Result<Dashboard[]>;

export class CreateDashboards
  implements
    IUseCase<
      CreateDashboardsRequestDto,
      CreateDashboardsResponseDto,
      CreateDashboardsAuthDto,
      IConnectionPool
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  constructor(dashboardRepo: IDashboardRepo) {
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    req: CreateDashboardsRequestDto,
    auth: CreateDashboardsAuthDto,
    connPool: IConnectionPool
  ): Promise<CreateDashboardsResponseDto> {
    try {
      if (auth.isSystemInternal && !req.targetOrgId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrgId)
        throw new Error('Caller organization id missing');
      if (!req.targetOrgId && !auth.callerOrgId)
        throw new Error('No organization Id instance provided');
      if (req.targetOrgId && auth.callerOrgId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dashboards = req.toCreate.map((el) =>
        Dashboard.create({
          id: uuidv4(),
          url: el.url,
          name: el.name,
        })
      );

      if (req.writeToPersistence)
        await this.#dashboardRepo.insertMany(dashboards, auth, connPool);

      return Result.ok(dashboards);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
