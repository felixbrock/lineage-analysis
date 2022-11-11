import { v4 as uuidv4 } from 'uuid';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dashboard } from '../entities/dashboard';
import { ReadDashboards } from './read-dashboards';
import { IDashboardRepo } from './i-dashboard-repo';

export interface CreateDashboardRequestDto {
  url?: string;
  materializationName: string;
  materializationId: string;
  columnName: string;
  columnId: string;
  lineageId: string;
  targetOrganizationId?: string;
  writeToPersistence: boolean;
}

export interface CreateDashboardAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
  jwt:string
}

export type CreateDashboardResponseDto = Result<Dashboard>;

export class CreateDashboard
  implements
    IUseCase<
      CreateDashboardRequestDto,
      CreateDashboardResponseDto,
      CreateDashboardAuthDto
      
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  readonly #readDashboards: ReadDashboards;


  constructor(readDashboards: ReadDashboards, dashboardRepo: IDashboardRepo) {
    this.#readDashboards = readDashboards;
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    request: CreateDashboardRequestDto,
    auth: CreateDashboardAuthDto,
  ): Promise<CreateDashboardResponseDto> {
    try {
      if (auth.isSystemInternal && !request.targetOrganizationId)
        throw new Error('Target organization id missing');
      if (!auth.isSystemInternal && !auth.callerOrganizationId)
        throw new Error('Caller organization id missing');
      if (!request.targetOrganizationId && !auth.callerOrganizationId)
        throw new Error('No organization Id instance provided');
        if (request.targetOrganizationId && auth.callerOrganizationId)
        throw new Error('callerOrgId and targetOrgId provided. Not allowed');

      const dashboard = Dashboard.create({
        id: uuidv4(),
        lineageId: request.lineageId,
        url: request.url,
        materializationName: request.materializationName,
        columnName: request.columnName,
        columnId: request.columnId,
        materializationId: request.materializationId,
      });

      if (!request.url) return Result.ok(dashboard);

      const readDashboardsResult = await this.#readDashboards.execute(
        {
          url: request.url,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        auth,
      );

      if (!readDashboardsResult.success)
        throw new Error(readDashboardsResult.error);
      if (!readDashboardsResult.value)
        throw new Error('Reading dashboards failed');
      if (readDashboardsResult.value.length)
        throw new Error(`Dashboard already exists`);

      if (request.writeToPersistence)
        await this.#dashboardRepo.insertOne(dashboard, auth, request.targetOrganizationId);

      return Result.ok(dashboard);
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
