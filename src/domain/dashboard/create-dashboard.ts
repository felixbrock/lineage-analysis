import { ObjectId } from 'mongodb';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { Dashboard } from '../entities/dashboard';
import { ReadDashboards } from './read-dashboards';
import { IDashboardRepo } from './i-dashboard-repo';
import { DbConnection } from '../services/i-db';

export interface CreateDashboardRequestDto {
  url?: string;
  materializationName: string;
  materializationId: string;
  columnName: string;
  columnId: string;
  lineageIds: string[];
  targetOrganizationId?: string;
  writeToPersistence: boolean;
}

export interface CreateDashboardAuthDto {
  isSystemInternal: boolean;
  callerOrganizationId?: string;
}

export type CreateDashboardResponseDto = Result<Dashboard>;

export class CreateDashboard
  implements
    IUseCase<
      CreateDashboardRequestDto,
      CreateDashboardResponseDto,
      CreateDashboardAuthDto,
      DbConnection
    >
{
  readonly #dashboardRepo: IDashboardRepo;

  readonly #readDashboards: ReadDashboards;

  #dbConnection: DbConnection;

  constructor(readDashboards: ReadDashboards, dashboardRepo: IDashboardRepo) {
    this.#readDashboards = readDashboards;
    this.#dashboardRepo = dashboardRepo;
  }

  async execute(
    request: CreateDashboardRequestDto,
    auth: CreateDashboardAuthDto,
    dbConnection: DbConnection
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

      let organizationId: string;
      if (auth.isSystemInternal && request.targetOrganizationId)
        organizationId = request.targetOrganizationId;
      else if (!auth.isSystemInternal && auth.callerOrganizationId)
        organizationId = auth.callerOrganizationId;
      else throw new Error('Unhandled organization id declaration');

      this.#dbConnection = dbConnection;

      const dashboard = Dashboard.create({
        id: new ObjectId().toHexString(),
        lineageIds: request.lineageIds,
        url: request.url,
        materializationName: request.materializationName,
        columnName: request.columnName,
        columnId: request.columnId,
        materializationId: request.materializationId,
        organizationId,
      });

      if (!request.url) return Result.ok(dashboard);

      const readDashboardsResult = await this.#readDashboards.execute(
        {
          url: request.url,
          lineageIds: request.lineageIds,
          targetOrganizationId: request.targetOrganizationId,
        },
        { isSystemInternal: auth.isSystemInternal, callerOrganizationId: auth.callerOrganizationId },
        this.#dbConnection
      );

      if (!readDashboardsResult.success)
        throw new Error(readDashboardsResult.error);
      if (!readDashboardsResult.value)
        throw new Error('Reading dashboards failed');
      if (readDashboardsResult.value.length)
        throw new Error(`Dashboard already exists`);

      if (request.writeToPersistence)
        await this.#dashboardRepo.insertOne(dashboard, this.#dbConnection);

      return Result.ok(dashboard);
    } catch (error: unknown) {
      if(error instanceof Error && error.message) console.trace(error.message); else if (!(error instanceof Error) && error) console.trace(error);
      return Result.fail('');
    }
  }
}
