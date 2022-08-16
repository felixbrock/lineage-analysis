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
  lineageId: string;
  targetOrganizationId: string;
  writeToPersistence: boolean;
}

export interface CreateDashboardAuthDto {
  isSystemInternal: boolean;
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
      if (!auth.isSystemInternal) throw new Error('Unauthorized');

      this.#dbConnection = dbConnection;

      const dashboard = Dashboard.create({
        id: new ObjectId().toHexString(),
        lineageId: request.lineageId,
        url: request.url,
        materializationName: request.materializationName,
        columnName: request.columnName,
        columnId: request.columnId,
        materializationId: request.materializationId,
        organizationId: request.targetOrganizationId,
      });

      if(!request.url) return Result.ok(dashboard);

      const readDashboardsResult = await this.#readDashboards.execute(
        {
          url: request.url,
          lineageId: request.lineageId,
          targetOrganizationId: request.targetOrganizationId,
        },
        {  isSystemInternal: auth.isSystemInternal },
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
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
