// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { buildDashboardDto } from '../../../domain/dashboard/dashboard-dto';
import {
  ReadDashboards,
  ReadDashboardsAuthDto,
  ReadDashboardsRequestDto,
  ReadDashboardsResponseDto,
} from '../../../domain/dashboard/read-dashboards';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadDashboardsController extends BaseController {
  readonly #readDashboards: ReadDashboards;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(
    readDashboards: ReadDashboards,
    getAccounts: GetAccounts,
    dbo: Dbo
  ) {
    super();
    this.#readDashboards = readDashboards;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadDashboardsRequestDto => {
    const {url, name, materializationName, columnName,
        id, columnId, materializationId, lineageId
     } = httpRequest.query;

    if (!lineageId)
      throw new TypeError(
        'When querying dependencies the lineageId must be provided'
      );
    if (typeof lineageId !== 'string')
      throw new TypeError(
        'When querying dependencies the lineageId query param must be of type string'
      );

    return {
      url: typeof url === 'string' ? url : undefined,
      name: typeof name === 'string' ? name : undefined,
      materializationName: typeof materializationName === 'string' ? materializationName : undefined,
      columnName: typeof columnName === 'string' ? columnName : undefined,
      id: typeof id === 'string' ? id : undefined,
      columnId: typeof columnId === 'string' ? columnId : undefined,
      materializationId: typeof materializationId === 'string' ? materializationId : undefined,
      lineageId,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo
  ): ReadDashboardsAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadDependenciesController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadDependenciesInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadDependenciesInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: ReadDashboardsRequestDto = this.#buildRequestDto(req);
      // const authDto: ReadDependenciesAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );

      const useCaseResult: ReadDashboardsResponseDto =
        await this.#readDashboards.execute(
          requestDto,
          {
            organizationId: 'todo',
          },
          this.#dbo.dbConnection
        );


      if (!useCaseResult.success) {
        return ReadDashboardsController.badRequest(res, useCaseResult.error);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => buildDashboardDto(element))
        : useCaseResult.value;

      return ReadDashboardsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadDashboardsController.fail(res, error);
      if (error instanceof Error)
        return ReadDashboardsController.fail(res, error);
      return ReadDashboardsController.fail(res, 'Unknown error occured');
    }
  }
}
