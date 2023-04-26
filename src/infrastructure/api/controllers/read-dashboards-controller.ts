// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadDashboards,
  ReadDashboardsAuthDto,
  ReadDashboardsRequestDto,
  ReadDashboardsResponseDto,
} from '../../../domain/dashboard/read-dashboards';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from './shared/base-controller';
import Dbo from '../../persistence/db/mongo-db';

export default class ReadDashboardsController extends BaseController {
  readonly #readDashboards: ReadDashboards;

  readonly #dbo: Dbo;

  constructor(
    readDashboards: ReadDashboards,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile,
    dbo: Dbo
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readDashboards = readDashboards;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadDashboardsRequestDto => {
    const {
      url,
      name,
      materializationName,
      columnName,
      id,
      columnId,
      materializationId,
      targetOrgId,
    } = httpRequest.query;

    return {
      url: typeof url === 'string' ? url : undefined,
      name: typeof name === 'string' ? name : undefined,
      materializationName:
        typeof materializationName === 'string'
          ? materializationName
          : undefined,
      columnName: typeof columnName === 'string' ? columnName : undefined,
      id: typeof id === 'string' ? id : undefined,
      columnId: typeof columnId === 'string' ? columnId : undefined,
      materializationId:
        typeof materializationId === 'string' ? materializationId : undefined,
      targetOrgId: typeof targetOrgId === 'string' ? targetOrgId : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadDashboardsAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadDashboardsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(jwt);

      if (!getUserAccountInfoResult.success)
        return ReadDashboardsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadDashboardsRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);


      const useCaseResult: ReadDashboardsResponseDto =
        await this.#readDashboards.execute(requestDto, authDto, this.#dbo.dbConnection);

      if (!useCaseResult.success) {
        return ReadDashboardsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      await this.#dbo.releaseConnections();

      return ReadDashboardsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadDashboardsController.fail(
        res,
        'Internal error occurred while reading dashboards'
      );
    }
  }
}
