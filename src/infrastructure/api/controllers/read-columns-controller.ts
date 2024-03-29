// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadColumns,
  ReadColumnsAuthDto,
  ReadColumnsRequestDto,
  ReadColumnsResponseDto,
} from '../../../domain/column/read-columns';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from './shared/base-controller';
import Dbo from '../../persistence/db/mongo-db';

export default class ReadColumnsController extends BaseController {
  readonly #readColumns: ReadColumns;

  readonly #dbo: Dbo;

  constructor(
    readColumns: ReadColumns,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile,
    dbo: Dbo
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readColumns = readColumns;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadColumnsRequestDto => {
    const {
      relationNames,
      names,
      index,
      type,
      materializationIds,
      targetOrgId,
    } = httpRequest.query;

    return {
      relationNames:
        typeof relationNames === 'string' ? relationNames.replaceAll(/\s/g, '').split(',') : [],
      names: typeof names === 'string' ? names.replaceAll(/\s/g, '').split(',') : [],
      index: typeof index === 'string' ? index : undefined,
      type: typeof type === 'string' ? type : undefined,
      materializationIds:
        typeof materializationIds === 'string'
          ? materializationIds.replaceAll(/\s/g, '').split(',')
          : [],
      targetOrgId: typeof targetOrgId === 'string' ? targetOrgId : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadColumnsAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadColumnsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(jwt);

      if (!getUserAccountInfoResult.success)
        return ReadColumnsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadColumnsRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);


      const useCaseResult: ReadColumnsResponseDto =
        await this.#readColumns.execute(requestDto, authDto, this.#dbo.dbConnection);


      if (!useCaseResult.success) {
        return ReadColumnsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadColumnsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadColumnsController.fail(
        res,
        'Internal error occurred while reading columns'
      );
    }
  }
}
