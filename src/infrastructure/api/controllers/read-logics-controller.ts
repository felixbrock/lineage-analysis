// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import {
  ReadLogics,
  ReadLogicsAuthDto,
  ReadLogicsRequestDto,
  ReadLogicsResponseDto,
} from '../../../domain/logic/read-logics';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from './shared/base-controller';
import Dbo from '../../persistence/db/mongo-db';

export default class ReadLogicsController extends BaseController {
  readonly #readLogics: ReadLogics;

  readonly #dbo: Dbo;

  constructor(
    readLogics: ReadLogics,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile,
    dbo: Dbo
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readLogics = readLogics;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadLogicsRequestDto => {
    const { relationNames, targetOrgId } = httpRequest.query;

    const isStringArray = (obj: unknown): obj is string[] => !!obj && Array.isArray(obj) && obj.every(el => typeof el === 'string');

    return {
      relationNames: isStringArray(relationNames)? relationNames : undefined,
      targetOrgId: typeof targetOrgId === 'string' ? targetOrgId : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadLogicsAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadLogicsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(jwt);

      if (!getUserAccountInfoResult.success)
        return ReadLogicsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadLogicsRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);


      const useCaseResult: ReadLogicsResponseDto =
        await this.#readLogics.execute(requestDto, authDto, this.#dbo.dbConnection);


      if (!useCaseResult.success) {
        return ReadLogicsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadLogicsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadLogicsController.fail(
        res,
        'Internal error occurred while reading logics'
      );
    }
  }
}
