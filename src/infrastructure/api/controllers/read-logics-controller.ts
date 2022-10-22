// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadLogics,
  ReadLogicsAuthDto,
  ReadLogicsRequestDto,
  ReadLogicsResponseDto,
} from '../../../domain/logic/read-logics';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../../shared/base-controller';

export default class ReadLogicsController extends BaseController {
  readonly #readLogics: ReadLogics;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(readLogics: ReadLogics, getAccounts: GetAccounts, dbo: Dbo) {
    super();
    this.#readLogics = readLogics;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadLogicsRequestDto => {
    const { relationName, lineageId, targetOrganizationId } = httpRequest.query;

    if (!lineageId)
      throw new TypeError(
        'When querying logics the lineageId must be provided'
      );
    if (typeof lineageId !== 'string')
      throw new TypeError(
        'When querying logics the lineageId query param must be of type string'
      );

    return {
      relationName: typeof relationName === 'string' ? relationName : undefined,
      lineageId,
      targetOrganizationId: typeof targetOrganizationId === 'string' ? targetOrganizationId : undefined,
    };
  };

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadLogicsAuthDto => ({
      callerOrganizationId: userAccountInfo.callerOrganizationId,
      isSystemInternal: userAccountInfo.isSystemInternal,
    });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadLogicsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadLogicsController.getUserAccountInfo(jwt, this.#getAccounts);

      if (!getUserAccountInfoResult.success)
        return ReadLogicsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadLogicsRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value);

      const useCaseResult: ReadLogicsResponseDto =
        await this.#readLogics.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return ReadLogicsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadLogicsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return ReadLogicsController.fail(res, 'Internal error occurred while reading logics');
    }
  }
}
