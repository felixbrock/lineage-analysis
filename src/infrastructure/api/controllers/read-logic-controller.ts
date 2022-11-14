// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadLogic,
  ReadLogicAuthDto,
  ReadLogicRequestDto,
  ReadLogicResponseDto,
} from '../../../domain/logic/read-logic';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../../shared/base-controller';

export default class ReadLogicController extends BaseController {
  readonly #readLogic: ReadLogic;

  readonly #getAccounts: GetAccounts;


  constructor(readLogic: ReadLogic, getAccounts: GetAccounts) {
    super();
    this.#readLogic = readLogic;
    this.#getAccounts = getAccounts;
  }

  #buildRequestDto = (httpRequest: Request): ReadLogicRequestDto => ({
    id: httpRequest.params.id,
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo, jwt: string): ReadLogicAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadLogicController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadLogicController.getUserAccountInfo(jwt, this.#getAccounts);

      if (!getUserAccountInfoResult.success)
        return ReadLogicController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadLogicRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);

      const useCaseResult: ReadLogicResponseDto = await this.#readLogic.execute(
        requestDto,
        authDto,
      );

      if (!useCaseResult.success) {
        return ReadLogicController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      return ReadLogicController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return ReadLogicController.fail(res, 'Internal error occurred while reading logic');
    }
  }
}
