// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { buildLogicDto } from '../../../domain/logic/logic-dto';
import {
  ReadLogic,
  ReadLogicAuthDto,
  ReadLogicRequestDto,
  ReadLogicResponseDto,
} from '../../../domain/logic/read-logic';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

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

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadLogicAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadLogicController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadLogicInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadLogicInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: ReadLogicRequestDto = this.#buildRequestDto(req);
      // const authDto: ReadLogicAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );

      const useCaseResult: ReadLogicResponseDto = await this.#readLogic.execute(
        requestDto,
        {
          organizationId: 'todo',
        }
      );

      if (!useCaseResult.success) {
        return ReadLogicController.badRequest(res, useCaseResult.error);
      }

      const resultValue = useCaseResult.value
        ? buildLogicDto(useCaseResult.value)
        : useCaseResult.value;

      return ReadLogicController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadLogicController.fail(res, error);
      if (error instanceof Error) return ReadLogicController.fail(res, error);
      return ReadLogicController.fail(res, 'Unknown error occured');
    }
  }
}
