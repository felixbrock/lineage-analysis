// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadSelector,
  ReadSelectorAuthDto,
  ReadSelectorRequestDto,
  ReadSelectorResponseDto,
} from '../../../domain/selector/read-selector';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadSelectorController extends BaseController {
  #readSelector: ReadSelector;

  #getAccounts: GetAccounts;

  public constructor(readSelector: ReadSelector, getAccounts: GetAccounts) {
    super();
    this.#readSelector = readSelector;
    this.#getAccounts = getAccounts;
  }

  // eslint-disable-next-line class-methods-use-this
  #buildRequestDto = (httpRequest: Request): ReadSelectorRequestDto => ({
    id: httpRequest.params.selectorId,
  });

  // eslint-disable-next-line class-methods-use-this
  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadSelectorAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadSelectorController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];     

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadSelectorController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return ReadSelectorController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new Error('Authorization failed');

      const requestDto: ReadSelectorRequestDto = this.#buildRequestDto(req);
      const authDto: ReadSelectorAuthDto = this.#buildAuthDto(
        getUserAccountInfoResult.value
      );

      const useCaseResult: ReadSelectorResponseDto =
        await this.#readSelector.execute(requestDto, authDto);

      if (!useCaseResult.success) {
        return ReadSelectorController.badRequest(res, useCaseResult.error);
      }

      return ReadSelectorController.ok(res, useCaseResult.value, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadSelectorController.fail(res, error);
      if (error instanceof Error)
        return ReadSelectorController.fail(res, error);
      return ReadSelectorController.fail(res, 'Unknown error occured');
    }
  }
}
