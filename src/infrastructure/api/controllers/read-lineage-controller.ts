// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadLineage,
  ReadLineageAuthDto,
  ReadLineageRequestDto,
  ReadLineageResponseDto,
} from '../../../domain/lineage/read-lineage';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../../shared/base-controller';

export default class ReadLineageController extends BaseController {
  readonly #readLineage: ReadLineage;

  readonly #getAccounts: GetAccounts;

  constructor(readLineage: ReadLineage, getAccounts: GetAccounts) {
    super();
    this.#readLineage = readLineage;
    this.#getAccounts = getAccounts;
  }

  #buildRequestDto = (httpRequest: Request): ReadLineageRequestDto => {
    const { tolerateIncomplete, minuteTolerance } = httpRequest.query;

    if (
      tolerateIncomplete === undefined ||
      typeof tolerateIncomplete !== 'string' ||
      !['true', 'false'].includes(tolerateIncomplete)
    )
      throw new Error('TolerateIncomplete param missing or in wrong format');

      if (
        minuteTolerance &&
        (typeof minuteTolerance !== 'string' ||
        Number.isNaN(minuteTolerance))
      )
        throw new Error('MinuteTolerance param in wrong format');

    return {
      id: httpRequest.params.id,
      tolerateIncomplete: tolerateIncomplete === 'true',
      minuteTolerance: Number(minuteTolerance)
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadLineageAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadLineageController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadLineageController.getUserAccountInfo(jwt, this.#getAccounts);

      if (!getUserAccountInfoResult.success)
        return ReadLineageController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadLineageRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);

      const useCaseResult: ReadLineageResponseDto =
        await this.#readLineage.execute(requestDto, authDto);

      if (!useCaseResult.success) {
        return ReadLineageController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      return ReadLineageController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return ReadLineageController.fail(
        res,
        'Internal error occurred while reading lineage'
      );
    }
  }
}
