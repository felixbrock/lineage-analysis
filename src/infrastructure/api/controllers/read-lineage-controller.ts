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
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../../shared/base-controller';

export default class ReadLineageController extends BaseController {
  readonly #readLineage: ReadLineage;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(readLineage: ReadLineage, getAccounts: GetAccounts, dbo: Dbo) {
    super();
    this.#readLineage = readLineage;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadLineageRequestDto => ({
    id: httpRequest.params.id,
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadLineageAuthDto => {
    if (!userAccountInfo.callerOrganizationId) throw new Error('Unauthorized');

    return {
      callerOrganizationId: userAccountInfo.callerOrganizationId,
    };
  };

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
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value);

      const useCaseResult: ReadLineageResponseDto =
        await this.#readLineage.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return ReadLineageController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      return ReadLineageController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      return ReadLineageController.fail(res, 'Internal error occurred while reading lineage');
    }
  }
}
