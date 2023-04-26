// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
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
} from './shared/base-controller';
import Dbo from '../../persistence/db/mongo-db';

export default class ReadLineageController extends BaseController {
  readonly #readLineage: ReadLineage;

  readonly #dbo: Dbo;
  
  constructor(
    readLineage: ReadLineage,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile,
    dbo: Dbo
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readLineage = readLineage;
    this.#dbo = dbo;
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
        Number.isNaN(Number(minuteTolerance)))
    )
      throw new Error('MinuteTolerance param in wrong format');

    return {
      id: httpRequest.params.id,
      tolerateIncomplete: tolerateIncomplete === 'true',
      minuteTolerance: Number(minuteTolerance),
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
        await this.getUserAccountInfo(jwt);

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
        await this.#readLineage.execute(requestDto, authDto, this.#dbo.dbConnection);

      await this.#dbo.releaseConnections();

      if (!useCaseResult.success) {
        return ReadLineageController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      return ReadLineageController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadLineageController.fail(
        res,
        'Internal error occurred while reading lineage'
      );
    }
  }
}
