// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  MaterializationType,
  materializationTypes,
} from '../../../domain/entities/materialization';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import {
  ReadMaterializations,
  ReadMaterializationsAuthDto,
  ReadMaterializationsRequestDto,
  ReadMaterializationsResponseDto,
} from '../../../domain/materialization/read-materializations';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from './shared/base-controller';
import Dbo from '../../persistence/db/mongo-db';

export default class ReadMaterializationsController extends BaseController {
  readonly #readMaterializations: ReadMaterializations;

  readonly #dbo: Dbo;

  constructor(
    readMaterializations: ReadMaterializations,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile,
    dbo: Dbo
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readMaterializations = readMaterializations;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadMaterializationsRequestDto => {
    const {
      relationName,
      materializationType,
      names,
      schemaName,
      databaseName,
      logicId,
      targetOrgId,
    } = httpRequest.query;

    const isMaterializationType = (
      queryParam: string
    ): queryParam is MaterializationType => queryParam in materializationTypes;

    if (materializationType) {
      if (typeof materializationType !== 'string')
        throw new TypeError(
          'When querying materializations the matType query param must be of type string'
        );

      if (!isMaterializationType(materializationType))
        throw new TypeError(
          'When querying materializations the materializationType needs to be of type MaterializationType'
        );
    }

    const isStringArray = (obj: unknown): obj is string[] =>
      Array.isArray(obj) && obj.every((el) => typeof el === 'string');

    if (names && !isStringArray(names))
      throw new Error('names format not accepted');

    return {
      relationName: typeof relationName === 'string' ? relationName : undefined,
      materializationType:
        materializationType && isMaterializationType(materializationType)
          ? materializationType
          : undefined,
      names: typeof names === 'string' ? [names] : names,
      schemaName: typeof schemaName === 'string' ? schemaName : undefined,
      databaseName: typeof databaseName === 'string' ? databaseName : undefined,
      logicId: typeof logicId === 'string' ? logicId : undefined,
      targetOrgId: typeof targetOrgId === 'string' ? targetOrgId : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadMaterializationsAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadMaterializationsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(jwt);

      if (!getUserAccountInfoResult.success)
        return ReadMaterializationsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadMaterializationsRequestDto =
        this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);


      const useCaseResult: ReadMaterializationsResponseDto =
        await this.#readMaterializations.execute(requestDto, authDto, this.#dbo.dbConnection);

      await this.#dbo.releaseConnections();

      if (!useCaseResult.success) {
        return ReadMaterializationsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadMaterializationsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadMaterializationsController.fail(
        res,
        'Internal error occurred while reading materializations'
      );
    }
  }
}
