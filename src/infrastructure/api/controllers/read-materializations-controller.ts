// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  MaterializationType,
  materializationTypes,
} from '../../../domain/entities/materialization';
import {
  ReadMaterializations,
  ReadMaterializationsAuthDto,
  ReadMaterializationsRequestDto,
  ReadMaterializationsResponseDto,
} from '../../../domain/materialization/read-materializations';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../../shared/base-controller';

export default class ReadMaterializationsController extends BaseController {
  readonly #readMaterializations: ReadMaterializations;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(
    readMaterializations: ReadMaterializations,
    getAccounts: GetAccounts,
    dbo: Dbo
  ) {
    super();
    this.#readMaterializations = readMaterializations;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadMaterializationsRequestDto => {
    const {
      relationName,
      materializationType,
      name,
      schemaName,
      databaseName,
      logicId,
      lineageId,
      targetOrganizationId,
    } = httpRequest.query;

    const isMaterializationType = (
      queryParam: string
    ): queryParam is MaterializationType => queryParam in materializationTypes;

    if (materializationType) {
      if (typeof materializationType !== 'string')
        throw new TypeError(
          'When querying materializations the lineageId query param must be of type string'
        );

      if (!isMaterializationType(materializationType))
        throw new TypeError(
          'When querying materializations the materializationType needs to be of type MaterializationType'
        );
    }

    if (!lineageId)
      throw new TypeError(
        'When querying materializations the lineageId must be provided'
      );
    if (typeof lineageId !== 'string')
      throw new TypeError(
        'When querying materializations the lineageId query param must be of type string'
      );

    return {
      relationName: typeof relationName === 'string' ? relationName : undefined,
      materializationType:
        materializationType && isMaterializationType(materializationType)
          ? materializationType
          : undefined,
      name: typeof name === 'string' ? name : undefined,
      schemaName: typeof schemaName === 'string' ? schemaName : undefined,
      databaseName: typeof databaseName === 'string' ? databaseName : undefined,
      logicId: typeof logicId === 'string' ? logicId : undefined,
      lineageId,
      targetOrganizationId:
        typeof targetOrganizationId === 'string'
          ? targetOrganizationId
          : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo
  ): ReadMaterializationsAuthDto => ({
    callerOrganizationId: userAccountInfo.callerOrganizationId,
    isSystemInternal: userAccountInfo.isSystemInternal,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadMaterializationsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadMaterializationsController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return ReadMaterializationsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadMaterializationsRequestDto =
        this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value);

      const useCaseResult: ReadMaterializationsResponseDto =
        await this.#readMaterializations.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return ReadMaterializationsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadMaterializationsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      return ReadMaterializationsController.fail(
        res,
        'Internal error occurred while reading materializations'
      );
    }
  }
}
