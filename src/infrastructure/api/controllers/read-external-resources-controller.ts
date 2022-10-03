// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  parseExternalResourceType,
} from '../../../domain/entities/external-resource';
import {
  ReadExternalResources,
  ReadExternalResourcesAuthDto,
  ReadExternalResourcesRequestDto,
  ReadExternalResourcesResponseDto,
} from '../../../domain/external-resource/read-external-resources';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadExternalResourcesController extends BaseController {
  readonly #readExternalResources: ReadExternalResources;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(
    readExternalResources: ReadExternalResources,
    getAccounts: GetAccounts,
    dbo: Dbo
  ) {
    super();
    this.#readExternalResources = readExternalResources;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (
    httpRequest: Request
  ): ReadExternalResourcesRequestDto => {
    const { name, type, lineageId, targetOrganizationId } = httpRequest.query;

    if (typeof lineageId !== 'string')
      throw new Error('Query param lineageId needs to be of type string');

    return {
      type:
        typeof type === 'string' ? parseExternalResourceType(type) : undefined,
      name: typeof name === 'string' ? name : undefined,
      targetOrganizationId:
        typeof targetOrganizationId === 'string'
          ? targetOrganizationId
          : undefined,
      lineageId,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo
  ): ReadExternalResourcesAuthDto => ({
    callerOrganizationId: userAccountInfo.callerOrganizationId,
    isSystemInternal: userAccountInfo.isSystemInternal,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadExternalResourcesController.unauthorized(
          res,
          'Unauthorized'
        );

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadExternalResourcesController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return ReadExternalResourcesController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadExternalResourcesRequestDto =
        this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value);

      const useCaseResult: ReadExternalResourcesResponseDto =
        await this.#readExternalResources.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return ReadExternalResourcesController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) =>
            element.toDto()
          )
        : useCaseResult.value;

      return ReadExternalResourcesController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      return ReadExternalResourcesController.fail(
        res,
        'Internal error occurred while reading externalresources'
      );
    }
  }
}
