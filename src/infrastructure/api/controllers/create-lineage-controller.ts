// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  CreateLineage,
  CreateLineageAuthDto,
  CreateLineageRequestDto,
} from '../../../domain/lineage/create-lineage';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class CreateLineageController extends BaseController {
  readonly #createLineage: CreateLineage;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(
    createLineage: CreateLineage,
    getAccounts: GetAccounts,
    dbo: Dbo
  ) {
    super();
    this.#createLineage = createLineage;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): CreateLineageRequestDto => ({
    lineageId: httpRequest.body.lineageId,
    lineageCreatedAt: httpRequest.body.lineageCreatedAt,
    targetOrganizationId: httpRequest.body.targetOrganizationId,
    catalog: httpRequest.body.catalog,
    manifest: httpRequest.body.manifest,
    biType: httpRequest.body.biType,
  });

  #buildAuthDto = (
    jwt: string,
    userAccountInfo: UserAccountInfo
  ): CreateLineageAuthDto => ({
    jwt,
    isSystemInternal: userAccountInfo.isSystemInternal,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return CreateLineageController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await CreateLineageController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return CreateLineageController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      if (!getUserAccountInfoResult.value.isSystemInternal)
        return CreateLineageController.unauthorized(res, 'Unauthorized');

      const requestDto: CreateLineageRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(jwt, getUserAccountInfoResult.value);

      this.#createLineage
        .execute(requestDto, authDto, this.#dbo.dbConnection)
        .then((result) => {
          if (!result.success) console.error(result.error);
        })
        .catch((err) => console.error(err));

      // if (!useCaseResult.success) {
      //   return CreateLineageController.badRequest(res, useCaseResult.error);
      // }

      // const resultValue = useCaseResult.value
      //   ? buildLineageDto(useCaseResult.value)
      //   : useCaseResult.value;

      return CreateLineageController.ok(res, 'Lineage creation is in progress...', CodeHttp.OK);
    } catch (error: unknown) {
      console.error(error);
      if (typeof error === 'string')
        return CreateLineageController.fail(res, error);
      if (error instanceof Error)
        return CreateLineageController.fail(res, error);
      return CreateLineageController.fail(res, 'Unknown error occured');
    }
  }
}
