// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  CreateLineage,
  CreateLineageAuthDto,
  CreateLineageRequestDto,
  CreateLineageResponseDto,
} from '../../../domain/lineage/create-lineage';
import { buildLineageDto } from '../../../domain/lineage/lineage-dto';
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

  #buildRequestDto = (req: Request): CreateLineageRequestDto => {
    const { targetOrganizationId, ...remainingBody } = req.body;

    const isBase64 = (content: string): boolean =>
      Buffer.from(content, 'base64').toString('base64') === content;
    const toUtf8 = (content: string): string =>
      Buffer.from(content, 'base64').toString('utf8');

    // https://stackoverflow.com/questions/50966023/which-variant-of-base64-encoding-is-created-by-buffer-tostringbase64
    if (!isBase64(remainingBody.catalog) || !isBase64(remainingBody.manifest))
      throw new Error(
        'Catalog of manifest not in base64 format or in wrong base64 variant (required variant: RFC 4648 §4)'
      );

    return {
      ...remainingBody,
      catalog: toUtf8(remainingBody.catalog),
      manifest: toUtf8(remainingBody.manifest),
    };
  };

  #buildAuthDto = (
    jwt: string,
    userAccountInfo: UserAccountInfo
  ): CreateLineageAuthDto => ({
    jwt,
    isSystemInternal: userAccountInfo.isSystemInternal,
    callerOrganizationId: userAccountInfo.callerOrganizationId
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

      const requestDto: CreateLineageRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(jwt, getUserAccountInfoResult.value);

      const useCaseResult: CreateLineageResponseDto =
        await this.#createLineage.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return CreateLineageController.badRequest(res, useCaseResult.error);
      }

      const resultValue = useCaseResult.value
        ? buildLineageDto(useCaseResult.value)
        : useCaseResult.value;

      return CreateLineageController.ok(res, resultValue, CodeHttp.CREATED);

      // this.#createLineage
      //   .execute(requestDto, authDto, this.#dbo.dbConnection)
      //   .then((result) => {
      //     if (!result.success) console.error(result.error);
      //   })
      //   .catch((err) => console.error(err));

      // // if (!useCaseResult.success) {
      // //   return CreateLineageController.badRequest(res, useCaseResult.error);
      // // }

      // // const resultValue = useCaseResult.value
      // //   ? buildLineageDto(useCaseResult.value)
      // //   : useCaseResult.value;

      // return CreateLineageController.ok(res, 'Lineage creation is in progress...', CodeHttp.CREATED);
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
