// TODO: Violation of control flow. DI for express instead
import { createPool } from 'snowflake-sdk';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import {
  CreateLineage,
  CreateLineageAuthDto,
  CreateLineageRequestDto,
  CreateLineageResponseDto,
} from '../../../domain/lineage/create-lineage/create-lineage';
import Result from '../../../domain/value-types/transient-types/result';

import {
  CodeHttp,
  InternalInvokeController,
  Request,
  Response,
  UserAccountInfo,
} from '../../../shared/internal-invoke-controller';

export default class InternalInvokeCreateLineageController extends InternalInvokeController<CreateLineageRequestDto> {
  readonly #createLineage: CreateLineage;

  constructor(createLineage: CreateLineage, getAccounts: GetAccounts, getSnowflakeProfile: GetSnowflakeProfile) {
    super(getAccounts, getSnowflakeProfile);
    this.#createLineage = createLineage;
  }

  #transformReq = (req: CreateLineageRequestDto): CreateLineageRequestDto => {
    const isBase64 = (content: string): boolean =>
      Buffer.from(content, 'base64').toString('base64') === content;
    const toUtf8 = (content: string): string =>
      Buffer.from(content, 'base64').toString('utf8');

    if (!req.dbtCatalog && !req.dbtManifest) return req;

    if (!!req.dbtCatalog !== !!req.dbtManifest)
      throw new Error(
        'In case of dbt based lineage creation, both, the dbtCatalog and dbtManifest file need to be provided'
      );

    // https://stackoverflow.com/questions/50966023/which-variant-of-base64-encoding-is-created-by-buffer-tostringbase64
    if (
      req.dbtCatalog &&
      req.dbtManifest &&
      (!isBase64(req.dbtCatalog) || !isBase64(req.dbtManifest))
    )
      throw new Error(
        'Catalog of manifest not in base64 format or in wrong base64 variant (required variant: RFC 4648 §4)'
      );

    return {
      ...req,
      dbtCatalog: req.dbtCatalog ? toUtf8(req.dbtCatalog) : undefined,
      dbtManifest: req.dbtManifest ? toUtf8(req.dbtManifest) : undefined,
    };
  };

  #buildAuthDto = (
    jwt: string,
    userAccountInfo: UserAccountInfo
  ): CreateLineageAuthDto => ({
    jwt,
    isSystemInternal: userAccountInfo.isSystemInternal,
  });

  protected async executeImpl(
    req: Request<CreateLineageRequestDto>
  ): Promise<Response> {
    try {
      const { jwt } = req.auth;

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(
          jwt,
        );

      if (!getUserAccountInfoResult.success)
        return InternalInvokeCreateLineageController.unauthorized(
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      if (!getUserAccountInfoResult.value.isSystemInternal)
        return InternalInvokeCreateLineageController.unauthorized(
          'Unauthorized'
        );

      const authDto = this.#buildAuthDto(jwt, getUserAccountInfoResult.value);

      const connPool = await this.createConnectionPool(jwt, createPool);

      const useCaseResult: CreateLineageResponseDto =
        await this.#createLineage.execute(this.#transformReq(req.req), authDto, connPool);

      if (!useCaseResult.success) {
        return InternalInvokeCreateLineageController.badRequest();
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      await connPool.drain();

      return InternalInvokeCreateLineageController.ok(
        resultValue,
        CodeHttp.CREATED
      );

      // this.#createLineage
      //   .execute(requestDto, authDto, this.#dbo.)
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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return InternalInvokeCreateLineageController.fail(
        'Internal error occurred while invoking create lineage operation'
      );
    }
  }
}
