// TODO: Violation of control flow. DI for express instead
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  CreateLineage,
  CreateLineageAuthDto,
  CreateLineageRequestDto,
  CreateLineageResponseDto,
} from '../../../domain/lineage/create-lineage/create-lineage';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  CodeHttp,
  InternalInvokeController,
  Request,
  Response,
  UserAccountInfo,
} from '../../../shared/internal-invoke-controller';

export default class InternalInvokeCreateLineageController extends InternalInvokeController<CreateLineageRequestDto> {
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

  #transformReq = (req: CreateLineageRequestDto): CreateLineageRequestDto => {
    const isBase64 = (content: string): boolean =>
      Buffer.from(content, 'base64').toString('base64') === content;
    const toUtf8 = (content: string): string =>
      Buffer.from(content, 'base64').toString('utf8');

    // https://stackoverflow.com/questions/50966023/which-variant-of-base64-encoding-is-created-by-buffer-tostringbase64
    if (!isBase64(req.catalog) || !isBase64(req.manifest))
      throw new Error(
        'Catalog of manifest not in base64 format or in wrong base64 variant (required variant: RFC 4648 §4)'
      );

    return {
      ...req,
      catalog: toUtf8(req.catalog),
      manifest: toUtf8(req.manifest),
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
        await InternalInvokeCreateLineageController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return InternalInvokeCreateLineageController.unauthorized(
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      if (!getUserAccountInfoResult.value.isSystemInternal)
        return InternalInvokeCreateLineageController.unauthorized('Unauthorized');

      const authDto = this.#buildAuthDto(jwt, getUserAccountInfoResult.value);

      const useCaseResult: CreateLineageResponseDto =
        await this.#createLineage.execute(
          this.#transformReq(req.req),
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return InternalInvokeCreateLineageController.badRequest();
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.toDto()
        : useCaseResult.value;

      return InternalInvokeCreateLineageController.ok(resultValue, CodeHttp.CREATED);

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
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return InternalInvokeCreateLineageController.fail('Internal error occurred while invoking create lineage operation');
    }
  }
}
