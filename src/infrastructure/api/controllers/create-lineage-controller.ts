// TODO: Violation of control flow. DI for express instead
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
  CodeHttp,
  InternalInvokeController,
  Request,
  Response,
  UserAccountInfo,
} from '../../shared/internal-invoke-controller';

export default class CreateLineageController extends InternalInvokeController<CreateLineageRequestDto> {
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
        await CreateLineageController.getUserAccountInfo(
          jwt,
          this.#getAccounts
        );

      if (!getUserAccountInfoResult.success)
        return CreateLineageController.unauthorized(
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      if (!getUserAccountInfoResult.value.isSystemInternal)
        return CreateLineageController.unauthorized('Unauthorized');

      const authDto = this.#buildAuthDto(jwt, getUserAccountInfoResult.value);

      const useCaseResult: CreateLineageResponseDto =
        await this.#createLineage.execute(
          req.req,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return CreateLineageController.badRequest(useCaseResult.error);
      }

      const resultValue = useCaseResult.value
        ? buildLineageDto(useCaseResult.value)
        : useCaseResult.value;

      return CreateLineageController.ok(resultValue, CodeHttp.CREATED);

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
      if (typeof error === 'string') return CreateLineageController.fail(error);
      if (error instanceof Error) return CreateLineageController.fail(error.message);
      return CreateLineageController.fail('Unknown error occured');
    }
  }
}
