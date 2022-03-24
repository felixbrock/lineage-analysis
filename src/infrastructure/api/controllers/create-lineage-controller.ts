// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  CreateLineage,
  CreateLineageAuthDto,
  CreateLineageRequestDto,
  CreateLineageResponseDto,
} from '../../../domain/lineage/create-lineage';
import { ParseSQL } from '../../../domain/sql-parser-api/parse-sql';

import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class CreateLineageController extends BaseController {
  #createLineage: CreateLineage;

  #getAccounts: GetAccounts;

  constructor(createLineage: CreateLineage, getAccounts: GetAccounts) {
    super();
    this.#createLineage = createLineage;
    this.#getAccounts = getAccounts;
  }

  #buildRequestDto = (httpRequest: Request): CreateLineageRequestDto => ({
    tableId: httpRequest.params.tableId,
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo): CreateLineageAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return CreateLineageController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await CreateLineageInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return CreateLineageInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: CreateLineageRequestDto = this.#buildRequestDto(req);
      // const authDto: CreateLineageAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );

      const useCaseResult: CreateLineageResponseDto =
        await this.#createLineage.execute(requestDto, {
          organizationId: 'todo',
        });

      if (!useCaseResult.success) {
        return CreateLineageController.badRequest(res, useCaseResult.error);
      }

      return CreateLineageController.ok(res, useCaseResult.value, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return CreateLineageController.fail(res, error);
      if (error instanceof Error)
        return CreateLineageController.fail(res, error);
      return CreateLineageController.fail(res, 'Unknown error occured');
    }
  }
}