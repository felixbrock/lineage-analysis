// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadLineageSnapshot,
  ReadLineageSnapshotAuthDto,
  ReadLineageSnapshotRequestDto,
  ReadLineageSnapshotResponseDto,
} from '../../../domain/lineage/read-lineage-snapshot';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadLineageSnapshotController extends BaseController {
  readonly #readLineageSnapshot: ReadLineageSnapshot;

  readonly #getAccounts: GetAccounts;

  constructor(readLineageSnapshot: ReadLineageSnapshot, getAccounts: GetAccounts) {
    super();
    this.#readLineageSnapshot = readLineageSnapshot;
    this.#getAccounts = getAccounts;
  }

  #buildRequestDto = (httpRequest: Request): ReadLineageSnapshotRequestDto => ({
    lineageId: httpRequest.params.lineageId
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadLineageSnapshotAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadLineageSnapshotController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadLineageSnapshotInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadLineageSnapshotInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: ReadLineageSnapshotRequestDto = this.#buildRequestDto(req);
      // const authDto: ReadLineageSnapshotAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );

      const useCaseResult: ReadLineageSnapshotResponseDto =
        await this.#readLineageSnapshot.execute(requestDto, {
          organizationId: 'todo',
        });

      if (!useCaseResult.success) {
        return ReadLineageSnapshotController.badRequest(res, useCaseResult.error);
      }

      return ReadLineageSnapshotController.ok(res, useCaseResult.value, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadLineageSnapshotController.fail(res, error);
      if (error instanceof Error)
        return ReadLineageSnapshotController.fail(res, error);
      return ReadLineageSnapshotController.fail(res, 'Unknown error occured');
    }
  }
}
