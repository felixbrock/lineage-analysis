// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { buildLineageDto } from '../../../domain/lineage/lineage-dto';
import {
  ReadLineage,
  ReadLineageAuthDto,
  ReadLineageRequestDto,
  ReadLineageResponseDto,
} from '../../../domain/lineage/read-lineage';
import { IDb } from '../../../domain/services/i-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadLineageController extends BaseController {
  readonly #readLineage: ReadLineage;

  readonly #getAccounts: GetAccounts;

  readonly #db: IDb;

  constructor(readLineage: ReadLineage, getAccounts: GetAccounts, db: IDb) {
    super();
    this.#readLineage = readLineage;
    this.#getAccounts = getAccounts;
    this.#db = db;
  }

  #buildRequestDto = (httpRequest: Request): ReadLineageRequestDto => ({
    id: httpRequest.params.id,
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadLineageAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadLineageController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadLineageInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadLineageInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: ReadLineageRequestDto = this.#buildRequestDto(req);
      // const authDto: ReadLineageAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );
      const client = this.#db.createClient();
      const dbConnection = await this.#db.connect(client);

      const useCaseResult: ReadLineageResponseDto =
        await this.#readLineage.execute(
          requestDto,
          {
            organizationId: 'todo',
          },
          dbConnection
        );

      await this.#db.close(client);

      if (!useCaseResult.success) {
        return ReadLineageController.badRequest(res, useCaseResult.error);
      }

      const resultValue = useCaseResult.value
        ? buildLineageDto(useCaseResult.value)
        : useCaseResult.value;

      return ReadLineageController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadLineageController.fail(res, error);
      if (error instanceof Error) return ReadLineageController.fail(res, error);
      return ReadLineageController.fail(res, 'Unknown error occured');
    }
  }
}
