// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { ParseSQL } from '../../../domain/sql-parser-api/parse-sql';
import {
  ReadTable,
  ReadTableAuthDto,
  ReadTableRequestDto,
  ReadTableResponseDto,
} from '../../../domain/table/read-table';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadTableController extends BaseController {
  #readTable: ReadTable;

  #getAccounts: GetAccounts;

  #parseSQL: ParseSQL;

  constructor(readTable: ReadTable, getAccounts: GetAccounts, parseSQL: ParseSQL) {
    super();
    this.#readTable = readTable;
    this.#getAccounts = getAccounts;
    this.#parseSQL = parseSQL;
  }

  #buildRequestDto = (httpRequest: Request): ReadTableRequestDto => ({
    id: httpRequest.params.tableId
  });

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadTableAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {      
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadTableController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];     

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadTableController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadTableController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new Error('Authorization failed');

      const requestDto: ReadTableRequestDto = this.#buildRequestDto(req);
      // const authDto: ReadTableAuthDto = this.#buildAuthDto(
      //   getUserAccountInfoResult.value
      // );

      const useCaseResult: ReadTableResponseDto =
        await this.#readTable.execute(requestDto, {organizationId:'authDto'});

      if (!useCaseResult.success) {
        return ReadTableController.badRequest(res, useCaseResult.error);
      }

      return ReadTableController.ok(res, useCaseResult.value, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadTableController.fail(res, error);
      if (error instanceof Error)
        return ReadTableController.fail(res, error);
      return ReadTableController.fail(res, 'Unknown error occured');
    }
  }
}
