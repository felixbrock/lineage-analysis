// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { buildColumnDto } from '../../../domain/column/column-dto';
import {
  ReadColumns,
  ReadColumnsAuthDto,
  ReadColumnsRequestDto,
  ReadColumnsResponseDto,
} from '../../../domain/column/read-columns';
import Result from '../../../domain/value-types/transient-types/result';
import Dbo from '../../persistence/db/mongo-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadColumnsController extends BaseController {
  readonly #readColumns: ReadColumns;

  readonly #getAccounts: GetAccounts;

  readonly #dbo: Dbo;

  constructor(readColumns: ReadColumns, getAccounts: GetAccounts, dbo: Dbo) {
    super();
    this.#readColumns = readColumns;
    this.#getAccounts = getAccounts;
    this.#dbo = dbo;
  }

  #buildRequestDto = (httpRequest: Request): ReadColumnsRequestDto => {
    const { relationName, name, index, type, materializationId, lineageId, targetOrganizationId } =
      httpRequest.query;

    if (!lineageId)
      throw new TypeError(
        'When querying columns the lineageId must be provided'
      );
    if (typeof lineageId !== 'string')
      throw new TypeError(
        'When querying columns the lineageId query param must be of type string'
      );

    return {
      relationName: typeof relationName === 'string' ? relationName : undefined,
      name: typeof name === 'string' ? name : undefined,
      index: typeof index === 'string' ? index : undefined,
      type: typeof type === 'string' ? type : undefined,
      materializationId:
        typeof materializationId === 'string' ? materializationId : undefined,
      lineageId,
      targetOrganizationId: typeof targetOrganizationId === 'string' ? targetOrganizationId : undefined,
    };
  };

  #buildAuthDto = (userAccountInfo: UserAccountInfo): ReadColumnsAuthDto => ({
      callerOrganizationId: userAccountInfo.callerOrganizationId,
      isSystemInternal: userAccountInfo.isSystemInternal,
    });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadColumnsController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await ReadColumnsController.getUserAccountInfo(jwt, this.#getAccounts);

      if (!getUserAccountInfoResult.success)
        return ReadColumnsController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadColumnsRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value);

      const useCaseResult: ReadColumnsResponseDto =
        await this.#readColumns.execute(
          requestDto,
          authDto,
          this.#dbo.dbConnection
        );

      if (!useCaseResult.success) {
        return ReadColumnsController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => buildColumnDto(element))
        : useCaseResult.value;

      return ReadColumnsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      return ReadColumnsController.fail(res, 'Internal error occurred while reading columns');
    }
  }
}
