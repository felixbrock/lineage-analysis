// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import { MaterializationType } from '../../../domain/entities/materialization';
import { buildMaterializationDto } from '../../../domain/materialization/materialization-dto';
import {
  ReadMaterializations,
  ReadMaterializationsAuthDto,
  ReadMaterializationsRequestDto,
  ReadMaterializationsResponseDto,
} from '../../../domain/materialization/read-materializations';
import { IDb } from '../../../domain/services/i-db';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from '../../shared/base-controller';

export default class ReadMaterializationsController extends BaseController {
  readonly #readMaterializations: ReadMaterializations;

  readonly #getAccounts: GetAccounts;

  readonly #db: IDb;

  constructor(
    readMaterializations: ReadMaterializations,
    getAccounts: GetAccounts,
    db: IDb
  ) {
    super();
    this.#readMaterializations = readMaterializations;
    this.#getAccounts = getAccounts;
    this.#db = db;
  }

  #buildRequestDto = (httpRequest: Request): ReadMaterializationsRequestDto => {
    const {
      dbtModelId,
      materializationType,
      name,
      schemaName,
      databaseName,
      logicId,
      lineageId,
    } = httpRequest.query;

    const isMaterializationType = (
      queryParam: string
    ): queryParam is MaterializationType => queryParam in MaterializationType;

    if (materializationType) {
      if (typeof materializationType !== 'string')
        throw new TypeError(
          'When querying materializations the lineageId query param must be of type string'
        );

      if (!isMaterializationType(materializationType))
        throw new TypeError(
          'When querying materializations the materializationType needs to be of type MaterializationType'
        );
    }

    if (!lineageId)
      throw new TypeError(
        'When querying materializations the lineageId must be provided'
      );
    if (typeof lineageId !== 'string')
      throw new TypeError(
        'When querying materializations the lineageId query param must be of type string'
      );

    return {
      dbtModelId: typeof dbtModelId === 'string' ? dbtModelId : undefined,
      materializationType:
        materializationType && isMaterializationType(materializationType)
          ? materializationType
          : undefined,
      name: typeof name === 'string' ? name : undefined,
      schemaName: typeof schemaName === 'string' ? schemaName : undefined,
      databaseName: typeof databaseName === 'string' ? databaseName : undefined,
      logicId: typeof logicId === 'string' ? logicId : undefined,
      lineageId,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo
  ): ReadMaterializationsAuthDto => ({
    organizationId: userAccountInfo.organizationId,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      // const authHeader = req.headers.authorization;

      // if (!authHeader)
      //   return ReadMaterializationsController.unauthorized(res, 'Unauthorized');

      // const jwt = authHeader.split(' ')[1];

      // const getUserAccountInfoResult: Result<UserAccountInfo> =
      //   await ReadMaterializationsInfoController.getUserAccountInfo(
      //     jwt,
      //     this.#getAccounts
      //   );

      // if (!getUserAccountInfoResult.success)
      //   return ReadMaterializationsInfoController.unauthorized(
      //     res,
      //     getUserAccountInfoResult.error
      //   );
      // if (!getUserAccountInfoResult.value)
      //   throw new ReferenceError('Authorization failed');

      const requestDto: ReadMaterializationsRequestDto =
        this.#buildRequestDto(req);
      // const authDto: ReadMaterializationsAuthDto = this.#buildAuthDto(
      //   getUserAccountResult.value
      // );
      const client = this.#db.createClient();
      const dbConnection = await this.#db.connect(client);

      const useCaseResult: ReadMaterializationsResponseDto =
        await this.#readMaterializations.execute(
          requestDto,
          {
            organizationId: 'todo',
          },
          dbConnection
        );

      await this.#db.close(client);

      if (!useCaseResult.success) {
        return ReadMaterializationsController.badRequest(
          res,
          useCaseResult.error
        );
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => buildMaterializationDto(element))
        : useCaseResult.value;

      return ReadMaterializationsController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (typeof error === 'string')
        return ReadMaterializationsController.fail(res, error);
      if (error instanceof Error)
        return ReadMaterializationsController.fail(res, error);
      return ReadMaterializationsController.fail(res, 'Unknown error occured');
    }
  }
}
