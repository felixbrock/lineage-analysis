// TODO: Violation of control flow. DI for express instead
import { Request, Response } from 'express';
import { createPool } from 'snowflake-sdk';
import { GetAccounts } from '../../../domain/account-api/get-accounts';
import {
  ReadDependencies,
  ReadDependenciesAuthDto,
  ReadDependenciesRequestDto,
  ReadDependenciesResponseDto,
} from '../../../domain/dependency/read-dependencies';
import {
  DependencyType,
  dependencyTypes,
} from '../../../domain/entities/dependency';
import { GetSnowflakeProfile } from '../../../domain/integration-api/get-snowflake-profile';
import Result from '../../../domain/value-types/transient-types/result';

import {
  BaseController,
  CodeHttp,
  UserAccountInfo,
} from './shared/base-controller';

export default class ReadDependenciesController extends BaseController {
  readonly #readDependencies: ReadDependencies;

  constructor(
    readDependencies: ReadDependencies,
    getAccounts: GetAccounts,
    getSnowflakeProfile: GetSnowflakeProfile
  ) {
    super(getAccounts, getSnowflakeProfile);
    this.#readDependencies = readDependencies;
  }

  #buildRequestDto = (httpRequest: Request): ReadDependenciesRequestDto => {
    const { type, headId, tailId, targetOrgId } = httpRequest.query;

    const isDependencyType = (
      queryParam: string
    ): queryParam is DependencyType => queryParam in dependencyTypes;

    if (type) {
      if (typeof type !== 'string')
        throw new TypeError(
          'When querying dependencies the type query param must be of type string'
        );

      if (!isDependencyType(type))
        throw new TypeError(
          'When querying dependencies the type needs to be of type DependencyType'
        );
    }


    return {
      type: type && isDependencyType(type) ? type : undefined,
      headId: typeof headId === 'string' ? headId : undefined,
      tailId: typeof tailId === 'string' ? tailId : undefined,
      targetOrgId: typeof targetOrgId === 'string' ? targetOrgId : undefined,
    };
  };

  #buildAuthDto = (
    userAccountInfo: UserAccountInfo,
    jwt: string
  ): ReadDependenciesAuthDto => ({
    callerOrgId: userAccountInfo.callerOrgId,
    isSystemInternal: userAccountInfo.isSystemInternal,
    jwt,
  });

  protected async executeImpl(req: Request, res: Response): Promise<Response> {
    try {
      const authHeader = req.headers.authorization;

      if (!authHeader)
        return ReadDependenciesController.unauthorized(res, 'Unauthorized');

      const jwt = authHeader.split(' ')[1];

      const getUserAccountInfoResult: Result<UserAccountInfo> =
        await this.getUserAccountInfo(jwt);

      if (!getUserAccountInfoResult.success)
        return ReadDependenciesController.unauthorized(
          res,
          getUserAccountInfoResult.error
        );
      if (!getUserAccountInfoResult.value)
        throw new ReferenceError('Authorization failed');

      const requestDto: ReadDependenciesRequestDto = this.#buildRequestDto(req);
      const authDto = this.#buildAuthDto(getUserAccountInfoResult.value, jwt);

      const connPool = await this.createConnectionPool(jwt, createPool);

      const useCaseResult: ReadDependenciesResponseDto =
        await this.#readDependencies.execute(requestDto, authDto, connPool);

      await connPool.drain();
      await connPool.clear();

      if (!useCaseResult.success) {
        return ReadDependenciesController.badRequest(res);
      }

      const resultValue = useCaseResult.value
        ? useCaseResult.value.map((element) => element.toDto())
        : useCaseResult.value;

      return ReadDependenciesController.ok(res, resultValue, CodeHttp.OK);
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return ReadDependenciesController.fail(
        res,
        'Internal error occurred while reading dependencies'
      );
    }
  }
}
