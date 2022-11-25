import jsonwebtoken from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { appConfig } from '../../../../config';
import {
  GetAccounts,
  GetAccountsResponseDto,
} from '../../../../domain/account-api/get-accounts';
import { GetSnowflakeProfile } from '../../../../domain/integration-api/get-snowflake-profile';
import { SnowflakeProfileDto } from '../../../../domain/integration-api/i-integration-api-repo';
import { DbOptions } from '../../../../domain/services/i-db';
import { IConnectionPool } from '../../../../domain/snowflake-api/i-snowflake-api-repo';
import Result from '../../../../domain/value-types/transient-types/result';

export enum CodeHttp {
  OK = 200,
  CREATED,
  BAD_REQUEST = 400,
  UNAUTHORIZED,
  FORBIDDEN = 403,
  NOT_FOUND,
  CONFLICT = 409,
  SERVER_ERROR = 500,
}

export interface UserAccountInfo {
  userId?: string;
  accountId?: string;
  callerOrgId?: string;
  isSystemInternal: boolean;
}

export interface Response {
  status: number;
  payload: any;
}

export const internalInvokeTypes = ['create-lineage'] as const;
export type InternalInvokeType = typeof internalInvokeTypes[number];

export const parseInternalInvokeType = (
  internalInvokeType: string
): InternalInvokeType => {
  const identifiedElement = internalInvokeTypes.find(
    (element) => element.toLowerCase() === internalInvokeType.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

export interface Request<R> {
  internalInvokeType: InternalInvokeType;
  req: R;
  auth: { jwt: string };
}

export abstract class InternalInvokeController<R> {
  #getSnowflakeProfile: GetSnowflakeProfile;

  #getAccounts: GetAccounts;

  constructor(getAccounts: GetAccounts, getSnowflakeProfile: GetSnowflakeProfile) {
    this.#getAccounts = getAccounts;
    this.#getSnowflakeProfile = getSnowflakeProfile;
  }

  static jsonResponse(code: number, payload: { [key: string]: any }): Response {
    return { status: code, payload };
  }

  async execute(req: Request<R>): Promise<void | Response> {
    try {
      const res = await this.executeImpl(req);
      return res;
    } catch (error) {
      return InternalInvokeController.fail('An unexpected error occurred');
    }
  }

  #getProfile = async (
    jwt: string,
    targetOrgId?: string
  ): Promise<SnowflakeProfileDto> => {
    const readSnowflakeProfileResult = await this.#getSnowflakeProfile.execute(
      { targetOrgId },
      {
        jwt,
      }
    );

    if (!readSnowflakeProfileResult.success)
      throw new Error(readSnowflakeProfileResult.error);
    if (!readSnowflakeProfileResult.value)
      throw new Error('SnowflakeProfile does not exist');

    return readSnowflakeProfileResult.value;
  };

  protected createConnectionPool = async (
    jwt: string,
    createPool: (
      options: DbOptions,
      poolOptions: { min: number; max: number }
    ) => IConnectionPool,
    targetOrgId?: string
  ): Promise<IConnectionPool> => {
    const profile = await this.#getProfile(jwt, targetOrgId);

    const options: DbOptions = {
      account: profile.accountId,
      password: profile.password,
      username: profile.username,
      warehouse: profile.warehouseName,
    };

    return createPool(options, {
      max: 10,
      min: 0,
    });
  };

  protected async getUserAccountInfo(
    jwt: string,
  ): Promise<Result<UserAccountInfo>> {
    if (!jwt) return Result.fail('Unauthorized');

    try {
      const client = jwksClient({
        jwksUri: `https://cognito-idp.${appConfig.cloud.region}.amazonaws.com/${appConfig.cloud.userPoolId}/.well-known/jwks.json`,
      });

      const unverifiedDecodedAuthPayload = jsonwebtoken.decode(jwt, {
        json: true,
        complete: true,
      });

      if (!unverifiedDecodedAuthPayload) return Result.fail('Unauthorized');

      const { kid } = unverifiedDecodedAuthPayload.header;

      if (!kid) return Result.fail('Unauthorized');

      const key = await client.getSigningKey(kid);
      const signingKey = key.getPublicKey();

      const authPayload = jsonwebtoken.verify(jwt, signingKey, {
        algorithms: ['RS256'],
      });

      if (typeof authPayload === 'string')
        return Result.fail('Unexpected auth payload format');

      const isSystemInternal = authPayload.scope
        ? authPayload.scope.includes('system-internal/system-internal')
        : false;

      if (isSystemInternal)
        return Result.ok({
          isSystemInternal,
          accountId: undefined,
          callerOrgId: undefined,
          userId: undefined,
        });

      const getAccountsResult: GetAccountsResponseDto =
        await this.#getAccounts.execute(
          {
            userId: authPayload.username,
          },
          { jwt }
        );

      if (!getAccountsResult.value)
        throw new ReferenceError(
          `No account found for ${authPayload.username}`
        );
      if (!getAccountsResult.value.length)
        throw new ReferenceError(
          `No account found for ${authPayload.username}`
        );

      console.log(`Requested by ${authPayload.username}`);

      return Result.ok({
        userId: authPayload.username,
        accountId: getAccountsResult.value[0].id,
        callerOrgId: getAccountsResult.value[0].organizationId,
        isSystemInternal,
      });
    } catch (error: unknown) {
      if (error instanceof Error ) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }

  static ok(dto?: { [key: string]: any }, created?: CodeHttp): Response {
    const codeHttp: CodeHttp = created || CodeHttp.OK;
    return InternalInvokeController.jsonResponse(codeHttp, dto || {});
  }

  static badRequest(message?: string): Response {
    return InternalInvokeController.jsonResponse(CodeHttp.BAD_REQUEST, {
      message: message || 'BadRequest',
    });
  }

  static unauthorized(message?: string): Response {
    return InternalInvokeController.jsonResponse(CodeHttp.UNAUTHORIZED, {
      message: message || 'Unauthorized',
    });
  }

  static notFound(message?: string): Response {
    return InternalInvokeController.jsonResponse(CodeHttp.NOT_FOUND, {
      message: message || 'Not found',
    });
  }

  static fail(error: Error | string): Response {
    return InternalInvokeController.jsonResponse(CodeHttp.SERVER_ERROR, {
      message: error.toString(),
    });
  }

  protected abstract executeImpl(req: Request<R>): Promise<Response>;
}
