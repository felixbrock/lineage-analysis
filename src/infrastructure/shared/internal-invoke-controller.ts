import jsonwebtoken from 'jsonwebtoken';
import jwksClient from 'jwks-rsa';
import { appConfig } from '../../config';
import {
  GetAccounts,
  GetAccountsResponseDto,
} from '../../domain/account-api/get-accounts';
import Result from '../../domain/value-types/transient-types/result';

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
  callerOrganizationId?: string;
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

  static async getUserAccountInfo(
    jwt: string,
    getAccounts: GetAccounts
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
          callerOrganizationId: undefined,
          userId: undefined,
        });

      const getAccountsResult: GetAccountsResponseDto =
        await getAccounts.execute(
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
        callerOrganizationId: getAccountsResult.value[0].organizationId,
        isSystemInternal,
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
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
