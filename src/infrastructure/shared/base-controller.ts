import { Request, Response } from 'express';
import jsonwebtoken from 'jsonwebtoken';
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
  userId: string;
  accountId: string;
  organizationId: string;
}

export abstract class BaseController {
  public static jsonResponse(
    res: Response,
    code: number,
    message: string
  ): Response {
    return res.status(code).json({ message });
  }

  public async execute(req: Request, res: Response): Promise<void | Response> {
    try {
      await this.executeImpl(req, res);
    } catch (error) {
      BaseController.fail(res, 'An unexpected error occurred');
    }
  }

  public static async getUserAccountInfo(
    jwt: string,
    getAccounts: GetAccounts
  ): Promise<Result<UserAccountInfo>> {
    if (!jwt) return Result.fail('Unauthorized');

    const authPayload = jsonwebtoken.decode(jwt, { json: true });

    if (!authPayload) return Result.fail('Unauthorized - No auth payload');

    try {
      const getAccountsResult: GetAccountsResponseDto =
        await getAccounts.execute(
          {
            userId: authPayload.username,
          },
          { jwt }
        );

      if (!getAccountsResult.value)
        throw new Error(`No account found for ${authPayload.username}`);
      if (!getAccountsResult.value.length)
        throw new Error(`No account found for ${authPayload.username}`);

      return Result.ok({
        userId: authPayload.username,
        accountId: getAccountsResult.value[0].id,
        organizationId: getAccountsResult.value[0].organizationId,
      });
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }

  public static ok<T>(res: Response, dto?: T, created?: CodeHttp): Response {
    const codeHttp: CodeHttp = created || CodeHttp.OK;
    if (dto) {
      res.type('application/json');

      return res.status(codeHttp).json(dto);
    }
    return res.sendStatus(codeHttp);
  }

  public static badRequest(res: Response, message?: string): Response {
    return BaseController.jsonResponse(
      res,
      CodeHttp.BAD_REQUEST,
      message || 'BadRequest'
    );
  }

  public static unauthorized(res: Response, message?: string): Response {
    return BaseController.jsonResponse(
      res,
      CodeHttp.UNAUTHORIZED,
      message || 'Unauthorized'
    );
  }

  public static notFound(res: Response, message?: string): Response {
    return BaseController.jsonResponse(
      res,
      CodeHttp.NOT_FOUND,
      message || 'Not found'
    );
  }

  public static fail(res: Response, error: Error | string): Response {
    return res.status(CodeHttp.SERVER_ERROR).json({
      message: error.toString(),
    });
  }

  protected abstract executeImpl(
    req: Request,
    res: Response
  ): Promise<Response>;
}
