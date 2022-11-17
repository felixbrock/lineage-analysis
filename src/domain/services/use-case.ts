import { ConnectionPool } from "../snowflake-api/i-snowflake-api-repo";

export default interface IUseCase<IRequest, IResponse, IAuth> {
  execute(request: IRequest, auth: IAuth, connPool?: ConnectionPool
    ): Promise<IResponse> | IResponse;
  // eslint-disable-next-line semi
}
