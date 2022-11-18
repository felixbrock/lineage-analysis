import { IConnectionPool } from "../snowflake-api/i-snowflake-api-repo";

export default interface IUseCase<IRequest, IResponse, IAuth> {
  execute(request: IRequest, auth: IAuth, connPool?: IConnectionPool
    ): Promise<IResponse> | IResponse;
  // eslint-disable-next-line semi
}
