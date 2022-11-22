export default interface IUseCase<IRequest, IResponse, IAuth, IDbConnection = undefined> {
  execute(request: IRequest, auth: IAuth, connPool: IDbConnection
    ): Promise<IResponse> | IResponse;
  // eslint-disable-next-line semi
}
