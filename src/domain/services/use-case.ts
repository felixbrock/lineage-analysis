export default interface IUseCase<IRequest, IResponse, IAuth, IDb = undefined> {
  execute(request: IRequest, auth: IAuth, iDb: IDb
    ): Promise<IResponse> | IResponse;
  // eslint-disable-next-line semi
}
