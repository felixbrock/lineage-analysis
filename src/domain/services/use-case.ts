export default interface IUseCase<IRequest, IResponse, IAuth> {
  execute(request: IRequest, auth: IAuth): Promise<IResponse> | IResponse;
  // eslint-disable-next-line semi
}