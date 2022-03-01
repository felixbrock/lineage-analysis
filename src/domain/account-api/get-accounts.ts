import { URLSearchParams } from 'url';
import Result from '../value-types/transient-types/result';
import IUseCase from '../services/use-case';
import { IAccountApiRepository } from './i-account-api-repository';
import { AccountDto } from './account-dto';

export interface GetAccountsRequestDto {
  userId: string;
}

export interface GetAccountsAuthDto {
  jwt: string;
}

export type GetAccountsResponseDto = Result<AccountDto[]>;

export class GetAccounts
  implements
    IUseCase<GetAccountsRequestDto, GetAccountsResponseDto, GetAccountsAuthDto>
{
  #accountApiRepository: IAccountApiRepository;

  public constructor(accountApiRepository: IAccountApiRepository) {
    this.#accountApiRepository = accountApiRepository;
  }

  public async execute(
    request: GetAccountsRequestDto,
    auth: GetAccountsAuthDto
  ): Promise<GetAccountsResponseDto> {
    try {
      const getAccountsResponse: AccountDto[] =
        await this.#accountApiRepository.getBy(
          new URLSearchParams({ userId: request.userId }),
          auth.jwt
        );

      if (!getAccountsResponse.length)
        throw new Error(`No accounts found for user id ${request.userId}`);

      return Result.ok(getAccountsResponse);
    } catch (error: unknown) {
      if (typeof error === 'string') return Result.fail(error);
      if (error instanceof Error) return Result.fail(error.message);
      return Result.fail('Unknown error occured');
    }
  }
}
