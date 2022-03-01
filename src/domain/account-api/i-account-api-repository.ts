import { URLSearchParams } from "url";
import { AccountDto } from "./account-dto";

export interface IAccountApiRepository {
  getBy(params: URLSearchParams, jwt: string): Promise<AccountDto[]>;
}