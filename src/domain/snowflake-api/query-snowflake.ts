import { Blob } from 'node:buffer';
import Result from '../value-types/transient-types/result';

import {
  Binds,
  IConnectionPool,
  ISnowflakeApiRepo,
  SnowflakeQueryResult,
} from './i-snowflake-api-repo';
import BaseAuth from '../services/base-auth';
import IUseCase from '../services/use-case';

export interface QuerySnowflakeRequestDto {
  queryText: string;
  binds: Binds;
}

export type QuerySnowflakeAuthDto = BaseAuth;

export type QuerySnowflakeResponseDto = Result<SnowflakeQueryResult>;

export class QuerySnowflake
  implements
    IUseCase<
      QuerySnowflakeRequestDto,
      QuerySnowflakeResponseDto,
      QuerySnowflakeAuthDto,
      IConnectionPool
    >
{
  readonly #snowflakeApiRepo: ISnowflakeApiRepo;

  constructor(snowflakeApiRepo: ISnowflakeApiRepo) {
    this.#snowflakeApiRepo = snowflakeApiRepo;
  }

  #splitBinds = (queryTextSize: number, binds: Binds): Binds[] => {
    // todo - Upload as file and then copy into table
    const byteToMBDivisor = 1000000;
    const maxQueryMBSize = 1;
    const querySizeOffset = 0.1;
    const maxQueryTextMBSize = 0.2;

    const queryTextMBSize = queryTextSize / byteToMBDivisor;
    const bindsSize = new Blob([JSON.stringify(binds)]).size;
    const bindsMBSize = bindsSize / byteToMBDivisor;

    if (queryTextMBSize + bindsMBSize < maxQueryMBSize * (1 - querySizeOffset))
      return [binds];
    if (queryTextMBSize > maxQueryTextMBSize)
      throw new Error('Query text size too large. Implement file upload');

    // in MB (subtracting offset)
    const maxSize = 1 * (1 - querySizeOffset);
    const maxBindsSequenceMBSize = maxSize - queryTextMBSize;

    const numSequences = Math.ceil(bindsMBSize / maxBindsSequenceMBSize);
    const numElementsPerSequence = Math.ceil(binds.length / numSequences);

    const res: Binds[] = [];
    for (let i = 0; i < binds.length; i += numElementsPerSequence) {
      const chunk = binds.slice(i, i + numElementsPerSequence);
      res.push(chunk);
    }

    return res;
  };

  async execute(
    request: QuerySnowflakeRequestDto,
    auth: QuerySnowflakeAuthDto,
    connPool: IConnectionPool
  ): Promise<QuerySnowflakeResponseDto> {
    try {
      console.log(request.queryText);
      console.log(request.binds.length);

      const bindSequences = this.#splitBinds(
        new Blob([request.queryText]).size,
        request.binds
      );

      const results = await Promise.all(
        bindSequences.map(async (el) => {
          const res = await this.#snowflakeApiRepo.runQuery(
            request.queryText,
            el,
            connPool
          );

          return res;
        })
      );

      return Result.ok(results.flat());
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Result.fail('');
    }
  }
}
