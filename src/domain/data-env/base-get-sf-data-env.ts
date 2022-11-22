import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';

interface DatabaseRepresentation {
  name: string;
  ownerId: string;
  isTransient: boolean;
  comment?: string;
}

export default abstract class BaseGetSfDataEnv {
  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  /* Get database representations from snowflake */
  protected getDbRepresentations = async (connPool: IConnectionPool, auth: BaseAuth): Promise<DatabaseRepresentation[]> => {
    const queryText =
      "select database_name, database_owner, is_transient, comment from cito.information_schema.databases where not array_contains(database_name::variant, ['SNOWFLAKE', 'SNOWFLAKE_SAMPLE_DATA', 'CITO'])";
    const queryResult = await this.#querySnowflake.execute(
      { queryText, binds: [] },
      auth,
      connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dbRepresentations: DatabaseRepresentation[] = results.map((el) => {
      const {
        DATABASE_NAME: name,
        DATABASE_OWNER: ownerId,
        IS_TRANSIENT: isTransient,
        COMMENT: comment,
      } = el;

      const isComment = (val: unknown): val is string | undefined =>
        !val || typeof val === 'string';

      if (
        typeof name !== 'string' ||
        typeof ownerId !== 'string' ||
        typeof isTransient !== 'string' ||
        !['yes', 'no'].includes(isTransient.toLowerCase()) ||
        !isComment(comment)
      )
        throw new Error(
          'Received mat representation field value in unexpected format'
        );

      return {
        name: name.toLowerCase(),
        ownerId,
        isTransient: isTransient.toLowerCase() !== 'no',
        comment: comment || undefined,
      };
    });

    return dbRepresentations;
  };
}
