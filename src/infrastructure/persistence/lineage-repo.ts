import {
  Auth,
  ILineageRepo,
  LineageUpdateDto,
} from '../../domain/lineage/i-lineage-repo';
import { Lineage, LineageProperties } from '../../domain/entities/lineage';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';

export default class LineageRepo implements ILineageRepo {
  readonly #matName = 'lineage';

  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  #buildLineage = (sfEntity: SnowflakeEntity): Lineage => {const {
    ID: id,
    COMPLETED: completed,
    CREATED_AT: createdAt,
  } = sfEntity;

  if (
    typeof id !== 'string' ||
    typeof completed !== 'boolean' ||
    typeof createdAt !== 'string'
  )
    throw new Error(
      'Retrieved unexpected lineage field types from persistence'
    );

  return this.#toEntity({ id, completed, createdAt });};

  findOne = async (
    lineageId: string,
    targetOrgId: string,
    auth: Auth
  ): Promise<Lineage | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName}
      } where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [lineageId];

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length > 1)
        throw new Error(`Multiple lineage entities with id found`);

      return !result.value.length ? null : this.#buildLineage(result.value[0]);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findLatest = async (
    filter: { completed: boolean },
    targetOrgId: string,
    auth: Auth
  ): Promise<Lineage | null> => {
    try {
      const queryText = `select * from cito.lineage.${
        this.#matName
      } where completed = ? order by executed_at desc limit 1;`;

      const binds = [filter.completed.toString()];

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no lineage entities with id found`);

      const {
        ID: id,
        COMPLETED: completed,
        CREATED_AT: createdAt,
      } = result.value[0];

      if (
        typeof id !== 'string' ||
        typeof completed !== 'boolean' ||
        typeof createdAt !== 'string'
      )
        throw new Error(
          'Retrieved unexpected lineage field types from persistence'
        );

      return this.#toEntity({ id, completed, createdAt });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (targetOrgId: string, auth: Auth): Promise<Lineage[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds: [] },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return result.value.map((el) => {
        const { ID: id, COMPLETED: completed, CREATED_AT: createdAt } = el;

        if (
          typeof id !== 'string' ||
          typeof completed !== 'boolean' ||
          typeof createdAt !== 'string'
        )
          throw new Error(
            'Retrieved unexpected lineage field types from persistence'
          );

        return this.#toEntity({ id, completed, createdAt });
      });
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertOne = async (
    lineage: Lineage,
    targetOrgId: string,
    auth: Auth
  ): Promise<string> => {
    const colDefinitions: ColumnDefinition[] = [
      { name: 'id' },
      { name: 'created_at' },
      { name: 'completed' },
    ];

    const row = `(?, ?, ?)`;
    const binds = [lineage.id, lineage.createdAt, lineage.completed.toString()];

    try {
      const queryText = getInsertQuery(this.#matName, colDefinitions, [row]);

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return lineage.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  updateOne = async (
    lineageId: string,
    updateDto: LineageUpdateDto,
    targetOrgId: string,
    auth: Auth
  ): Promise<string> => {
    const colDefinitions: ColumnDefinition[] = [{ name: 'id' }];
    const binds = [lineageId];

    if (updateDto.completed) {
      colDefinitions.push({ name: 'completed' });
      binds.push(updateDto.completed.toString());
    }

    try {
      const queryText = getUpdateQuery(this.#matName, colDefinitions, [
        `(${binds.map(() => '?').join(', ')})`,
      ]);

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return lineageId;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #toEntity = (lineageProperties: LineageProperties): Lineage =>
    Lineage.create(lineageProperties);
}
