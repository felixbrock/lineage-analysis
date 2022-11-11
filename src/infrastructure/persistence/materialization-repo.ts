import {
  Auth,
  IMaterializationRepo,
  MaterializationQueryDto,
} from '../../domain/materialization/i-materialization-repo';
import {
  Materialization,
  MaterializationProps,
  parseMaterializationType,
} from '../../domain/entities/materialization';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';

export default class MaterializationRepo implements IMaterializationRepo {
  readonly #matName = 'materialization';

  readonly #colDefinitions: ColumnDefinition[] = [
    { name: 'id' },
    { name: 'name' },
    { name: 'schema_name' },
    { name: 'database_name' },
    { name: 'relation_name' },
    { name: 'type' },
    { name: 'isTransient' },
    { name: 'logicId' },
    { name: 'ownerId' },
    { name: 'lineageIds' },
    { name: 'comment' },
  ];

  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  #buildMaterialization = (sfEntity: SnowflakeEntity): Materialization => {
    const {
      ID: id,
      NAME: name,
      SCHEMA_NAME: schemaName,
      DATABASE_NAME: databaseName,
      RELATION_NAME: relationName,
      TYPE: type,
      IS_TRANSIENT: isTransient,
      LOGIC_ID: logicId,
      OWNER_ID: ownerId,
      LINEAGE_IDS: lineageIds,
      COMMENT: comment,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof schemaName !== 'string' ||
      typeof databaseName !== 'string' ||
      typeof relationName !== 'string' ||
      typeof type !== 'string' ||
      typeof isTransient !== 'boolean' ||
      typeof logicId !== 'string' ||
      typeof ownerId !== 'string' ||
      typeof lineageIds !== 'object' ||
      typeof comment !== 'string'
    )
      throw new Error(
        'Retrieved unexpected materialization field types from persistence'
      );

    const isStringArray = (value: unknown): value is string[] =>
      Array.isArray(value) && value.every((el) => typeof el === 'string');

    if (!isStringArray(lineageIds))
      throw new Error(
        'Type mismatch detected when reading materialization from persistence'
      );

    return this.#toEntity({
      id,
      name,
      schemaName,
      databaseName,
      relationName,
      type: parseMaterializationType(type),
      isTransient,
      logicId,
      ownerId,
      lineageIds,
      comment,
    });
  };

  findOne = async (
    materializationId: string,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName}
            } where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [materializationId];

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(
          `Multiple or no materialization entities with id found`
        );

      return !result.value.length
        ? null
        : this.#buildMaterialization(result.value[0]);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    materializationQueryDto: MaterializationQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization[]> => {
    try {
      if (!Object.keys(materializationQueryDto).length)
        return await this.all(auth, targetOrgId);

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [materializationQueryDto.lineageId];
      let whereClause = 'array_contains(?::variant, lineage_ids) ';

      if (materializationQueryDto.relationName) {
        binds.push(materializationQueryDto.relationName);
        whereClause = whereClause.concat('and relation_name = ? ');
      }
      if (materializationQueryDto.type) {
        binds.push(materializationQueryDto.type);
        whereClause = whereClause.concat('and type = ? ');
      }
      if (materializationQueryDto.name) {
        binds.push(
          Array.isArray(materializationQueryDto.name)
            ? materializationQueryDto.name.map((el) => `'${el}'`).join(', ')
            : materializationQueryDto.name
        );
        whereClause = whereClause.concat(
          Array.isArray(materializationQueryDto.name)
            ? 'and array_contains(name::variant, array_construct(?))'
            : 'and name = ? '
        );
      }
      if (materializationQueryDto.schemaName) {
        binds.push(materializationQueryDto.schemaName);
        whereClause = whereClause.concat('and schema_name = ? ');
      }
      if (materializationQueryDto.databaseName) {
        binds.push(materializationQueryDto.databaseName);
        whereClause = whereClause.concat('and database_name = ? ');
      }
      if (materializationQueryDto.logicId) {
        binds.push(materializationQueryDto.logicId);
        whereClause = whereClause.concat('and logic_id = ? ');
      }

      const queryText = `select * from cito.lineage.${this.#matName}
          } where  ${whereClause};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(
          `Multiple or no materialization entities with id found`
        );

      return result.value.map((el) => this.#buildMaterialization(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (
    auth: Auth,
    targetOrgId?: string
  ): Promise<Materialization[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds: [] },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(
          `Multiple or no materialization entities with id found`
        );

      return result.value.map((el) => this.#buildMaterialization(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #getBinds = (el: Materialization): (string | number)[] => [
    el.id,
    el.name,
    el.schemaName,
    el.databaseName,
    el.relationName,
    el.type,
    el.isTransient ? el.isTransient.toString() : 'null',
    el.logicId || 'null',
    el.ownerId || 'null',
    JSON.stringify(el.lineageIds),
    el.comment || 'null',
  ];

  insertOne = async (
    materialization: Materialization,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    try {
      const binds = this.#getBinds(materialization);

      const row = `(${binds.map(() => '?').join(', ')})`;

      const queryText = getInsertQuery(this.#matName, this.#colDefinitions, [
        row,
      ]);

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return materialization.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    materializations: Materialization[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]> => {
    try {
      const binds = materializations.map((materialization) =>
        this.#getBinds(materialization)
      );

      const rows = binds.map((el) => `(${el.map(() => '?').join(', ')})`);

      const queryText = getInsertQuery(
        this.#matName,
        this.#colDefinitions,
        rows
      );

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return materializations.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    materializations: Materialization[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<number> => {
    try {
      const binds = materializations.map((materialization) =>
        this.#getBinds(materialization)
      );

      const rows = binds.map((el) => `(${el.map(() => '?').join(', ')})`);

      const queryText = getUpdateQuery(
        this.#matName,
        this.#colDefinitions,
        rows
      );

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return materializations.length;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #toEntity = (materializationProps: MaterializationProps): Materialization =>
    Materialization.build(materializationProps);
}
