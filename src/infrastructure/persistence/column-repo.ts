import {
  Auth,
  ColumnQueryDto,
  IColumnRepo,
} from '../../domain/column/i-column-repo';
import {
  Column,
  ColumnProps,
  parseColumnDataType,
} from '../../domain/entities/column';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';

export default class ColunRepo implements IColumnRepo {
  readonly #matName = 'columns';

  readonly #colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'name', nullable: false },
    { name: 'relation_name', nullable: false },
    { name: 'index', nullable: false },
    { name: 'data_type', nullable: false },
    { name: 'is_identity', nullable: true },
    { name: 'is_nullable', nullable: true },
    { name: 'materialization_id', nullable: false },
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
    { name: 'comment', nullable: true },
  ];

  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  #buildColumn = (sfEntity: SnowflakeEntity): Column => {
    const {
      ID: id,
      NAME: name,
      RELATION_NAME: relationName,
      INDEX: index,
      DATA_TYPE: dataType,
      IS_IDENTITY: isIdentity,
      IS_NULLABLE: isNullable,
      MATERIALIZATION_ID: materializationId,
      LINEAGE_IDS: lineageIds,
      COMMENT: comment,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof relationName !== 'string' ||
      typeof index !== 'string' ||
      typeof dataType !== 'string' ||      
      typeof materializationId !== 'string'
    )
      throw new Error(
        'Retrieved unexpected column field types from persistence'
      );

    const isStringArray = (value: unknown): value is string[] =>
      Array.isArray(value) && value.every((el) => typeof el === 'string');
    const isOptionalOfType = <T>(val: unknown, type: string): val is T =>
      val === null || typeof val === type;

    if (
      !isStringArray(lineageIds) ||
      !isOptionalOfType<boolean>(isIdentity, 'boolean') ||
      !isOptionalOfType<boolean>(isNullable, 'boolean') ||
      !isOptionalOfType<string>(comment, 'string')
    )
      throw new Error(
        'Type mismatch detected when reading column from persistence'
      );

    return this.#toEntity({
      id,
      name,
      relationName,
      index,
      dataType: parseColumnDataType(dataType),
      isIdentity,
      isNullable,
      materializationId,
      lineageIds,
      comment,
    });
  };

  findOne = async (
    columnId: string,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Column | null> => {
    try {
      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [columnId];

      const queryText = `select * from cito.lineage.${this.#matName}
    where id = ?;`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no column entities with id found`);

      return !result.value.length ? null : this.#buildColumn(result.value[0]);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    columnQueryDto: ColumnQueryDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Column[]> => {
    try {
      if (!Object.keys(columnQueryDto).length)
        return await this.all(auth, targetOrgId);

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [columnQueryDto.lineageId];
      let whereClause = 'array_contains(?::variant, lineage_ids) ';

      if (columnQueryDto.relationName) {
        binds.push(
          Array.isArray(columnQueryDto.relationName)
            ? columnQueryDto.relationName.map((el) => `'${el}'`).join(', ')
            : columnQueryDto.relationName
        );
        whereClause = whereClause.concat(
          Array.isArray(columnQueryDto.relationName)
            ? 'and array_contains(relation_name::variant, array_construct(?))'
            : 'and relation_name = ? '
        );
      }
      if (columnQueryDto.name) {
        binds.push(
          Array.isArray(columnQueryDto.name)
            ? columnQueryDto.name.map((el) => `'${el}'`).join(', ')
            : columnQueryDto.name
        );
        whereClause = whereClause.concat(
          Array.isArray(columnQueryDto.name)
            ? 'and array_contains(name::variant, array_construct(?))'
            : 'and name = ? '
        );
      }
      if (columnQueryDto.index) {
        binds.push(columnQueryDto.index);
        whereClause = whereClause.concat('and index = ? ');
      }
      if (columnQueryDto.type) {
        binds.push(columnQueryDto.type);
        whereClause = whereClause.concat('and type = ? ');
      }
      if (columnQueryDto.materializationId) {
        binds.push(
          Array.isArray(columnQueryDto.materializationId)
            ? columnQueryDto.materializationId.map((el) => `'${el}'`).join(', ')
            : columnQueryDto.materializationId
        );
        whereClause = whereClause.concat(
          Array.isArray(columnQueryDto.materializationId)
            ? 'and array_contains(materializationId::variant, array_construct(?))'
            : 'and materialization_id = ? '
        );
      }

      const queryText = `select * from cito.lineage.${this.#matName}
        where  ${whereClause};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return result.value.map((el) => this.#buildColumn(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (auth: Auth, targetOrgId?: string): Promise<Column[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds: [] },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no column entities with id found`);

      return result.value.map((el) => this.#buildColumn(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #getBinds = (col: Column): (string | number)[] => [
    col.id,
    col.name,
    col.relationName,
    col.index,
    col.dataType,
    col.isIdentity !== undefined ? col.isIdentity.toString() : 'null',
    col.isNullable !== undefined ? col.isNullable.toString() : 'null',
    col.materializationId,
    JSON.stringify(col.lineageIds),
    col.comment || 'null',
  ];

  insertOne = async (
    column: Column,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    try {
      const binds = this.#getBinds(column);

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

      return column.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    columns: Column[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]> => {
    try {
      const binds = columns.map((column) => this.#getBinds(column));

      const row = `(${this.#colDefinitions.map(() => '?').join(', ')})`;

      const queryText = getInsertQuery(this.#matName, this.#colDefinitions, [
        row,
      ]);

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return columns.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    columns: Column[],
    auth: Auth,
    targetOrgId?: string
  ): Promise<number> => {
    try {
      const binds = columns.map((column) => this.#getBinds(column));

      const row = `(${this.#colDefinitions.map(() => '?').join(', ')})`;

      const queryText = getUpdateQuery(this.#matName, this.#colDefinitions, [
        row,
      ]);

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return columns.length;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #toEntity = (columnProperties: ColumnProps): Column =>
    Column.build(columnProperties);
}
