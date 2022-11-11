import {
  Auth,
  ILogicRepo,
  LogicQueryDto,
} from '../../domain/logic/i-logic-repo';
import { DependentOn, Logic, LogicProps, Refs } from '../../domain/entities/logic';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';

export default class LogicRepo implements ILogicRepo {
  readonly #matName = 'logic';

  readonly #colDefinitions: ColumnDefinition[] = [
    { name: 'id' },
    { name: 'relation_name' },
    { name: 'sql' },
    { name: 'dependent_on', selectType: 'parse_json' },
    { name: 'parsed_logic' },
    { name: 'statement_refs', selectType: 'parse_json' },
    { name: 'lineage_ids', selectType: 'parse_json' },
  ];

  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  #buildLogic = (sfEntity: SnowflakeEntity): Logic => {
    const {
      ID: id,
      RELATION_NAME: relationName,
      SQL: sql,
      DEPENDENT_ON: dependentOn,
      PARSED_LOGIC: parsedLogic,
      STATEMENT_REFS: statementRefs,
      LINEAGE_IDS: lineageIds,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof relationName !== 'string' ||
      typeof sql !== 'string' ||
      typeof dependentOn !== 'object' ||
      typeof parsedLogic !== 'string' ||
      typeof statementRefs !== 'object' ||
      typeof lineageIds !== 'object'
    )
      throw new Error(
        'Retrieved unexpected logic field types from persistence'
      );

    const isDependentOnObj = (el: unknown): el is DependentOn => !!el && 'dbtDependencyDefinitions' in el && 'dwDependencyDefinitions' in el;
    const isRefsObj = (el: unknown): el is Refs => !!el && 'materializations' in el && 'columns' in el && 'wildcards' in el;
    const isStringArray = (value: unknown): value is string[] => Array.isArray(value) && value.every(el => typeof el === 'string');

    if(!isDependentOnObj(dependentOn) || !isRefsObj(statementRefs) || !isStringArray(lineageIds)) throw new Error('Type mismatch detected when reading logic from persistence');

    return this.#toEntity({
      id,
      sql,
      relationName,
      dependentOn,
      lineageIds,
      parsedLogic,
      statementRefs,
    });
  };

  findOne = async (
    logicId: string,
    targetOrgId?: string,
    auth: Auth
  ): Promise<Logic | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName}
        } where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [logicId];

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length > 1)
        throw new Error(`Multiple logic entities with id found`);

      return !result.value.length ? null :  this.#buildLogic(result.value[0]);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    logicQueryDto: LogicQueryDto,
    targetOrgId?: string,
    auth: Auth
  ): Promise<Logic[]> => {
    try {
      if (!Object.keys(logicQueryDto).length)
        return await this.all(targetOrgId, auth);

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [logicQueryDto.lineageId];
      if (logicQueryDto.relationName) binds.push(logicQueryDto.relationName);

      const queryText = `select * from cito.lineage.${this.#matName}
      } where array_contains(?::variant, lineage_ids) ${
        logicQueryDto.relationName ? 'and relation_name = ?' : ''
      };`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no logic entities with id found`);

      return result.value.map((el) => this.#buildLogic(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (targetOrgId?: string, auth: Auth): Promise<Logic[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds: [] },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no logic entities with id found`);

      return result.value.map((el) => this.#buildLogic(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertOne = async (
    logic: Logic,
    targetOrgId?: string,
    auth: Auth
  ): Promise<string> => {
    try {
      const binds = [
        logic.id,
        logic.relationName,
        logic.sql,
        JSON.stringify(logic.dependentOn),
        logic.parsedLogic,
        JSON.stringify(logic.statementRefs),
        JSON.stringify(logic.lineageIds),
      ];
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

      return logic.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    logics: Logic[],
    targetOrgId?: string,
    auth: Auth
  ): Promise<string[]> => {
    try {
      const binds = logics.map((el) => [
        el.id,
        el.relationName,
        el.sql,
        JSON.stringify(el.dependentOn),
        el.parsedLogic,
        JSON.stringify(el.statementRefs),
        JSON.stringify(el.lineageIds),
      ]);

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

      return logics.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    logics: Logic[],
    targetOrgId?: string,
    auth: Auth
  ): Promise<number> => {
    try {
      const binds = logics.map((el) => [
        el.id,
        el.relationName,
        el.sql,
        JSON.stringify(el.dependentOn),
        el.parsedLogic,
        JSON.stringify(el.statementRefs),
        JSON.stringify(el.lineageIds),
      ]);

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

      return logics.length;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #toEntity = (logicProperties: LogicProps): Logic =>
    Logic.build(logicProperties);
}
