import {
  Auth,
  DashboardQueryDto,
  IDashboardRepo,
} from '../../domain/dashboard/i-dashboard-repo';
import { Dashboard, DashboardProps } from '../../domain/entities/dashboard';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';

export default class DashboardRepo implements IDashboardRepo {
  readonly #matName = 'dashboard';

  readonly #colDefinitions: ColumnDefinition[] = [
    { name: 'id' },
    { name: 'name' },
    { name: 'url' },
    { name: 'materialization_name' },
    { name: 'materialization_id' },
    { name: 'column_name' },
    { name: 'column_id' },
    { name: 'lineageIds' },
  ];

  readonly #querySnowflake: QuerySnowflake;

  constructor(querySnowflake: QuerySnowflake) {
    this.#querySnowflake = querySnowflake;
  }

  #buildDashboard = (sfEntity: SnowflakeEntity): Dashboard => {
    const {
      ID: id,
      NAME: name,
      URL: url,
      MATERIALIZATION_NAME: materializationName,
      MATERIALIZATION_ID: materializationId,
      COLUMN_NAME: columnName,
      COLUMN_ID: columnId,
      LINEAGE_IDS: lineageIds,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof url !== 'string' ||
      typeof materializationName !== 'string' ||
      typeof materializationId !== 'string' ||
      typeof columnName !== 'string' ||
      typeof columnId !== 'string' ||
      typeof lineageIds !== 'object'
    )
      throw new Error(
        'Retrieved unexpected dashboard field types from persistence'
      );

    const isStringArray = (value: unknown): value is string[] =>
      Array.isArray(value) && value.every((el) => typeof el === 'string');

    if (!isStringArray(lineageIds))
      throw new Error(
        'Type mismatch detected when reading dashboard from persistence'
      );

    return this.#toEntity({
      id,
      name,
      url,
      materializationName,
      materializationId,
      columnName,
      columnId,
      lineageIds,
    });
  };

  findOne = async (
    dashboardId: string,
    auth: Auth,
    targetOrgId?: string,
  ): Promise<Dashboard | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName}
                } where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [dashboardId];

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dashboard entities with id found`);

      return !result.value.length
        ? null
        : this.#buildDashboard(result.value[0]);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    dashboardQueryDto: DashboardQueryDto,
    auth: Auth,
    targetOrgId?: string,
  ): Promise<Dashboard[]> => {
    try {
      if (!Object.keys(dashboardQueryDto).length)
        return await this.all(targetOrgId, auth);

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [dashboardQueryDto.lineageId];
      let whereClause = 'array_contains(?::variant, lineage_ids) ';

      if (dashboardQueryDto.url) {
        binds.push(dashboardQueryDto.url);
        whereClause = whereClause.concat('and url = ? ');
      }
      if (dashboardQueryDto.name) {
        binds.push(dashboardQueryDto.name);
        whereClause = whereClause.concat('and name = ? ');
      }
      if (dashboardQueryDto.materializationName) {
        binds.push(dashboardQueryDto.materializationName);
        whereClause = whereClause.concat('and materialization_name = ? ');
      }
      if (dashboardQueryDto.materializationId) {
        binds.push(dashboardQueryDto.materializationId);
        whereClause = whereClause.concat('and materialization_id = ? ');
      }
      if (dashboardQueryDto.columnName) {
        binds.push(dashboardQueryDto.columnName);
        whereClause = whereClause.concat('and column_name = ? ');
      }
      if (dashboardQueryDto.columnId) {
        binds.push(dashboardQueryDto.columnId);
        whereClause = whereClause.concat('and column_id = ? ');
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
        throw new Error(`Multiple or no dashboard entities with id found`);

      return result.value.map((el) => this.#buildDashboard(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (targetOrgId?: string, auth: Auth): Promise<Dashboard[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.#matName};`;

      const result = await this.#querySnowflake.execute(
        { queryText, targetOrgId, binds: [] },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dashboard entities with id found`);

      return result.value.map((el) => this.#buildDashboard(el));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #getBinds = (el: Dashboard): (string | number)[] => [
    el.id,
    el.name || 'null',
    el.url || 'null',
    el.materializationName,
    el.materializationId,
    el.columnName,
    el.columnId,
    JSON.stringify(el.lineageIds),
  ];

  insertOne = async (
    dashboard: Dashboard,
    targetOrgId?: string,
    auth: Auth
  ): Promise<string> => {
    try {
      const binds = this.#getBinds(dashboard);

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

      return dashboard.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    dashboards: Dashboard[],
    targetOrgId?: string,
    auth: Auth
  ): Promise<string[]> => {
    try {
      const binds = dashboards.map((dashboard) => this.#getBinds(dashboard));

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

      return dashboards.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  replaceMany = async (
    dashboards: Dashboard[],
    targetOrgId?: string,
    auth: Auth
  ): Promise<number> => {
    try {
      const binds = dashboards.map((dashboard) => this.#getBinds(dashboard));

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

      return dashboards.length;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  #toEntity = (props: DashboardProps): Dashboard => Dashboard.build(props);
}
