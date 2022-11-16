import {
  Auth,
  DashboardQueryDto,
  IDashboardRepo,
} from '../../domain/dashboard/i-dashboard-repo';
import { Dashboard, DashboardProps } from '../../domain/entities/dashboard';
import { SnowflakeProfileDto } from '../../domain/integration-api/i-integration-api-repo';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo from './shared/base-sf-repo';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';

export default class DashboardRepo
  extends BaseSfRepo<Dashboard, DashboardProps>
  implements IDashboardRepo
{
  readonly matName = 'dashboards';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'name', nullable: true },
    { name: 'url', nullable: true },
    { name: 'materialization_name', nullable: false },
    { name: 'materialization_id', nullable: false },
    { name: 'column_name', nullable: false },
    { name: 'column_id', nullable: false },
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
  ];

// eslint-disable-next-line @typescript-eslint/no-useless-constructor
constructor(querySnowflake: QuerySnowflake) {
  super(querySnowflake);
}

  buildEntityProps = (sfEntity: SnowflakeEntity): DashboardProps => {
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
      typeof materializationName !== 'string' ||
      typeof materializationId !== 'string' ||
      typeof columnName !== 'string' ||
      typeof columnId !== 'string'
    )
      throw new Error(
        'Retrieved unexpected dashboard field types from persistence'
      );


    if (
      !DashboardRepo.isStringArray(lineageIds) ||
      !DashboardRepo.isOptionalOfType<string>(name, 'string') ||
      !DashboardRepo.isOptionalOfType<string>(url, 'string')
    )
      throw new Error(
        'Type mismatch detected when reading dashboard from persistence'
      );

    return {
      id,
      name,
      url,
      materializationName,
      materializationId,
      columnName,
      columnId,
      lineageIds,
    };
  };

  findOne = async (
    dashboardId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName}
                 where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [dashboardId];

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dashboard entities with id found`);

      return !result.value.length
        ? null
        : this.toEntity(this.buildEntityProps(result.value[0]));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findBy = async (
    dashboardQueryDto: DashboardQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard[]> => {
    try {
      if (!Object.keys(dashboardQueryDto).length)
        return await this.all(profile, auth, targetOrgId);

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

      const queryText = `select * from cito.lineage.${this.matName}
              where  ${whereClause};`;

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');

      return result.value.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  all = async (
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dashboard[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName};`;

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds: [], profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dashboard entities with id found`);

      return result.value.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  getBinds = (entity: Dashboard): (string | number)[] => [
    entity.id,
    entity.name || 'null',
    entity.url || 'null',
    entity.materializationName,
    entity.materializationId,
    entity.columnName,
    entity.columnId,
    JSON.stringify(entity.lineageIds),
  ];

  insertOne = async (
    dashboard: Dashboard,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    try {
      const binds = this.getBinds(dashboard);

      const row = `(${binds.map(() => '?').join(', ')})`;

      const queryText = getInsertQuery(this.matName, this.colDefinitions, [
        row,
      ]);

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
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
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]> => {
    try {
      const binds = dashboards.map((dashboard) => this.getBinds(dashboard));

      const row = `(${this.colDefinitions.map(() => '?').join(', ')})`;

      const queryText = getInsertQuery(this.matName, this.colDefinitions, [
        row,
      ]);

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
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
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<number> => {
    try {
      const binds = dashboards.map((dashboard) => this.getBinds(dashboard));

      const row = `(${this.colDefinitions.map(() => '?').join(', ')})`;

      const queryText = getUpdateQuery(this.matName, this.colDefinitions, [
        row,
      ]);

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
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

  toEntity = (props: DashboardProps): Dashboard => Dashboard.build(props);
}
