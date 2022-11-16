import {
  Auth,
  ILineageRepo,
  LineageUpdateDto,
} from '../../domain/lineage/i-lineage-repo';
import { Lineage, LineageProps } from '../../domain/entities/lineage';
import {
  ColumnDefinition,
  getInsertQuery,
  getUpdateQuery,
} from './shared/query';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { SnowflakeProfileDto } from '../../domain/integration-api/i-integration-api-repo';
import BaseSfRepo from './shared/base-sf-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';

export default class LineageRepo
  extends BaseSfRepo<Lineage, LineageProps>
  implements ILineageRepo
{
  readonly matName = 'lineage_snapshots';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'created_at', nullable: false },
    { name: 'completed', nullable: false },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (sfEntity: SnowflakeEntity): LineageProps => {
    const { ID: id, COMPLETED: completed, CREATED_AT: createdAt } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof completed !== 'boolean' ||
      !(createdAt instanceof Date)
    )
      throw new Error(
        'Retrieved unexpected lineage field types from persistence'
      );

    return {
      id,
      completed,
      createdAt: createdAt.toISOString(),
    };
  };

  findOne = async (
    lineageId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Lineage | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName}
       where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [lineageId];

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length > 1)
        throw new Error(`Multiple lineage entities with id found`);

      return !result.value.length
        ? null
        : this.toEntity(this.buildEntityProps(result.value[0]));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  findLatest = async (
    filter: { tolerateIncomplete: boolean; minuteTolerance?: number },
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Lineage | null> => {
    const minuteTolerance: number = filter.minuteTolerance || 10;

    const queryText = `select * from cito.lineage.${this.matName} 
    where completed = true 
    ${
      filter.tolerateIncomplete
        ? `or (completed = false and timediff(minute, created_at, sysdate()) < ?)`
        : ''
    }
    order by created_at desc limit 1;`;

    const binds = filter.tolerateIncomplete ? [minuteTolerance] : [];

    try {
      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length > 1)
        throw new Error(`Multiple lineage entities with id found`);

      return !result.value.length
        ? null
        : this.toEntity(this.buildEntityProps(result.value[0]));
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
  ): Promise<Lineage[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName};`;

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds: [], profile },
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

  getBinds = (entity: Lineage): (string | number)[] => [
    entity.id,
    entity.createdAt,
    entity.completed.toString(),
  ];

  insertOne = async (
    lineage: Lineage,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    const row = `(?, ?, ?)`;
    const binds = this.getBinds(lineage);

    try {
      const queryText = getInsertQuery(this.matName, this.colDefinitions, [
        row,
      ]);

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
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
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    const idDef = this.colDefinitions.find((el) => el.name === 'id');
    if (!idDef) throw new Error('Missing col definition');

    const colDefinitions: ColumnDefinition[] = [idDef];
    const binds = [lineageId];

    if (updateDto.completed !== undefined) {
      const completedDef = this.colDefinitions.find(
        (el) => el.name === 'completed'
      );
      if (!completedDef) throw new Error('Missing col definition');
      colDefinitions.push(completedDef);
      binds.push(updateDto.completed.toString());
    }

    try {
      const queryText = getUpdateQuery(this.matName, colDefinitions, [
        `(${binds.map(() => '?').join(', ')})`,
      ]);

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
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

  toEntity = (lineageProperties: LineageProps): Lineage =>
    Lineage.build(lineageProperties);
}
