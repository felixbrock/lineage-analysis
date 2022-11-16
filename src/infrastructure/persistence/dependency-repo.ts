import {
  Auth,
  DependencyQueryDto,
  IDependencyRepo,
} from '../../domain/dependency/i-dependency-repo';
import {
  Dependency,
  DependencyProps,
  parseDependencyType,
} from '../../domain/entities/dependency';
import { SnowflakeProfileDto } from '../../domain/integration-api/i-integration-api-repo';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo from './shared/base-sf-repo';
import { ColumnDefinition, getInsertQuery } from './shared/query';

export default class DependencyRepo
  extends BaseSfRepo<Dependency, DependencyProps>
  implements IDependencyRepo
{
  readonly matName = 'dependencies';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'type', nullable: false },
    { name: 'head_id', nullable: false },
    { name: 'tail_id', nullable: false },
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
  ];

// eslint-disable-next-line @typescript-eslint/no-useless-constructor
constructor(querySnowflake: QuerySnowflake) {
  super(querySnowflake);
}

  buildEntityProps = (sfEntity: SnowflakeEntity): DependencyProps => {
    const {
      ID: id,
      TYPE: type,
      HEAD_ID: headId,
      TAIL_ID: tailId,
      LINEAGE_IDS: lineageIds,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof type !== 'string' ||
      typeof headId !== 'string' ||
      typeof tailId !== 'string' ||
      typeof lineageIds !== 'string'
    )
      throw new Error(
        'Retrieved unexpected dependency field types from persistence'
      );

    if (!DependencyRepo.isStringArray(lineageIds))
      throw new Error(
        'Type mismatch detected when reading materialization from persistence'
      );

    return {
      id,
      headId,
      tailId,
      lineageIds,
      type: parseDependencyType(type),
    };
  };

  findOne = async (
    dependencyId: string,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency | null> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName}
             where id = ?;`;

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [dependencyId];

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds, profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dependency entities with id found`);

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
    dependencyQueryDto: DependencyQueryDto,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<Dependency[]> => {
    try {
      if (!Object.keys(dependencyQueryDto).length)
        return await this.all(profile, auth, targetOrgId);

      // using binds to tell snowflake to escape params to avoid sql injection attack
      const binds: (string | number)[] = [dependencyQueryDto.lineageId];
      let whereClause = 'array_contains(?::variant, lineage_ids) ';

      if (dependencyQueryDto.tailId) {
        binds.push(dependencyQueryDto.tailId);
        whereClause = whereClause.concat('and tail_id = ? ');
      }
      if (dependencyQueryDto.headId) {
        binds.push(dependencyQueryDto.headId);
        whereClause = whereClause.concat('and head_id = ? ');
      }
      if (dependencyQueryDto.type) {
        binds.push(dependencyQueryDto.type);
        whereClause = whereClause.concat('and type = ? ');
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
  ): Promise<Dependency[]> => {
    try {
      const queryText = `select * from cito.lineage.${this.matName};`;

      const result = await this.querySnowflake.execute(
        { queryText, targetOrgId, binds: [], profile },
        auth
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length !== 1)
        throw new Error(`Multiple or no dependency entities with id found`);

      return result.value.map((el) => this.toEntity(this.buildEntityProps(el)));
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  getBinds = (entity: Dependency): (string | number)[] => [
    entity.id,
    entity.type,
    entity.headId,
    entity.tailId,
    JSON.stringify(entity.lineageIds),
  ];

  insertOne = async (
    dependency: Dependency,
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string> => {
    try {
      const binds = this.getBinds(dependency);

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

      return dependency.id;
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  insertMany = async (
    dependencys: Dependency[],
    profile: SnowflakeProfileDto,
    auth: Auth,
    targetOrgId?: string
  ): Promise<string[]> => {
    try {
      const binds = dependencys.map((dependency) => this.getBinds(dependency));

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

      return dependencys.map((el) => el.id);
    } catch (error: unknown) {
      if (error instanceof Error && error.message) console.trace(error.message);
      else if (!(error instanceof Error) && error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  toEntity = (props: DependencyProps): Dependency => Dependency.build(props);
}
