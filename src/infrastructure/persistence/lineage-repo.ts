import {
  Lineage,
  LineageProps,
  parseLineageCreationState,
} from '../../domain/entities/lineage';
import {
  Bind,
  IConnectionPool,
  SnowflakeEntity,
} from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';
import {
  ILineageRepo,
  LineageQueryDto,
  LineageUpdateDto,
} from '../../domain/lineage/i-lineage-repo';
import BaseAuth from '../../domain/services/base-auth';

export default class LineageRepo
  extends BaseSfRepo<Lineage, LineageProps, LineageQueryDto, LineageUpdateDto>
  implements ILineageRepo
{
  readonly matName = 'lineage_snapshots';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'created_at', nullable: false },
    { name: 'creation_state', nullable: false },
    { name: 'db_covered_names', selectType: 'parse_json', nullable: false },
    { name: 'diff', nullable: true },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (sfEntity: SnowflakeEntity): LineageProps => {
    const {
      ID: id,
      CREATION_STATE: creationState,
      CREATED_AT: createdAt,
      DB_COVERED_NAMES: dbCoveredNames,
      DIFF: diff,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      !(createdAt instanceof Date) ||
      !LineageRepo.isStringArray(dbCoveredNames) ||
      !LineageRepo.isOptionalOfType<string>(diff, 'string')
    )
      throw new Error(
        'Retrieved unexpected lineage field types from persistence'
      );

    return {
      id,
      creationState: parseLineageCreationState(creationState),
      createdAt: createdAt.toISOString(),
      dbCoveredNames,
      diff,
    };
  };

  findLatest = async (
    filter: { tolerateIncomplete: boolean; minuteTolerance?: number },
    auth: BaseAuth,
    connPool: IConnectionPool
  ): Promise<Lineage | undefined> => {
    const minuteTolerance: number = filter.minuteTolerance || 10;

    const queryText = `select * from cito.lineage.${this.matName} 
    where creation_state = 'completed' 
    ${
      filter.tolerateIncomplete
        ? `or (creation_state != 'completed' and timediff(minute, created_at, sysdate()) < ?)`
        : ''
    }
    order by created_at desc limit 1;`;

    const binds = filter.tolerateIncomplete ? [minuteTolerance] : [];

    try {
      const result = await this.querySnowflake.execute(
        { queryText, binds },
        auth,
        connPool
      );

      if (!result.success) throw new Error(result.error);
      if (!result.value) throw new Error('Missing sf query value');
      if (result.value.length > 1)
        throw new Error(`Multiple lineage entities with id found`);

      return !result.value.length
        ? undefined
        : this.toEntity(this.buildEntityProps(result.value[0]));
    } catch (error: unknown) {
      if (error instanceof Error) console.error(error.stack);
      else if (error) console.trace(error);
      return Promise.reject(new Error());
    }
  };

  getBinds = (entity: Lineage): Bind[] => [
    entity.id,
    entity.createdAt,
    entity.creationState,
    JSON.stringify(entity.dbCoveredNames),
    entity.diff || 'null',
  ];

  buildFindByQuery(dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${JSON.stringify(dto)}]`
    );
  }

  buildUpdateQuery(id: string, dto: LineageUpdateDto): Query {
    const colDefinitions: ColumnDefinition[] = [this.getDefinition('id')];
    const binds = [id];

    if (dto.creationState) {
      colDefinitions.push(this.getDefinition('creation_state'));
      binds.push(dto.creationState.toString());
    }

    if (dto.dbCoveredNames)
      colDefinitions.push(this.getDefinition('db_covered_names'));
    binds.push(JSON.stringify(dto.dbCoveredNames));

    if (dto.diff) {
      colDefinitions.push(this.getDefinition('diff'));
      binds.push(dto.diff);
    }

    const text = this.getUpdateQueryText(this.matName, colDefinitions, [
      `(${binds.map(() => '?').join(', ')})`,
    ]);

    return { text, binds, colDefinitions };
  }

  toEntity = (lineageProperties: LineageProps): Lineage =>
    Lineage.build(lineageProperties);
}
