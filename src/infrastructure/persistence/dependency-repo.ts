import {
  DependencyQueryDto,
  DependencyUpdateDto,
  IDependencyRepo,
} from '../../domain/dependency/i-dependency-repo';
import {
  Dependency,
  DependencyProps,
  parseDependencyType,
} from '../../domain/entities/dependency';
import { Bind, SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';

export default class DependencyRepo
  extends BaseSfRepo<
    Dependency,
    DependencyProps,
    DependencyQueryDto,
    DependencyUpdateDto
  >
  implements IDependencyRepo
{
  readonly matName = 'dependencies';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'type', nullable: false },
    { name: 'head_id', nullable: false },
    { name: 'tail_id', nullable: false },
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
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof type !== 'string' ||
      typeof headId !== 'string' ||
      typeof tailId !== 'string' 
    )
      throw new Error(
        'Retrieved unexpected dependency field types from persistence'
      );

    return {
      id,
      headId,
      tailId,
      type: parseDependencyType(type),
    };
  };

  getBinds = (entity: Dependency): Bind[] => [
    entity.id,
    entity.type,
    entity.headId,
    entity.tailId,
  ];

  protected buildFindByQuery(dto: DependencyQueryDto): Query {
    const binds: (string | number)[] = [];
    let whereClause = '';

    if (dto.tailId) {
      binds.push(dto.tailId);
      whereClause = whereClause.concat('and tail_id = ? ');
    }
    if (dto.headId) {
      binds.push(dto.headId);
      whereClause = whereClause.concat('and head_id = ? ');
    }
    if (dto.type) {
      binds.push(dto.type);
      whereClause = whereClause.concat('and type = ? ');
    }

    const text = `select * from cito.lineage.${this.matName}
        where  ${whereClause};`;

    return { text, binds };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(dto)}]`
    );
  }

  toEntity = (props: DependencyProps): Dependency => Dependency.build(props);
}
