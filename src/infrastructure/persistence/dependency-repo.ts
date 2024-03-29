import { Document } from 'mongodb';
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

  buildEntityProps = (document: Document): DependencyProps => {
    const {
      id,
      type,
      head_id: headId,
      tail_id: tailId,
    } = document;

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

  getValues = (entity: Dependency): (string | number)[] => [
    entity.id,
    entity.type,
    entity.headId,
    entity.tailId,
  ];

  protected buildFindByQuery(dto: DependencyQueryDto): Query {
    const values: (string | number)[] = [];
    const filter: any = {};

    if (dto.tailId) {
      values.push(dto.tailId);
      filter.tail_id = dto.tailId;
    }
    if (dto.headId) {
      values.push(dto.headId);
      filter.head_id = dto.headId;
    }
    if (dto.type) {
      values.push(dto.type);
      filter.type = dto.type;
    }

    return { filter, values };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(dto)}]`
    );
  }

  toEntity = (props: DependencyProps): Dependency => Dependency.build(props);
}
