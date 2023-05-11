import { Document } from 'mongodb';
import {
  DashboardQueryDto,
  DashboardUpdateDto,
  IDashboardRepo,
} from '../../domain/dashboard/i-dashboard-repo';
import { Dashboard, DashboardProps } from '../../domain/entities/dashboard';

import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';

export default class DashboardRepo
  extends BaseSfRepo<
    Dashboard,
    DashboardProps,
    DashboardQueryDto,
    DashboardUpdateDto
  >
  implements IDashboardRepo
{
  readonly matName = 'dashboards';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'name', nullable: true },
    { name: 'url', nullable: true },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (document: Document): DashboardProps => {
    const { id, name, url } = document;

    if (typeof id !== 'string' || typeof url !== 'string')
      throw new Error(
        'Retrieved unexpected dashboard field types from persistence'
      );

    if (!DashboardRepo.isOptionalOfType<string>(name, 'string'))
      throw new Error(
        'Type mismatch detected when reading dashboard from persistence'
      );

    return {
      id,
      name,
      url,
    };
  };

  getValues = (entity: Dashboard): (string | number)[] => [
    entity.id,
    entity.name || 'null',
    entity.url,
  ];

  buildFindByQuery(dto: DashboardQueryDto): Query {
    const values: (string | number)[] = [];
    const filter: any = {};

    if (dto.url) {
      values.push(dto.url);
      filter.url = dto.url;
    }
    if (dto.name) {
      values.push(dto.name);
      filter.name = dto.name;
    }

    return { filter, values };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(
        dto
      )}]`
    );
  }

  toEntity = (props: DashboardProps): Dashboard => Dashboard.build(props);
}
