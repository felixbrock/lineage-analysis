import {
  DashboardQueryDto,
  DashboardUpdateDto,
  IDashboardRepo,
} from '../../domain/dashboard/i-dashboard-repo';
import { Dashboard, DashboardProps } from '../../domain/entities/dashboard';

import {
  Bind,
  SnowflakeEntity,
} from '../../domain/snowflake-api/i-snowflake-api-repo';
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
    { name: 'materialization_name', nullable: false },
    { name: 'materialization_id', nullable: false },
    { name: 'column_name', nullable: false },
    { name: 'column_id', nullable: false },
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
    };
  };

  getBinds = (entity: Dashboard): Bind[] => [
    entity.id,
    entity.name || 'null',
    entity.url || 'null',
    entity.materializationName,
    entity.materializationId,
    entity.columnName,
    entity.columnId,
  ];

  buildFindByQuery(dto: DashboardQueryDto): Query {
    const binds: (string | number)[] = [];
    let whereClause = '';

    if (dto.url) {
      binds.push(dto.url);
      const whereCondition = `url = ?`;

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.name) {
      binds.push(dto.name);
      const whereCondition = 'name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.materializationName) {
      binds.push(dto.materializationName);
      const whereCondition = 'materialization_name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.materializationId) {
      binds.push(dto.materializationId);
      const whereCondition = 'materialization_id = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.columnName) {
      binds.push(dto.columnName);
      const whereCondition = 'column_name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.columnId) {
      binds.push(dto.columnId);
      const whereCondition = 'column_id = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }

    const text = `select * from cito.lineage.${this.matName}
              ${whereClause ? 'where' : ''}  ${whereClause};`;

    return { text, binds };
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
