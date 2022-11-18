import {
  DashboardQueryDto,
  DashboardUpdateDto,
  IDashboardRepo,
} from '../../domain/dashboard/i-dashboard-repo';
import { Dashboard, DashboardProps } from '../../domain/entities/dashboard';

import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { Query } from './shared/base-sf-repo';
import {
  ColumnDefinition,
} from './shared/query';

export default class DashboardRepo
  extends BaseSfRepo<Dashboard, DashboardProps, DashboardQueryDto, DashboardUpdateDto>
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

  buildFindByQuery(dto: DashboardQueryDto): Query {
    const binds: (string | number)[] = [dto.lineageId];
      let whereClause = 'array_contains(?::variant, lineage_ids) ';

      if (dto.url) {
        binds.push(dto.url);
        whereClause = whereClause.concat('and url = ? ');
      }
      if (dto.name) {
        binds.push(dto.name);
        whereClause = whereClause.concat('and name = ? ');
      }
      if (dto.materializationName) {
        binds.push(dto.materializationName);
        whereClause = whereClause.concat('and materialization_name = ? ');
      }
      if (dto.materializationId) {
        binds.push(dto.materializationId);
        whereClause = whereClause.concat('and materialization_id = ? ');
      }
      if (dto.columnName) {
        binds.push(dto.columnName);
        whereClause = whereClause.concat('and column_name = ? ');
      }
      if (dto.columnId) {
        binds.push(dto.columnId);
        whereClause = whereClause.concat('and column_id = ? ');
      }

      const text = `select * from cito.lineage.${this.matName}
              where  ${whereClause};`;

      return {text, binds};
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(dto)}]`
    );
  }
  
  toEntity = (props: DashboardProps): Dashboard => Dashboard.build(props);
}
