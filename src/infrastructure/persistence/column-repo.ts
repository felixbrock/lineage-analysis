import {
  ColumnQueryDto,
  ColumnUpdateDto,
  IColumnRepo,
} from '../../domain/column/i-column-repo';
import {
  Column,
  ColumnProps,
  parseColumnDataType,
} from '../../domain/entities/column';

import {
  Bind,
  SnowflakeEntity,
} from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';

export default class ColumnRepo
  extends BaseSfRepo<Column, ColumnProps, ColumnQueryDto, ColumnUpdateDto>
  implements IColumnRepo
{
  readonly matName = 'columns';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'name', nullable: false },
    { name: 'relation_name', nullable: false },
    { name: 'index', nullable: false },
    { name: 'data_type', nullable: false },
    { name: 'is_identity', nullable: true },
    { name: 'is_nullable', nullable: true },
    { name: 'materialization_id', nullable: false },
    { name: 'comment', nullable: true },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (sfEntity: SnowflakeEntity): ColumnProps => {
    const {
      ID: id,
      NAME: name,
      RELATION_NAME: relationName,
      INDEX: index,
      DATA_TYPE: dataType,
      IS_IDENTITY: isIdentity,
      IS_NULLABLE: isNullable,
      MATERIALIZATION_ID: materializationId,
      COMMENT: comment,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof relationName !== 'string' ||
      typeof index !== 'string' ||
      typeof dataType !== 'string' ||
      typeof materializationId !== 'string'
    )
      throw new Error(
        'Retrieved unexpected column field types from persistence'
      );

    if (
      !ColumnRepo.isOptionalOfType<boolean>(isIdentity, 'boolean') ||
      !ColumnRepo.isOptionalOfType<boolean>(isNullable, 'boolean') ||
      !ColumnRepo.isOptionalOfType<string>(comment, 'string')
    )
      throw new Error(
        'Type mismatch detected when reading column from persistence'
      );

    return {
      id,
      name,
      relationName,
      index,
      dataType: parseColumnDataType(dataType),
      isIdentity,
      isNullable,
      materializationId,
      comment,
    };
  };

  getBinds = (entity: Column): Bind[] => [
    entity.id,
    entity.name,
    entity.relationName,
    entity.index,
    entity.dataType,
    entity.isIdentity !== undefined ? entity.isIdentity.toString() : 'null',
    entity.isNullable !== undefined ? entity.isNullable.toString() : 'null',
    entity.materializationId,
    entity.comment || 'null',
  ];

  buildFindByQuery(dto: ColumnQueryDto): Query {
    const binds: (string | number)[] = [];
    let whereClause = '';

    if (dto.relationNames && dto.relationNames.length) {
      binds.push(...dto.relationNames);
      const whereCondition = `array_contains(relation_name::variant, array_construct(${dto.relationNames
        .map(() => '?')
        .join(',')}))`;

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.names && dto.names.length) {
      binds.push(...dto.names);

      const whereCondition = `array_contains(name::variant, array_construct(${dto.names
        .map(() => '?')
        .join(',')}))`;

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.index) {
      binds.push(dto.index);
      const whereCondition = 'index = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.type) {
      binds.push(dto.type);
      const whereCondition = 'type = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.materializationIds && dto.materializationIds.length) {
      binds.push(...dto.materializationIds);

      const whereCondition = `array_contains(materializationId::variant, array_construct(${dto.materializationIds
        .map(() => '?')
        .join(',')}))`;

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }

    const text = `select * from cito.lineage.${this.matName}
        where  ${whereClause};`;

    return { binds, text };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(
        dto
      )}]`
    );
  }

  toEntity = (columnProperties: ColumnProps): Column =>
    Column.build(columnProperties);
}
