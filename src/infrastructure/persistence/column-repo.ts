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

import { Bind, SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { Query } from './shared/base-sf-repo';
import { ColumnDefinition } from './shared/query';

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
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
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
      LINEAGE_IDS: lineageIds,
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
      !ColumnRepo.isStringArray(lineageIds) ||
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
      lineageIds,
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
    JSON.stringify(entity.lineageIds),
    entity.comment || 'null',
  ];

  buildFindByQuery(dto: ColumnQueryDto): Query {
    const binds: Bind[] = [dto.lineageId];
    let whereClause = 'array_contains(?::variant, lineage_ids) ';

    if (dto.relationName) {
      binds.push(
        Array.isArray(dto.relationName)
          ? dto.relationName.map((el) => `'${el}'`).join(', ')
          : dto.relationName
      );
      whereClause = whereClause.concat(
        Array.isArray(dto.relationName)
          ? 'and array_contains(relation_name::variant, array_construct(?))'
          : 'and relation_name = ? '
      );
    }
    if (dto.name) {
      binds.push(
        Array.isArray(dto.name)
          ? dto.name.map((el) => `'${el}'`).join(', ')
          : dto.name
      );
      whereClause = whereClause.concat(
        Array.isArray(dto.name)
          ? 'and array_contains(name::variant, array_construct(?))'
          : 'and name = ? '
      );
    }
    if (dto.index) {
      binds.push(dto.index);
      whereClause = whereClause.concat('and index = ? ');
    }
    if (dto.type) {
      binds.push(dto.type);
      whereClause = whereClause.concat('and type = ? ');
    }
    if (dto.materializationId) {
      binds.push(
        Array.isArray(dto.materializationId)
          ? dto.materializationId.map((el) => `'${el}'`).join(', ')
          : dto.materializationId
      );
      whereClause = whereClause.concat(
        Array.isArray(dto.materializationId)
          ? 'and array_contains(materializationId::variant, array_construct(?))'
          : 'and materialization_id = ? '
      );
    }

    const text = `select * from cito.lineage.${this.matName}
        where  ${whereClause};`;

    return { binds, text };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(dto)}]`
    );
  }

  toEntity = (columnProperties: ColumnProps): Column =>
    Column.build(columnProperties);
}
