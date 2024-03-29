import { Document } from 'mongodb';
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

  buildEntityProps = (document: Document): ColumnProps => {
    const {
      id,
      name,
      relation_name: relationName,
      index,
      data_type: dataType,
      is_identity: isIdentity,
      is_nullable: isNullable,
      materialization_id: materializationId,
      comment,
    } = document;

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

  getValues = (entity: Column): (string | number | boolean)[] => [
    entity.id,
    entity.name,
    entity.relationName,
    entity.index,
    entity.dataType,
    entity.isIdentity !== undefined ? entity.isIdentity : 'null',
    entity.isNullable !== undefined ? entity.isNullable : 'null',
    entity.materializationId,
    entity.comment || 'null',
  ];

  buildFindByQuery(dto: ColumnQueryDto): Query {
    const values: (string | number)[] = [];
    const filter: any = {};

    if (dto.relationNames && dto.relationNames.length) {
      filter.relation_name = { $in: dto.relationNames };
    }

    if (dto.names && dto.names.length) {
      filter.name = { $in: dto.names };
    }

    if (dto.index) {
      values.push(dto.index);
      filter.index = dto.index;
    }

    if (dto.type) {
      values.push(dto.type);
      filter.type = dto.type;
    }
    
    if (dto.materializationIds && dto.materializationIds.length) {
      filter.materialization_id = { $in: dto.materializationIds };
    }

    return { values, filter };
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
