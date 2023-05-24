import { Document } from 'mongodb';
import {
  Materialization,
  MaterializationProps,
  parseMaterializationType,
} from '../../domain/entities/materialization';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';
import {
  IMaterializationRepo,
  MaterializationQueryDto,
  MaterializationUpdateDto,
} from '../../domain/materialization/i-materialization-repo';

export default class MaterializationRepo
  extends BaseSfRepo<
    Materialization,
    MaterializationProps,
    MaterializationQueryDto,
    MaterializationUpdateDto
  >
  implements IMaterializationRepo
{
  readonly matName = 'materializations';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'name', nullable: false },
    { name: 'schema_name', nullable: false },
    { name: 'database_name', nullable: false },
    { name: 'relation_name', nullable: false },
    { name: 'type', nullable: false },
    { name: 'is_transient', nullable: true },
    { name: 'logic_id', nullable: true },
    { name: 'owner_id', nullable: true },
    { name: 'comment', nullable: true },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (document: Document): MaterializationProps => {
    const {
      id,
      name,
      schema_name: schemaName,
      database_name: databaseName,
      relation_name: relationName,
      type,
      is_transient: isTransient,
      logic_id: logicId,
      owner_id: ownerId,
      comment,
    } = document;

    if (
      typeof id !== 'string' ||
      typeof name !== 'string' ||
      typeof schemaName !== 'string' ||
      typeof databaseName !== 'string' ||
      typeof relationName !== 'string' ||
      typeof type !== 'string'
    )
      throw new Error(
        'Retrieved unexpected materialization field types from persistence'
      );

    if (
      !MaterializationRepo.isOptionalOfType<boolean>(isTransient, 'boolean') ||
      !MaterializationRepo.isOptionalOfType<string>(logicId, 'string') ||
      !MaterializationRepo.isOptionalOfType<string>(ownerId, 'string') ||
      !MaterializationRepo.isOptionalOfType<string>(comment, 'string')
    )
      throw new Error(
        'Type mismatch detected when reading materialization from persistence'
      );

    return {
      id,
      name,
      schemaName,
      databaseName,
      relationName,
      type: parseMaterializationType(type),
      isTransient,
      logicId,
      ownerId,
      comment,
    };
  };

  getValues = (el: Materialization): (string | number | boolean)[] => [
    el.id,
    el.name,
    el.schemaName,
    el.databaseName,
    el.relationName,
    el.type,
    el.isTransient !== undefined ? el.isTransient : 'null',
    el.logicId || 'null',
    el.ownerId || 'null',
    el.comment || 'null',
  ];

  buildFindByQuery(dto: MaterializationQueryDto): Query {
    const values: (string | number)[] = [];
    const filter: any = {};

    if (dto.ids && dto.ids.length) {
      filter.id = { $in: dto.ids };
    }
    if (dto.relationName) {
      values.push(dto.relationName);
      filter.relation_name = dto.relationName;
    }
    if (dto.type) {
      values.push(dto.type);
      filter.type = dto.type;
    }
    if (dto.names && dto.names.length) {
      filter.name = { $in: dto.names };
    }
    if (dto.schemaName) {
      values.push(dto.schemaName);
      filter.schema_name = dto.schemaName;
    }
    if (dto.databaseName) {
      values.push(dto.databaseName);
      filter.database_name = dto.databaseName;
    }
    if (dto.logicId) {
      values.push(dto.logicId);
      filter.logic_id = dto.logicId;
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

  toEntity = (materializationProps: MaterializationProps): Materialization =>
    Materialization.build(materializationProps);
}
