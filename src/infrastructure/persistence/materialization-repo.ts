import {
  Materialization,
  MaterializationProps,
  parseMaterializationType,
} from '../../domain/entities/materialization';
import { ColumnDefinition } from './shared/query';
import { Bind, SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { Query } from './shared/base-sf-repo';
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
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
    { name: 'comment', nullable: true },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (sfEntity: SnowflakeEntity): MaterializationProps => {
    const {
      ID: id,
      NAME: name,
      SCHEMA_NAME: schemaName,
      DATABASE_NAME: databaseName,
      RELATION_NAME: relationName,
      TYPE: type,
      IS_TRANSIENT: isTransient,
      LOGIC_ID: logicId,
      OWNER_ID: ownerId,
      LINEAGE_IDS: lineageIds,
      COMMENT: comment,
    } = sfEntity;

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
      !MaterializationRepo.isStringArray(lineageIds) ||
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
      lineageIds,
      comment,
    };
  };

  getBinds = (el: Materialization): Bind[] => [
    el.id,
    el.name,
    el.schemaName,
    el.databaseName,
    el.relationName,
    el.type,
    el.isTransient !== undefined ? el.isTransient.toString() : 'null',
    el.logicId || 'null',
    el.ownerId || 'null',
    JSON.stringify(el.lineageIds),
    el.comment || 'null',
  ];

  buildFindByQuery(dto: MaterializationQueryDto): Query {
    const binds: Bind[] = [dto.lineageId];
    let whereClause = 'array_contains(?::variant, lineage_ids) ';

    if (dto.relationName) {
      binds.push(dto.relationName);
      whereClause = whereClause.concat('and relation_name = ? ');
    }
    if (dto.type) {
      binds.push(dto.type);
      whereClause = whereClause.concat('and type = ? ');
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
    if (dto.schemaName) {
      binds.push(dto.schemaName);
      whereClause = whereClause.concat('and schema_name = ? ');
    }
    if (dto.databaseName) {
      binds.push(dto.databaseName);
      whereClause = whereClause.concat('and database_name = ? ');
    }
    if (dto.logicId) {
      binds.push(dto.logicId);
      whereClause = whereClause.concat('and logic_id = ? ');
    }

    const text = `select * from cito.lineage.${this.matName}
    where  ${whereClause};`;

    return { text, binds };
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
