import {
  Materialization,
  MaterializationProps,
  parseMaterializationType,
} from '../../domain/entities/materialization';
import {
  Bind,
  SnowflakeEntity,
} from '../../domain/snowflake-api/i-snowflake-api-repo';
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
    el.comment || 'null',
  ];

  buildFindByQuery(dto: MaterializationQueryDto): Query {
    const binds: (string | number)[] = [];
    let whereClause = '';

    if (dto.ids && dto.ids.length) {
      binds.push(
        dto.ids.length === 1
          ? dto.ids[0]
          : dto.ids.map((el) => `'${el}'`).join(', ')
      );
      const whereCondition =
        'and array_contains(id::variant, array_construct(?))';
      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.relationName) {
      binds.push(dto.relationName);
      const whereCondition = 'and relation_name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.type) {
      binds.push(dto.type);
      const whereCondition = 'and type = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.names && dto.names.length) {
      binds.push(
        dto.names.length === 1
          ? dto.names[0]
          : dto.names.map((el) => `'${el}'`).join(', ')
      );
      const whereCondition =
        'and array_contains(name::variant, array_construct(?))';
      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.schemaName) {
      binds.push(dto.schemaName);
      const whereCondition = 'and schema_name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.databaseName) {
      binds.push(dto.databaseName);
      const whereCondition = 'and database_name = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }
    if (dto.logicId) {
      binds.push(dto.logicId);
      const whereCondition = 'and logic_id = ?';

      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
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
