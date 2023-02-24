import {
  DependentOn,
  Logic,
  LogicProps,
  Refs,
} from '../../domain/entities/logic';
import {
  Bind,
  SnowflakeEntity,
} from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { ColumnDefinition, Query } from './shared/base-sf-repo';
import {
  ILogicRepo,
  LogicQueryDto,
  LogicUpdateDto,
} from '../../domain/logic/i-logic-repo';

export default class LogicRepo
  extends BaseSfRepo<Logic, LogicProps, LogicQueryDto, LogicUpdateDto>
  implements ILogicRepo
{
  readonly matName = 'logics';

  readonly colDefinitions: ColumnDefinition[] = [
    { name: 'id', nullable: false },
    { name: 'relation_name', nullable: false },
    { name: 'sql', nullable: false },
    { name: 'dependent_on', selectType: 'parse_json', nullable: false },
    { name: 'parsed_logic', nullable: false },
    { name: 'statement_refs', selectType: 'parse_json', nullable: false },
  ];

  // eslint-disable-next-line @typescript-eslint/no-useless-constructor
  constructor(querySnowflake: QuerySnowflake) {
    super(querySnowflake);
  }

  buildEntityProps = (sfEntity: SnowflakeEntity): LogicProps => {
    const {
      ID: id,
      RELATION_NAME: relationName,
      SQL: sql,
      DEPENDENT_ON: dependentOn,
      PARSED_LOGIC: parsedLogic,
      STATEMENT_REFS: statementRefs,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof relationName !== 'string' ||
      typeof sql !== 'string' ||
      typeof dependentOn !== 'object' ||
      typeof parsedLogic !== 'string' ||
      typeof statementRefs !== 'object'
    )
      throw new Error(
        'Retrieved unexpected logic field types from persistence'
      );

    const isDependentOnObj = (el: unknown): el is DependentOn =>
      !!el &&
      'dbtDependencyDefinitions' in (el as DependentOn) &&
      'dwDependencyDefinitions' in (el as DependentOn);
    const isRefsObj = (el: unknown): el is Refs =>
      !!el &&
      'materializations' in (el as Refs) &&
      'columns' in (el as Refs) &&
      'wildcards' in (el as Refs);

    if (!isDependentOnObj(dependentOn) || !isRefsObj(statementRefs))
      throw new Error(
        'Type mismatch detected when reading logic from persistence'
      );

    return {
      id,
      sql,
      relationName,
      dependentOn,
      parsedLogic,
      statementRefs,
    };
  };

  getBinds = (entity: Logic): Bind[] => [
    entity.id,
    entity.relationName,
    entity.sql,
    JSON.stringify(entity.dependentOn),
    entity.parsedLogic,
    JSON.stringify(entity.statementRefs),
  ];

  buildFindByQuery(dto: LogicQueryDto): Query {
    const binds: (string | number)[] = [];
    let whereClause = '';

    if (dto.relationNames && dto.relationNames.length) {
      const whereCondition = `array_contains(relation_name::variant, array_construct(${dto.relationNames
        .map((el) => `'${el}'`)
        .join(',')}))`;
      whereClause = whereClause
        ? whereClause.concat(`and ${whereCondition} `)
        : whereCondition;
    }

    const text = `select * from cito.lineage.${this.matName}
  ${whereClause ? 'where' : ''} ${whereClause};`;

    return { text, binds };
  }

  buildUpdateQuery(id: string, dto: undefined): Query {
    throw new Error(
      `Update Method not implemented. Provided Input [${id}, ${JSON.stringify(
        dto
      )}]`
    );
  }

  toEntity = (logicProperties: LogicProps): Logic =>
    Logic.build(logicProperties);
}
