import {
  DependentOn,
  Logic,
  LogicProps,
  Refs,
} from '../../domain/entities/logic';
import { ColumnDefinition } from './shared/query';
import { SnowflakeEntity } from '../../domain/snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../../domain/snowflake-api/query-snowflake';
import BaseSfRepo, { Query } from './shared/base-sf-repo';
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
    { name: 'lineage_ids', selectType: 'parse_json', nullable: false },
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
      LINEAGE_IDS: lineageIds,
    } = sfEntity;

    if (
      typeof id !== 'string' ||
      typeof relationName !== 'string' ||
      typeof sql !== 'string' ||
      typeof dependentOn !== 'object' ||
      typeof parsedLogic !== 'string' ||
      typeof statementRefs !== 'object' ||
      typeof lineageIds !== 'object'
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

    if (
      !isDependentOnObj(dependentOn) ||
      !isRefsObj(statementRefs) ||
      !LogicRepo.isStringArray(lineageIds)
    )
      throw new Error(
        'Type mismatch detected when reading logic from persistence'
      );

    return {
      id,
      sql,
      relationName,
      dependentOn,
      lineageIds,
      parsedLogic,
      statementRefs,
    };
  };

  getBinds = (entity: Logic): (string | number)[] => [
    entity.id,
    entity.relationName,
    entity.sql,
    JSON.stringify(entity.dependentOn),
    entity.parsedLogic,
    JSON.stringify(entity.statementRefs),
    JSON.stringify(entity.lineageIds),
  ];

  buildFindByQuery(dto: LogicQueryDto): Query {
    const binds: (string | number)[] = [dto.lineageId];
    if (dto.relationName) binds.push(dto.relationName);

    const text = `select * from cito.lineage.${this.matName}
  where array_contains(?::variant, lineage_ids) ${
    dto.relationName ? 'and relation_name = ?' : ''
  };`;
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
