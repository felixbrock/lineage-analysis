import { Document } from 'mongodb';
import {
  DependentOn,
  Logic,
  LogicProps,
  Refs,
} from '../../domain/entities/logic';
import {
  Bind,
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

  buildEntityProps = (document: Document): LogicProps => {
    const {
      id,
      relation_name: relationName,
      sql,
      dependent_on: dependentOn,
      parsed_logic: parsedLogic,
      statement_refs: statementRefs,
    } = document;

    const dependentOnObject = JSON.parse(dependentOn);
    const statementRefsObject = JSON.parse(statementRefs);

    if (
      typeof id !== 'string' ||
      typeof relationName !== 'string' ||
      typeof sql !== 'string' ||
      typeof dependentOnObject !== 'object' ||
      typeof parsedLogic !== 'string' ||
      typeof statementRefsObject !== 'object'
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

    if (!isDependentOnObj(dependentOnObject) || !isRefsObj(statementRefsObject))
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
    const filter: any = {};

    if (dto.relationNames && dto.relationNames.length) {
      filter.relation_name = { $in: dto.relationNames };
    }

    return { filter, binds };
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
