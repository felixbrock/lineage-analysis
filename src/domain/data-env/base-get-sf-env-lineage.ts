import { v4 as uuidv4 } from 'uuid';
import { Dependency } from '../entities/dependency';
import BaseAuth from '../services/base-auth';
import { IConnectionPool } from '../snowflake-api/i-snowflake-api-repo';
import { QuerySnowflake } from '../snowflake-api/query-snowflake';

export const sfObjRefTypes = ['TABLE', 'VIEW'] as const;
export type SfObjRefType = typeof sfObjRefTypes[number];

export const parseSfObjRefType = (type: unknown): SfObjRefType => {
  if (typeof type !== 'string')
    throw new Error('Provision of type in non-string format');

  const identifiedElement = sfObjRefTypes.find(
    (element) => element.toLowerCase() === type.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

interface SfObjectRef {
  id: number;
  dbName: string;
  schemaName: string;
  matName: string;
  type: SfObjRefType;
}

export const sfObjDependencyTypes = [
  'BY_NAME',
  'BY_ID',
  'BY_NAME_AND_ID',
] as const;
export type SfObjDependencyType = typeof sfObjDependencyTypes[number];

export const parseSfObjDependencyType = (
  type: unknown
): SfObjDependencyType => {
  if (typeof type !== 'string')
    throw new Error('Provision of type in non-string format');

  const identifiedElement = sfObjDependencyTypes.find(
    (element) => element.toLowerCase() === type.toLowerCase()
  );
  if (identifiedElement) return identifiedElement;
  throw new Error('Provision of invalid type');
};

interface SfObjectDependency {
  head: SfObjectRef;
  tail: SfObjectRef;
  type: SfObjDependencyType;
}

export interface CitoMatRepresentation {
  id: string;
  relationName: string;
}

interface Auth extends Omit<BaseAuth, 'callerOrgId'> {
  callerOrgId: string;
}

export default abstract class BaseGetSfEnvLineage {
  readonly querySnowflake: QuerySnowflake;

  protected connPool?: IConnectionPool;

  protected auth?: Auth;

  constructor(querySnowflake: QuerySnowflake) {
    this.querySnowflake = querySnowflake;
  }

  #getDistinctRelationNames = async (
    sfObjDependencies: SfObjectDependency[]
  ): Promise<string[]> => {
    const distinctRelationNames = sfObjDependencies.reduce(
      (accumulation: string[], val: SfObjectDependency) => {
        const localAcc = accumulation;

        const refNameHead = `${val.head.dbName}.${val.head.schemaName}.${val.head.matName}`;
        const refNameTail = `${val.tail.dbName}.${val.tail.schemaName}.${val.tail.matName}`;

        if (!localAcc.includes(refNameHead)) localAcc.push(refNameHead);

        if (!localAcc.includes(refNameTail)) localAcc.push(refNameTail);

        return localAcc;
      },
      []
    );

    return distinctRelationNames;
  };

  #getAllLineageMats = async (
    relationNames: string[]
  ): Promise<CitoMatRepresentation[]> => {
    if (!this.connPool || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select id, relation_name from cito.lineage.materializations where array_contains((database_name ||'.' ||  schema_name ||'.' ||  name)::variant, array_construct(${relationNames
      .map((el) => `'${el}'`)
      .join(', ')}
    ));`;

    const queryResult = await this.querySnowflake.execute(
      { queryText, binds: [] },
      this.auth,
      this.connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const citoMatRepresentations = results.map((el): CitoMatRepresentation => {
      const { ID: id, RELATION_NAME: relationName } = el;

      if (typeof id !== 'string' || typeof relationName !== 'string')
        throw new Error('Received cito mat in unexpected format');

      return { id, relationName };
    });

    return citoMatRepresentations;
  };

  #getSfObjectDependencies = async (): Promise<SfObjectDependency[]> => {
    if (!this.connPool || !this.auth)
      throw new Error('Missing properties for generating sf data env');

    const queryText = `select * from snowflake.account_usage.object_dependencies;`;
    const queryResult = await this.querySnowflake.execute(
      { queryText, binds: [] },
      this.auth,
      this.connPool
    );
    if (!queryResult.success) {
      throw new Error(queryResult.error);
    }
    if (!queryResult.value) throw new Error('Query did not return a value');

    const results = queryResult.value;

    const dependencies: SfObjectDependency[] = results.map((el) => {
      const {
        REFERENCED_DATABASE: headDbName,
        REFERENCED_SCHEMA: headSchemaName,
        REFERENCED_OBJECT_NAME: headMatName,
        REFERENCED_OBJECT_ID: headObjId,
        REFERENCED_OBJECT_DOMAIN: headObjType,
        REFERENCING_DATABASE: tailDbName,
        REFERENCING_SCHEMA: tailSchemaName,
        REFERENCING_OBJECT_NAME: tailMatName,
        REFERENCING_OBJECT_ID: tailObjId,
        REFERENCING_OBJECT_DOMAIN: tailObjType,
        DEPENDENCY_TYPE: objDependencyType,
      } = el;

      if (
        typeof headDbName !== 'string' ||
        typeof headSchemaName !== 'string' ||
        typeof headMatName !== 'string' ||
        typeof headObjId !== 'number' ||
        typeof tailDbName !== 'string' ||
        typeof tailSchemaName !== 'string' ||
        typeof tailMatName !== 'string' ||
        typeof tailObjId !== 'number'
      )
        throw new Error('Received sf obj representation in unexpected format');

      const head: SfObjectRef = {
        id: headObjId,
        type: parseSfObjRefType(headObjType),
        dbName: headDbName,
        schemaName: headSchemaName,
        matName: headMatName,
      };
      const tail: SfObjectRef = {
        id: tailObjId,
        type: parseSfObjRefType(tailObjType),
        dbName: tailDbName,
        schemaName: tailSchemaName,
        matName: tailMatName,
      };

      return {
        head,
        tail,
        type: parseSfObjDependencyType(objDependencyType),
      };
    });

    return dependencies;
  };

  #filterReferencedMats = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: CitoMatRepresentation[]
  ): Promise<CitoMatRepresentation[]> => {
    const distinctRelationNames = sfObjDependencies.reduce(
      (accumulation: string[], val: SfObjectDependency) => {
        const localAcc = accumulation;

        const refNameHead = `${val.head.dbName}.${val.head.schemaName}.${val.head.matName}`;
        const refNameTail = `${val.tail.dbName}.${val.tail.schemaName}.${val.tail.matName}`;

        if (!localAcc.includes(refNameHead)) localAcc.push(refNameHead);

        if (!localAcc.includes(refNameTail)) localAcc.push(refNameTail);

        return localAcc;
      },
      []
    );

    const referencedMats = matReps.filter((el) =>
      distinctRelationNames.includes(el.relationName)
    );

    return referencedMats;
  };

  #buildDataDependencies = async (
    sfObjDependencies: SfObjectDependency[],
    matReps: CitoMatRepresentation[]
  ): Promise<Dependency[]> => {
    const referencedMatReps = await this.#filterReferencedMats(
      sfObjDependencies,
      matReps
    );

    const dependencies = sfObjDependencies.map((el): Dependency => {
      const headRelationName = `${el.head.dbName}.${el.head.schemaName}.${el.head.matName}`;
      const headMat = referencedMatReps.find(
        (entry) => entry.relationName === headRelationName
      );
      if (!headMat) throw new Error('Mat representation for head not found ');

      const tailRelationName = `${el.tail.dbName}.${el.tail.schemaName}.${el.tail.matName}`;
      const tailMat = referencedMatReps.find(
        (entry) => entry.relationName === tailRelationName
      );
      if (!tailMat) throw new Error('Mat representation for tail not found ');

      return Dependency.create({
        id: uuidv4(),
        headId: headMat.id,
        tailId: tailMat.id,
        type: 'data',
      });
    });

    return dependencies;
  };

  protected generateDependencies = async (): Promise<Dependency[]> => {
    const sfObjDependencies = await this.#getSfObjectDependencies();

    const distinctRelationNames = await this.#getDistinctRelationNames(
      sfObjDependencies
    );

    const citoMatReps = await this.#getAllLineageMats(distinctRelationNames);

    const dependencies = await this.#buildDataDependencies(
      sfObjDependencies,
      citoMatReps
    );

    return dependencies;
  };
}
