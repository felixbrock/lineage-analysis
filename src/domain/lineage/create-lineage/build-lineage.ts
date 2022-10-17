import { ObjectId } from 'mongodb';
import { Lineage } from '../../entities/lineage';

export type BuildLineageResult = Lineage;

/* Building a new lineage object that is referenced by resources like columns and materializations */
export const buildLineage = (organizationId: string): BuildLineageResult =>
  // todo - enable lineage updating
  // this.#lineage =
  //   lineageId && lineageCreatedAt
  //     ? Lineage.create({
  //         id: lineageId,
  //         createdAt: lineageCreatedAt,
  //       })
  //     : Lineage.create({ id: new ObjectId().toHexString() });

  Lineage.create({
    id: new ObjectId().toHexString(),
    organizationId,
  });
