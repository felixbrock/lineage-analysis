import { v4 as uuidv4 } from 'uuid';
import { Lineage } from '../../entities/lineage';

export type BuildLineageResult = Lineage;

/* Building a new lineage object that is referenced by resources like columns and materializations */
export const buildLineage = (): BuildLineageResult =>
  // todo - enable lineage updating
  // this.#lineage =
  //   lineageId && lineageCreatedAt
  //     ? Lineage.create({
  //         id: lineageId,
  //         createdAt: lineageCreatedAt,
  //       })
  //     : Lineage.create({ id: uuidv4() });

  Lineage.create({
    id: uuidv4(),
  });
