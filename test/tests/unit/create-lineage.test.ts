import { ObjectId } from 'mongodb';
import { Lineage } from '../../../src/domain/entities/lineage';

test('lineage creation', async () => {
  const result = Lineage.create({ id: new ObjectId().toHexString() });

  console.log(result.id);

  expect(!!result).toBe(true);
});
