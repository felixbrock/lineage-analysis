import dotenv from 'dotenv';

dotenv.config();

import app from '../../../src/infrastructure/ioc-register';
import fs from 'fs';

test('logic creation', async () => {
  const createLogic = app.resolve('createLogic');

  const parsedLogic = fs.readFileSync(
    'C:/Users/felix-pc/Hivedive Limited/Hivedive - Documents/Problem28/Product/Sample1SQLModels/model-snowflake_usage-V_DATE_STG.json',
    'utf-8'
  );

  const reqDto = {
    dbtModelId: 'model.snowflake_usage.V_DATE_STG',
    lineageId: '62701d10305dcf107071fa00',
    parsedLogic,
  };

  const auth = { organizationId: 'todo' };

  const result = await createLogic.execute(reqDto, auth);

  console.log(result.error);

  expect(result.success).toBe(true);
});
