import axios, { AxiosRequestConfig } from 'axios';
import { appConfig } from '../../../src/config';
import { GetAccounts } from '../../../src/domain/account-api/get-accounts';
import { CreateColumnAuthDto } from '../../../src/domain/column/create-column';
import { CreateLineage, CreateLineageRequestDto } from '../../../src/domain/lineage/create-lineage/create-lineage';
import ExpressApp from '../../../src/infrastructure/api/express-app';
import iocRegister from '../../../src/infrastructure/ioc-register';
import { handler } from '../../../src/lambda';
import { BaseController } from '../../../src/shared/base-controller';

// node --inspect-brk node_modules/.bin/jest -- create-lineage.test.ts --runInBand

// npm test -- create-lineage.test.ts

export interface SystemInternalAuthConfig {
  clientSecret: string;
  clientId: string;
  tokenUrl: string;
} 

const getSystemInternalAuthConfig = (): SystemInternalAuthConfig => {
  const clientSecret = '11ebmsj102ulmsljeqlomqq0bgo3155q8td0ui1t0d7rtek3ppqc';
  if (!clientSecret) throw new Error('auth client secret missing');

  const clientId = '54n1ig9sb07d4d9tiihdi0kifq';
  const tokenUrl = 'https://auth.citodata.com/oauth2/token';
  return { clientSecret, clientId, tokenUrl };
};

const getJwt = async (): Promise<string> => {
  try {
    const authConfig = getSystemInternalAuthConfig();

    const configuration: AxiosRequestConfig = {
      headers: {
        Authorization: `Basic ${Buffer.from(
          `${authConfig.clientId}:${authConfig.clientSecret}`
        ).toString('base64')}`,
        'Content-Type': 'application/x-www-form-urlencoded',
      },
      params: new URLSearchParams({
        grant_type: 'client_credentials',
        client_id: authConfig.clientId,
      }),
    };

    const response = await axios.post(
      authConfig.tokenUrl,
      undefined,
      configuration
    );
    const jsonResponse = response.data;
    if (response.status !== 200) throw new Error(jsonResponse.message);
    if (!jsonResponse.access_token)
      throw new Error('Did not receive an access token');
    return jsonResponse.access_token;
  } catch (error: unknown) {
    if (error instanceof Error ) console.error(error.stack);
    else if (error) console.trace(error);
    return Promise.reject(new Error(''));
  }
};

test('lineage creation', async () => {
  const jwt = await getJwt();

  const createLineage: CreateLineage = iocRegister.resolve('createLineage');

  const reqDto: CreateLineageRequestDto = {
    targetOrgId: '631789bf27518f97cf1c82b7',
  };

  const getAccounts: GetAccounts = iocRegister.resolve('getAccounts');
  const accountInfoResult = await BaseController.getUserAccountInfo(
    jwt,
    getAccounts
  );

  if (!accountInfoResult.success) throw new Error(accountInfoResult.error);

  const accountInfo = accountInfoResult.value;
  if (!accountInfo) throw new Error('Missing account info value');

  const auth: CreateColumnAuthDto = {
    jwt,
    isSystemInternal: accountInfo.isSystemInternal,
    callerOrgId: accountInfo.callerOrgId,
  };

  const result = await createLineage.execute(reqDto, auth);

  if(!result.success) throw new Error(result.error);
  
  console.log(result.value);

  expect(result.success).toBe(true);
});
