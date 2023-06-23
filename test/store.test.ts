import { describe, it, beforeAll, expect, afterAll } from 'vitest';
import {
  CreateTableCommand,
  DeleteTableCommand,
  DynamoDBClient,
  UpdateTimeToLiveCommand
} from '@aws-sdk/client-dynamodb';

import { create, DynamoDBStore } from '../src';
import { msToS } from '../src/utils';

describe('DynamoDBStore', function () {
  let client: DynamoDBClient;
  let store: DynamoDBStore;
  const config = {
    ttl: 300000,
    table: 'TestCache',
    keys: { pk: 'Id', sk: 'Name', ex: 'ExpiresAt' },
    meta: () => ({ CreatedAt: msToS(Date.now()) })
  };

  beforeAll(async () => {
    client = new DynamoDBClient({ endpoint: 'http://localhost:4566', region: 'us-east-1' });
    store = create({ ...config, dynamodb: client });

    await client.send(
      new CreateTableCommand({
        AttributeDefinitions: [
          {
            AttributeName: config.keys.pk,
            AttributeType: 'S'
          },
          {
            AttributeName: config.keys.sk,
            AttributeType: 'S'
          }
        ],
        KeySchema: [
          {
            AttributeName: config.keys.pk,
            KeyType: 'HASH'
          },
          {
            AttributeName: config.keys.sk,
            KeyType: 'RANGE'
          }
        ],
        ProvisionedThroughput: {
          ReadCapacityUnits: 5,
          WriteCapacityUnits: 5
        },
        TableName: config.table
      })
    );
    await client.send(
      new UpdateTimeToLiveCommand({
        TableName: config.table,
        TimeToLiveSpecification: {
          Enabled: true,
          AttributeName: config.keys.ex
        }
      })
    );
  });

  afterAll(async () => {
    await client.send(new DeleteTableCommand({ TableName: config.table }));
  });

  it('should set and get item', async () => {
    const value = {
      a: [{ b: 'c' }]
    };
    await store.set('123+foo', value);
    const item = await store.get('123+foo');

    expect(item).toStrictEqual(value);
  });

  it('should delete item', async () => {
    await store.del('123+foo');
    const item = await store.get('123+foo');

    expect(item).toBeUndefined();
  });

  it('should set and get batch of items', async () => {
    const key1 = '123+foo';
    const value1 = 1000;
    const key2 = '123+baz';
    const value2 = null;
    const key3 = '123+bar';
    const value3 = [{ a: 'b' }];
    const key4 = '456+bob';
    const value4 = { c: ['d'] };
    await store.mset([
      [key1, value1],
      [key2, value2],
      [key3, value3],
      [key4, value4]
    ]);
    const items = await store.mget(key1, key2, key3, key4);

    expect(items).toStrictEqual([value1, value2, value3, value4]);
  });

  it('should find keys by pattern', async () => {
    const keys = await store.keys('123+b*');

    expect(keys.sort()).toStrictEqual(['123+bar', '123+baz']);
  });

  it('should find all keys', async () => {
    const keys = await store.keys();

    expect(keys.sort()).toStrictEqual(['123+bar', '123+baz', '123+foo', '456+bob']);
  });

  it('should get item ttl', async () => {
    const ttl = await store.ttl('123+foo');

    expect(ttl > 0).toBeTruthy();
  });

  it('should update only ttl', async () => {
    const ttl = await store.ttl('123+foo');
    await store.touch('123+foo', config.ttl * 1000);

    const updated = await store.get('123+foo');
    const newTtl = await store.ttl('123+foo');

    expect(ttl.toString().length < newTtl.toString().length).toBeTruthy();
    expect(updated).toBe(1000);
  });

  it('should delete batch of items', async () => {
    const keys = ['123+foo', '123+baz'];
    await store.mdel(...keys);
    const items = await store.mget(...keys);

    expect(items).toStrictEqual([undefined, undefined]);
  });

  it('should reset cache', async () => {
    await store.reset();
    const items = await store.mget('123+bar', '456+bob');

    expect(items).toStrictEqual([undefined, undefined]);
  });
});
