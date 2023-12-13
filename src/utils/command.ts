import { Milliseconds } from 'cache-manager';
import {
  ReturnValue,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  AttributeValue,
  KeysAndAttributes,
  WriteRequest
} from '@aws-sdk/client-dynamodb';

import { Config } from '../types';
import { deserializeKey, msToS, eraseMask } from './item';

export const buildGetInput = (key: string, config: Config) => ({
  Key: deserializeKey(key, config),
  TableName: config.table,
  ReturnConsumedCapacity: ReturnConsumedCapacity.NONE
});

export const buildSetInput = (
  key: string,
  data: Record<string, AttributeValue>,
  config: Config & { ttl: Milliseconds }
) => {
  const expiresAt = msToS(Date.now() + config.ttl).toString();
  const Item = {
    ...data,
    ...deserializeKey(key, config),
    [config.keys.ex]: { N: expiresAt }
  };

  return {
    Item,
    TableName: config.table,
    ReturnValues: ReturnValue.NONE,
    ReturnConsumedCapacity: ReturnConsumedCapacity.NONE,
    ReturnItemCollectionMetrics: ReturnItemCollectionMetrics.NONE
  };
};

export const buildDelInput = (key: string, config: Config) => ({
  Key: deserializeKey(key, config),
  TableName: config.table
});

export const buildMGetInput = (
  keys: string[],
  config: Config & { unprocessed?: Record<string, KeysAndAttributes> }
) => ({
  RequestItems: config.unprocessed || {
    [config.table]: { Keys: keys.map((key) => deserializeKey(key, config)) }
  }
});

export const buildMSetInput = (
  args: [string, Record<string, AttributeValue>][],
  config: Config & { ttl: Milliseconds; unprocessed?: Record<string, WriteRequest[]> }
) => {
  const items = args.map(([key, data]) => {
    const expiresAt = msToS(Date.now() + config.ttl).toString();
    const Item = { ...data, ...deserializeKey(key, config), [config.keys.ex]: { N: expiresAt } };

    return { PutRequest: { Item } };
  });

  return {
    RequestItems: config.unprocessed || { [config.table]: items },
    ReturnValue: ReturnValue.NONE,
    ReturnConsumedCapacity: ReturnConsumedCapacity.NONE,
    ReturnItemCollectionMetrics: ReturnItemCollectionMetrics.NONE
  };
};

export const buildMDelInput = (
  keys: string[],
  config: Config & { unprocessed?: Record<string, WriteRequest[]> }
) => ({
  RequestItems: config.unprocessed || {
    [config.table]: keys.map((key) => ({ DeleteRequest: { Key: deserializeKey(key, config) } }))
  }
});

export const buildScanKeysInput = (
  cursor: Record<string, AttributeValue> | undefined,
  config: Config
) => {
  const input = {
    TableName: config.table,
    ExpressionAttributeNames: { '#pk': config.keys.pk, '#sk': config.keys.sk },
    ProjectionExpression: config.keys.sk ? '#pk, #sk' : '#pk'
  };

  if (cursor) {
    Object.assign(input, { ExclusiveStartKey: cursor });
  }

  return input;
};

export const buildQueryKeysInput = (
  pattern: string,
  cursor: Record<string, AttributeValue> | undefined,
  config: Config & { keys: Required<Config['keys']> }
) => {
  const key = deserializeKey(eraseMask(pattern), config);
  const pk = key[config.keys.pk];
  const sk = key[config.keys.sk];
  const input = {
    TableName: config.table,
    ProjectionExpression: '#pk, #sk',
    ExpressionAttributeValues: { ':pk': pk, ':sk': sk },
    KeyConditionExpression: '#pk = :pk and begins_with(#sk, :sk)',
    ExpressionAttributeNames: {
      '#pk': config.keys.pk,
      '#sk': config.keys.sk
    }
  };

  if (cursor) {
    Object.assign(input, { ExclusiveStartKey: cursor });
  }

  return input;
};

export const buildTTLInput = (key: string, config: Config) => ({
  Key: deserializeKey(key, config),
  TableName: config.table,
  ExpressionAttributeNames: { '#ex': config.keys.ex },
  ProjectionExpression: '#ex',
  ReturnConsumedCapacity: ReturnConsumedCapacity.NONE
});

export const buildTouchInput = (key: string, config: Config & { ttl: Milliseconds }) => {
  const expiresAt = msToS(Date.now() + config.ttl).toString();

  return {
    Key: deserializeKey(key, config),
    UpdateExpression: 'set #ex = :ex',
    ExpressionAttributeNames: { '#ex': config.keys.ex },
    ExpressionAttributeValues: { ':ex': { N: expiresAt } },
    TableName: config.table,
    ReturnValues: ReturnValue.NONE,
    ReturnConsumedCapacity: ReturnConsumedCapacity.NONE,
    ReturnItemCollectionMetrics: ReturnItemCollectionMetrics.NONE
  };
};
