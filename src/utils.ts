import { Milliseconds } from 'cache-manager';
import {
  ReturnValue,
  ReturnConsumedCapacity,
  ReturnItemCollectionMetrics,
  AttributeValue
} from '@aws-sdk/client-dynamodb';

import { Config } from './types';

const DELIMITER = '+';
const MASK = '*';

export const msToS = (ms: Milliseconds) => Math.round(ms / 1000);

export const serializeKey = (item: Record<string, AttributeValue>, config: Config) => {
  const pk = item[config.keys.pk].S;
  const sk = item[config.keys.sk || ''].S;

  return (sk ? `${pk}${DELIMITER}${sk}` : pk) as string;
};

export const deserializeKey = (key: string, config: Config) => {
  if (typeof key !== 'string' || !key.length || key.startsWith(DELIMITER)) {
    throw new Error('Unsupported cache key');
  }

  const [pk, sk] = key.split(DELIMITER);

  const dbKey = { [config.keys.pk]: { S: pk } };

  if (config.keys.sk) {
    Object.assign(dbKey, { [config.keys.sk]: { S: sk } });
  }

  return dbKey;
};

export const validateKeyPattern = (pattern?: string) => {
  if (!pattern) return;

  const isValid =
    typeof pattern === 'string' &&
    pattern.endsWith(MASK) &&
    pattern.split(MASK).length < 3 &&
    pattern.split(DELIMITER).length === 2;

  if (isValid) return;

  throw new Error('Bad key pattern provided. Possible value "bar+fo*"');
};

export const isExpired = (record: Record<string, AttributeValue>, config: Config) => {
  const expiresAt = parseInt(record[config.keys.ex].N as string, 10);

  return expiresAt * 1000 < Date.now();
};

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

export const buildMGetInput = (keys: string[], config: Config) => ({
  RequestItems: {
    [config.table]: { Keys: keys.map((key) => deserializeKey(key, config)) }
  }
});

export const buildMSetInput = (
  args: [string, Record<string, AttributeValue>][],
  config: Config & { ttl: Milliseconds }
) => {
  const items = args.map(([key, data]) => {
    const expiresAt = msToS(Date.now() + config.ttl).toString();
    const Item = { ...data, ...deserializeKey(key, config), [config.keys.ex]: { N: expiresAt } };

    return { PutRequest: { Item } };
  });

  return {
    RequestItems: { [config.table]: items },
    ReturnValue: ReturnValue.NONE,
    ReturnConsumedCapacity: ReturnConsumedCapacity.NONE,
    ReturnItemCollectionMetrics: ReturnItemCollectionMetrics.NONE
  };
};

export const buildMDelInput = (keys: string[], config: Config) => ({
  RequestItems: {
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
  const key = deserializeKey(pattern.replace(MASK, ''), config);
  const pk = key[config.keys.pk];
  const sk = key[config.keys.sk];
  const input = {
    TableName: config.table,
    ProjectionExpression: config.keys.sk ? '#pk, #sk' : '#pk',
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
