import { Milliseconds } from 'cache-manager';
import { AttributeValue } from '@aws-sdk/client-dynamodb';

import { Config } from '../types';

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

export const eraseMask = (pattern: string) => pattern.replace(MASK, '');
