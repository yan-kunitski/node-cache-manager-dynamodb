import { Store, Milliseconds, FactoryConfig } from 'cache-manager';
import chunk from 'lodash.chunk';
import { marshall, unmarshall } from '@aws-sdk/util-dynamodb';
import {
  AttributeValue,
  DynamoDBClient,
  GetItemCommand,
  PutItemCommand,
  DeleteItemCommand,
  BatchGetItemCommand,
  BatchWriteItemCommand,
  UpdateItemCommand,
  ScanCommand,
  QueryCommand
} from '@aws-sdk/client-dynamodb';

import { Config } from './types';
import {
  serializeKey,
  validateKeyPattern,
  buildGetInput,
  buildSetInput,
  buildDelInput,
  buildMGetInput,
  buildMSetInput,
  buildMDelInput,
  buildScanKeysInput,
  buildQueryKeysInput,
  buildTTLInput,
  buildTouchInput,
  isExpired
} from './utils';

class DynamoDBStore implements Store {
  private client: DynamoDBClient;
  private static defaultTtl = 60000;

  constructor(private readonly config: Config) {
    this.client =
      this.config.dynamodb instanceof DynamoDBClient
        ? this.config.dynamodb
        : new DynamoDBClient(this.config.dynamodb);
  }

  async get<T>(key: string) {
    const input = buildGetInput(key, this.config);
    const response = await this.client.send(new GetItemCommand(input));

    if (!response.Item) return;

    const item = unmarshall(response.Item);

    return isExpired(item, this.config) ? undefined : (item.data as T);
  }

  async set<T>(key: string, data: T, ttl?: Milliseconds) {
    const meta = this.config.meta && this.config.meta(data);
    const input = buildSetInput(key, marshall({ data, ...meta }), {
      ...this.config,
      ttl: ttl || this.config.ttl || DynamoDBStore.defaultTtl
    });
    await this.client.send(new PutItemCommand(input));
  }

  async del(key: string) {
    const input = buildDelInput(key, this.config);
    await this.client.send(new DeleteItemCommand(input));
  }

  async mget(...args: string[]) {
    // chunk is necessary due to DynamoDB API limitations
    // batch get https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchGetItem.html
    // batch write https://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
    const chunks = chunk(args, 100);
    const items = await Promise.all(
      chunks.map(async (keys) => {
        const input = buildMGetInput(keys, this.config);
        const response = await this.client.send(new BatchGetItemCommand(input));
        const items = response.Responses?.[this.config.table] || [];
        console.log('Found unprocessed keys', JSON.stringify(response.UnprocessedKeys || {}));
        const map = items.reduce((acc, item) => {
          const key = serializeKey(item, this.config);
          acc[key] = item;

          return acc;
        }, {} as Record<string, any>);

        return keys.map((key) => {
          if (!map[key]) return;

          const item = unmarshall(map[key]);

          return isExpired(item, this.config) ? undefined : item.data;
        });
      })
    );

    return items.flat();
  }

  async mset(args: [string, unknown][], ttl?: Milliseconds) {
    const chunks = chunk(args, 25);
    await Promise.all(
      chunks.map(async (values) => {
        const input = buildMSetInput(
          values.map(([key, data]) => {
            const meta = this.config.meta && this.config.meta(data);

            return [key, marshall({ data, ...meta })];
          }),
          {
            ...this.config,
            ttl: ttl || this.config.ttl || DynamoDBStore.defaultTtl
          }
        );

        const responses = await this.client.send(new BatchWriteItemCommand(input));
        console.log('Found unprocessed items', JSON.stringify(responses.UnprocessedItems || {}));

        return responses;
      })
    );
  }

  async mdel(...args: string[]) {
    const chunks = chunk(args, 25);
    await Promise.all(
      chunks.map((keys) => {
        const input = buildMDelInput(keys, this.config);

        return this.client.send(new BatchWriteItemCommand(input));
      })
    );
  }

  async keys(pattern?: string) {
    // DynamoDB supports only "begin_with" to retrieve items with similar !!secondary!! keys
    // So it's impossible to find keys by pattern "*foo_bar*", but it's possible to use "baz+foo_ba*"
    // https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Query.html

    validateKeyPattern(pattern);
    let cursor: Record<string, AttributeValue> | undefined = undefined;
    const keys: string[] = [];

    do {
      let command: QueryCommand | ScanCommand;

      if (pattern) {
        const input = buildQueryKeysInput(
          pattern,
          cursor,
          this.config as Config & { keys: Required<Config['keys']> }
        );
        command = new QueryCommand(input);
      } else {
        const input = buildScanKeysInput(cursor, this.config);
        // @ts-ignore
        command = new ScanCommand(input);
      }

      const response = await this.client.send(command);
      const serialized = (response.Items || []).map((item) => serializeKey(item, this.config));
      keys.push(...serialized);
      cursor = response.LastEvaluatedKey;
    } while (!!cursor);

    return keys;
  }

  async reset() {
    const keys = await this.keys();
    await this.mdel(...keys);
  }

  async ttl(key: string): Promise<Milliseconds> {
    const input = buildTTLInput(key, this.config);
    const response = await this.client.send(new GetItemCommand(input));
    const expiresAt = response.Item?.[this.config.keys.ex].N;

    return expiresAt ? parseInt(expiresAt, 10) * 1000 - Date.now() : -1;
  }

  async touch(key: string, ttl?: Milliseconds) {
    const input = buildTouchInput(key, {
      ...this.config,
      ttl: ttl || this.config.ttl || DynamoDBStore.defaultTtl
    });
    await this.client.send(new UpdateItemCommand(input));
  }
}

const create = (config: FactoryConfig<Config>) => new DynamoDBStore(config);

export { DynamoDBStore, create };
