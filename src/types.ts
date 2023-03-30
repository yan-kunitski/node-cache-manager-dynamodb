import { Config as CacheConfig } from 'cache-manager';
import { DynamoDBClientConfig, DynamoDBClient } from '@aws-sdk/client-dynamodb';

export interface Config extends CacheConfig {
  table: string;
  keys: { pk: string; sk?: string; ex: string };
  dynamodb: DynamoDBClientConfig | DynamoDBClient;
  meta?: (data: any) => Record<string, any>;
}
