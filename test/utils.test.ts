import { describe, expect, it } from 'vitest';

import { backoff, UnprocessedDataException } from '../src/utils/backoff';

describe('Store utils', function () {
  it('should backoff request', async () => {
    let data: number[] = [];
    let error: unknown;
    let request = async () => {
      data.push(1);
      throw new UnprocessedDataException();
    };

    try {
      await backoff(request, { numOfAttempts: 3 });
    } catch (err) {
      error = err;
    }

    expect(error).toBeDefined();
    expect(data.length).toBe(3);

    data = [];
    error = undefined;
    // @ts-ignore
    request = async () => {
      data.push(1);

      if (data.length !== 3) throw new UnprocessedDataException();
    };

    try {
      await backoff(request, { numOfAttempts: 3 });
    } catch (err) {
      error = err;
    }

    expect(error).toBeUndefined();
    expect(data.length).toBe(3);
  });

  it('should not backoff request', async () => {
    let data: number[] = [];
    let error: unknown;
    let request = async () => {
      data.push(1);
    };

    try {
      await backoff(request, { numOfAttempts: 3 });
    } catch (err) {
      error = err;
    }

    expect(error).toBeUndefined();
    expect(data.length).toBe(1);

    data = [];
    error = undefined;
    request = async () => {
      data.push(1);
      throw new Error('Not an UnprocessedDataException');
    };

    try {
      await backoff(request, { numOfAttempts: 3 });
    } catch (err) {
      error = err;
    }

    expect(error instanceof UnprocessedDataException).toBeFalsy();
    expect(data.length).toBe(1);
  });
});
