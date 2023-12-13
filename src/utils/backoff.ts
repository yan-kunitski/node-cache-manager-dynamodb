import { backOff, BackoffOptions } from 'exponential-backoff';

export class UnprocessedDataException extends Error {}

export const backoff = <T>(request: () => Promise<T>, options?: BackoffOptions): Promise<T> =>
  backOff(request, {
    retry: (err: Error) => {
      console.log('Backing off request. Error:', err);

      return err instanceof UnprocessedDataException;
    },
    ...options
  });
