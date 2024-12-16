# redis-bloom-filter

Port of Redisson's bloom filter to JavaScript, with some improvements (using xxh3-ts instead of highway hash 128).

## Usage

```ts
import { BloomFilter } from 'redis-bloom-filter';

const bloomFilter = new BloomFilter({
  redis: {
    url: 'redis://localhost:6379',
  },
});
```
