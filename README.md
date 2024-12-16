# redis-bloom-filter

[![CI](https://github.com/jimexist/redis-bloom-filter/actions/workflows/ci.yaml/badge.svg)](https://github.com/jimexist/redis-bloom-filter/actions/workflows/ci.yaml)
[![npm version](https://badge.fury.io/js/redis-bloom-filter.svg)](https://badge.fury.io/js/redis-bloom-filter)

Port of [Redisson's bloom filter][1] to JavaScript, with some improvements (using [xxh3-ts][2] instead of [highway hash 128][3]).

## Usage

```bash
npm install redis-bloom-filter
```

```ts
import { BloomFilter } from 'redis-bloom-filter';

const bloomFilter = new BloomFilter({
  redis: {
    url: 'redis://localhost:6379',
  },
  name: 'my-bloom-filter',
  ttlSeconds: 60, // optional, defaults to 0 (no TTL)
});
```

[1]: https://github.com/redisson/redisson/blob/master/redisson/src/main/java/org/redisson/RedissonBloomFilter.java
[2]: https://www.npmjs.com/package/xxh3-ts
[3]: https://github.com/google/highwayhash
