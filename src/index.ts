import Redis from "ioredis"
import { XXH3_128 } from "xxh3-ts"

export interface BloomFilterOptions {
  redis:
    | {
        url: string
      }
    | Redis
  name: string
  ttlSeconds?: number
}

export interface BloomFilterConfig {
  size: number
  hashIterations: number
  expectedInsertions: number
  falseProbability: number
}

export class BloomFilter<T> {
  private size = 0
  private hashIterations = 0
  private readonly redis: Redis
  private readonly redisKey: string
  private readonly redisConfigKey: string
  private readonly ttlSeconds: number | undefined

  constructor(private readonly options: BloomFilterOptions) {
    this.redis =
      "url" in this.options.redis
        ? new Redis(this.options.redis.url)
        : this.options.redis
    this.redisKey = this.options.name
    this.redisConfigKey = `${this.options.name}:config`
    this.ttlSeconds = this.options.ttlSeconds
  }

  private optimalNumOfHashFunctions(n: number, m: number): number {
    return Math.max(1, Math.round((m / n) * Math.log(2)))
  }

  private optimalNumOfBits(n: number, p: number): number {
    const minProb = p === 0 ? Number.MIN_VALUE : p
    return Math.floor((-n * Math.log(minProb)) / Math.log(2) ** 2)
  }

  private hash(item: T): [bigint, bigint] {
    const data = Buffer.from(JSON.stringify(item))
    const h = XXH3_128(data)
    return [h >> 64n, h & ((1n << 64n) - 1n)]
  }

  private computeIndexes(
    hash1: bigint,
    hash2: bigint,
    iterations: number,
    size: number,
  ): bigint[] {
    const bigSize = BigInt(size)
    const indexes = new Array<bigint>(iterations)
    let hash = hash1
    for (let i = 0; i < iterations; i++) {
      indexes[i] = (hash & 0x7fffffffffffffffn) % bigSize
      hash = i % 2 === 0 ? hash + hash2 : hash + hash1
    }
    return indexes
  }

  private getAllIndexes(items: T[]): bigint[] {
    return items.flatMap((item) => {
      const [hash1, hash2] = this.hash(item)
      return this.computeIndexes(hash1, hash2, this.hashIterations, this.size)
    })
  }

  private async readConfig(): Promise<void> {
    const { size, hashIterations } = await this.getConfigFields()
    this.size = size
    this.hashIterations = hashIterations
  }

  private async ensureConfigLoaded(): Promise<void> {
    if (this.size === 0) {
      await this.readConfig()
    }
  }

  private async getConfigFields(): Promise<BloomFilterConfig> {
    const config = await this.redis.hgetall(this.redisConfigKey)
    if (
      !config.size ||
      !config.hashIterations ||
      !config.expectedInsertions ||
      !config.falseProbability
    ) {
      throw new Error("Bloom filter is not initialized")
    }
    return {
      size: Number.parseInt(config.size, 10),
      hashIterations: Number.parseInt(config.hashIterations, 10),
      expectedInsertions: Number.parseInt(config.expectedInsertions, 10),
      falseProbability: Number.parseFloat(config.falseProbability),
    }
  }

  /**
   * Initialize bloom filter if not already initialized.
   * @throws {Error} If parameters are invalid or initialization fails
   */
  async tryInit(
    expectedInsertions: number,
    falseProbability: number,
  ): Promise<boolean> {
    if (falseProbability <= 0 || falseProbability > 1) {
      throw new Error("Bloom filter false probability must be between 0 and 1")
    }

    const size = this.optimalNumOfBits(expectedInsertions, falseProbability)
    const maxSize = Number.MAX_SAFE_INTEGER * 2
    if (size === 0 || size > maxSize) {
      throw new Error(
        `Invalid bloom filter size: ${size}. Must be between 1 and ${maxSize}`,
      )
    }

    const hashIterations = this.optimalNumOfHashFunctions(
      expectedInsertions,
      size,
    )

    const script = `
      local exists = redis.call('exists', KEYS[1])
      if exists == 0 then
        redis.call('hmset', KEYS[1],
          'size', ARGV[1],
          'hashIterations', ARGV[2],
          'expectedInsertions', ARGV[3],
          'falseProbability', ARGV[4]
        )
        -- set to 0 to ensure that key exists
        redis.call('setbit', KEYS[2], 0, 0)
        local ttl = tonumber(ARGV[5])
        if ttl ~= 0 then
          redis.call('expire', KEYS[1], ttl)
          redis.call('expire', KEYS[2], ttl)
        end
      end
      return exists
    `

    const pipeline = this.redis.pipeline()
    pipeline.eval(
      script,
      2,
      this.redisConfigKey,
      this.redisKey,
      size.toString(),
      hashIterations.toString(),
      expectedInsertions.toString(),
      falseProbability.toString(),
      (this.ttlSeconds || 0).toString(),
    )

    const results = await pipeline.exec()
    if (!results) {
      throw new Error("Failed to execute pipeline")
    }

    const [existsErr, exists] = results[0]
    if (existsErr) throw existsErr

    const result = exists === 0 ? 1 : 0 // Return 1 if config didn't exist

    if (result === 1) {
      this.size = size
      this.hashIterations = hashIterations
      return true
    }

    await this.readConfig()
    return false
  }

  async add(item: T): Promise<boolean> {
    return (await this.addAll([item])) === 1
  }

  async addAll(items: T[]): Promise<number> {
    if (items.length === 0) return 0

    await this.ensureConfigLoaded()
    const allIndexes = this.getAllIndexes(items)
    const pipeline = this.redis.multi()
    const indexesPerItem = this.hashIterations

    for (let i = 0; i < items.length; i++) {
      const start = i * indexesPerItem
      const end = start + indexesPerItem
      const indexes = allIndexes.slice(start, end)
      for (const idx of indexes) {
        pipeline.setbit(this.redisKey, Number(idx), 1)
      }
    }

    const results = await pipeline.exec()
    if (!results) {
      throw new Error("Failed to execute pipeline")
    }

    let addedCount = 0
    let newBitSetForCurrentItem = false
    for (let rIndex = 0; rIndex < results.length; rIndex++) {
      const [err, bitVal] = results[rIndex]
      if (err) throw err
      if (bitVal === 0) {
        newBitSetForCurrentItem = true
      }
      if ((rIndex + 1) % indexesPerItem === 0) {
        if (newBitSetForCurrentItem) {
          addedCount++
        }
        newBitSetForCurrentItem = false
      }
    }

    return addedCount
  }

  async contains(item: T): Promise<boolean> {
    return (await this.containsAll([item])) === 1
  }

  async containsAll(items: T[]): Promise<number> {
    if (items.length === 0) return 0

    await this.ensureConfigLoaded()
    const allIndexes = this.getAllIndexes(items)
    const pipeline = this.redis.multi()

    for (const idx of allIndexes) {
      pipeline.getbit(this.redisKey, Number(idx))
    }

    const results = await pipeline.exec()
    if (!results) {
      throw new Error("Failed to execute pipeline")
    }

    let presentCount = 0
    let allSetForCurrentItem = true
    const indexesPerItem = this.hashIterations

    for (let i = 0; i < results.length; i++) {
      const [err, bitVal] = results[i]
      if (err) throw err
      if (bitVal === 0) {
        allSetForCurrentItem = false
      }

      if ((i + 1) % indexesPerItem === 0) {
        if (allSetForCurrentItem) {
          presentCount++
        }
        allSetForCurrentItem = true
      }
    }

    return presentCount
  }

  /**
   * Get approximated count of items in the filter.
   * @returns Estimated number of items in the filter
   */
  async count(): Promise<number> {
    await this.ensureConfigLoaded()
    const bitCount = await this.redis.bitcount(this.redisKey)
    return Math.round(
      (-this.size / this.hashIterations) * Math.log(1 - bitCount / this.size),
    )
  }

  /**
   * Delete the bloom filter and its configuration.
   * @returns true if deleted successfully, false if filter didn't exist
   */
  async delete(): Promise<boolean> {
    const result = await this.redis.del(this.redisKey, this.redisConfigKey)
    this.size = 0
    this.hashIterations = 0
    return result > 0
  }

  /**
   * Check if the bloom filter exists.
   */
  async exists(): Promise<boolean> {
    return (await this.redis.exists(this.redisConfigKey)) === 1
  }

  async getExpectedInsertions(): Promise<number> {
    const config = await this.getConfigFields()
    return config.expectedInsertions
  }

  async getFalseProbability(): Promise<number> {
    const config = await this.getConfigFields()
    return config.falseProbability
  }

  getSize(): number {
    return this.size
  }

  getHashIterations(): number {
    return this.hashIterations
  }
}
