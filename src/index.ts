import Redis from "ioredis"
import { XXH3_128 } from "xxh3-ts"

export interface BloomFilterOptions {
  redis: {
    url: string
  }
  name: string
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
  private redis: Redis
  private redisKey: string
  private redisConfigKey: string

  constructor(private options: BloomFilterOptions) {
    this.redis = new Redis(this.options.redis.url)
    this.redisKey = this.options.name
    this.redisConfigKey = `${this.options.name}:config`
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
    // Extract two 64-bit parts
    const high = h >> 64n
    const low = h & ((1n << 64n) - 1n)
    return [high, low]
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
      const index = (hash & 0x7fffffffffffffffn) % bigSize
      indexes[i] = index
      hash = i % 2 === 0 ? hash + hash2 : hash + hash1
    }
    return indexes
  }

  private getAllIndexes(items: T[]): bigint[] {
    const allIndexes: bigint[] = []
    for (const item of items) {
      const [hash1, hash2] = this.hash(item)
      const indexes = this.computeIndexes(
        hash1,
        hash2,
        this.hashIterations,
        this.size,
      )
      allIndexes.push(...indexes)
    }
    return allIndexes
  }

  private async readConfig(): Promise<void> {
    const config = await this.getConfigFields()
    this.size = config.size
    this.hashIterations = config.hashIterations
  }

  private async ensureConfigLoaded(): Promise<void> {
    if (this.size === 0) {
      await this.readConfig()
    }
  }

  private async getConfigFields(): Promise<BloomFilterConfig> {
    const config = await this.redis.hgetall(this.redisConfigKey)
    if (
      !config ||
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
   */
  async tryInit(
    expectedInsertions: number,
    falseProbability: number,
  ): Promise<boolean> {
    if (falseProbability > 1) {
      throw new Error("Bloom filter false probability can't be greater than 1")
    }
    if (falseProbability < 0) {
      throw new Error("Bloom filter false probability can't be negative")
    }

    const size = this.optimalNumOfBits(expectedInsertions, falseProbability)
    if (size === 0) {
      throw new Error(`Bloom filter calculated size is ${size}`)
    }
    const maxSize = Number.MAX_SAFE_INTEGER * 2
    if (size > maxSize) {
      throw new Error(
        `Bloom filter size can't be greater than ${maxSize}. But calculated size is ${size}`,
      )
    }

    const hashIterations = this.optimalNumOfHashFunctions(
      expectedInsertions,
      size,
    )

    const script = `
      if redis.call('exists', KEYS[1]) == 1 then
        return 0
      end
      redis.call('hset', KEYS[1], 'size', ARGV[1])
      redis.call('hset', KEYS[1], 'hashIterations', ARGV[2])
      redis.call('hset', KEYS[1], 'expectedInsertions', ARGV[3])
      redis.call('hset', KEYS[1], 'falseProbability', ARGV[4])
      return 1
    `

    const result = (await this.redis.eval(
      script,
      1, // number of keys
      this.redisConfigKey,
      size.toString(),
      hashIterations.toString(),
      expectedInsertions.toString(),
      falseProbability.toString(),
    )) as number

    if (result === 1) {
      this.size = size
      this.hashIterations = hashIterations
      return true
    }

    // Already exists, load config
    await this.readConfig()
    return false
  }

  async add(item: T): Promise<boolean> {
    return (await this.addAll([item])) > 0
  }

  async addAll(items: T[]): Promise<number> {
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
      // SETBIT returns the old bit value, so if it's 0, it means we changed from 0 to 1
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
    return (await this.containsAll([item])) > 0
  }

  async containsAll(items: T[]): Promise<number> {
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
   * Approximated count of items.
   */
  async count(): Promise<number> {
    await this.ensureConfigLoaded()
    // BITCOUNT gives how many bits are set.
    const bitCount = await this.redis.bitcount(this.redisKey)
    const c = Math.round(
      (-this.size / this.hashIterations) * Math.log(1 - bitCount / this.size),
    )
    return c
  }

  async delete(): Promise<boolean> {
    const result = await this.redis.del(this.redisKey, this.redisConfigKey)
    this.size = 0 // reset size
    return result > 0
  }

  async isExists(): Promise<boolean> {
    const result = await this.redis.exists(this.redisConfigKey)
    return result === 1
  }

  async getSize(): Promise<number> {
    const config = await this.getConfigFields()
    return config.size
  }

  async getHashIterations(): Promise<number> {
    const config = await this.getConfigFields()
    return config.hashIterations
  }

  async getExpectedInsertions(): Promise<number> {
    const config = await this.getConfigFields()
    return config.expectedInsertions
  }

  async getFalseProbability(): Promise<number> {
    const config = await this.getConfigFields()
    return config.falseProbability
  }
}
