import { describe, it, expect, beforeEach, afterEach } from "vitest"
import { BloomFilter } from "../src"
import Redis from "ioredis"

function produceRandomBigIntString(size: number): string[] {
  const items = new Set<string>()
  while (items.size < size) {
    const randomBigInt = BigInt(
      Math.floor(Math.random() * Number.MAX_SAFE_INTEGER),
    )
    items.add(randomBigInt.toString())
  }
  return Array.from(items)
}

describe("BloomFilter", () => {
  let filter: BloomFilter<string>

  beforeEach(async () => {
    filter = new BloomFilter({
      redis: {
        url: "redis://localhost:6379",
      },
      name: "test-bloom-filter",
    })
    expect(await filter.tryInit(1000, 0.01)).toBe(true)
  })

  afterEach(async () => {
    await filter.delete()
  })

  it("should add and check single items", async () => {
    const item = "test-item"

    const added = await filter.add(item)
    expect(added).toBe(true)

    const contains = await filter.contains(item)
    expect(contains).toBe(true)

    const notContains = await filter.contains("non-existent")
    expect(notContains).toBe(false)
  })

  it("should add and check multiple items", async () => {
    const items = ["item1", "item2", "item3"]

    const addedCount = await filter.addAll(items)
    expect(addedCount).toBe(3)

    const containsCount = await filter.containsAll(items)
    expect(containsCount).toBe(3)

    const notContainsCount = await filter.containsAll(["non1", "non2"])
    expect(notContainsCount).toBe(0)
  })

  it("should return approximate count", async () => {
    const items = ["item1", "item2", "item3"]
    await filter.addAll(items)

    const count = await filter.count()
    // Count may not be exact due to bloom filter properties
    expect(count).toBeGreaterThanOrEqual(1)
    expect(count).toBeLessThanOrEqual(5)
  })

  it("should handle initialization parameters", async () => {
    const size = await filter.getSize()
    expect(size).toBeGreaterThan(0)

    const hashIterations = await filter.getHashIterations()
    expect(hashIterations).toBeGreaterThan(0)

    const expectedInsertions = await filter.getExpectedInsertions()
    expect(expectedInsertions).toBe(1000)

    const falseProbability = await filter.getFalseProbability()
    expect(falseProbability).toBe(0.01)
  })

  it("should check existence and deletion", async () => {
    const exists = await filter.exists()
    expect(exists).toBe(true)

    const deleted = await filter.delete()
    expect(deleted).toBe(true)

    const existsAfterDelete = await filter.exists()
    expect(existsAfterDelete).toBe(false)
  })

  it("should throw error when not initialized", async () => {
    await filter.delete()
    await expect(filter.add("test")).rejects.toThrow(
      "Bloom filter is not initialized",
    )
  })

  it("should handle multiple add operations correctly", async () => {
    const list = ["1", "2", "3"]
    const addedCount = await filter.addAll(list)
    expect(addedCount).toBe(3)

    const addedAgainCount = await filter.addAll(list)
    expect(addedAgainCount).toBe(0)

    const count = await filter.count()
    expect(count).toBeGreaterThanOrEqual(3)
    expect(count).toBeLessThanOrEqual(5)

    const addedNewCount = await filter.addAll(["1", "5"])
    expect(addedNewCount).toBe(1)

    const newCount = await filter.count()
    expect(newCount).toBeGreaterThanOrEqual(4)
    expect(newCount).toBeLessThanOrEqual(6)

    for (const item of list) {
      expect(await filter.contains(item)).toBe(true)
    }
  })

  it("should throw error for invalid false probability values", async () => {
    await filter.delete()
    await expect(filter.tryInit(1, -1)).rejects.toThrow(
      "Bloom filter false probability must be between 0 and 1",
    )
    await expect(filter.tryInit(1, 2)).rejects.toThrow(
      "Bloom filter false probability must be between 0 and 1",
    )
  })

  it("should have correct config values", async () => {
    await filter.delete()
    await filter.tryInit(100, 0.03)

    expect(await filter.getExpectedInsertions()).toBe(100)
    expect(await filter.getFalseProbability()).toBe(0.03)
    expect(filter.getHashIterations()).toBe(5)
    expect(filter.getSize()).toBe(729)
  })

  it("should have correct config values", async () => {
    await filter.delete()
    await filter.tryInit(10_000_000, 0.03)

    expect(await filter.getExpectedInsertions()).toBe(10000000)
    expect(await filter.getFalseProbability()).toBe(0.03)
    expect(filter.getHashIterations()).toBe(5)
    expect(filter.getSize()).toBe(72984408)
  })

  it("should handle initialization correctly", async () => {
    await filter.delete()
    expect(await filter.tryInit(55000000, 0.03)).toBe(true)
    expect(await filter.tryInit(55000001, 0.03)).toBe(false)

    await filter.delete()
    expect(await filter.exists()).toBe(false)
    expect(await filter.tryInit(55000001, 0.03)).toBe(true)
  })
})

describe("BloomFilter scale test", () => {
  let filter: BloomFilter<string>

  afterEach(async () => {
    if (filter) {
      await filter.delete()
    }
  })

  async function testLargeScaleOperations(size: number, fpSampleSize = 10000) {
    const items = produceRandomBigIntString(size)
    filter = new BloomFilter({
      redis: {
        url: "redis://localhost:6379",
      },
      name: `test-bd-${size}`,
    })
    expect(await filter.tryInit(size, 0.01)).toBe(true)

    const delta = 0.05

    const addedCount = await filter.addAll(items)
    expect(addedCount).toBeGreaterThanOrEqual(size - size * delta)
    expect(addedCount).toBeLessThanOrEqual(size + size * delta)

    const containsCount = await filter.containsAll(items)
    expect(containsCount).toBe(size)

    const itemsSet = new Set(items)
    const nonExistentItems = produceRandomBigIntString(fpSampleSize).filter(
      (item) => !itemsSet.has(item),
    )
    const falsePositives = await filter.containsAll(nonExistentItems)
    const falsePositiveRate = falsePositives / nonExistentItems.length
    expect(falsePositiveRate).toBeLessThanOrEqual(delta)

    const count = await filter.count()
    expect(count).toBeGreaterThanOrEqual(size - size * delta)
    expect(count).toBeLessThanOrEqual(size + size * delta)
  }

  for (const size of [100, 300, 1000, 3000, 10000, 30000]) {
    it(`should handle large scale operations with ${size} strings`, async () => {
      await testLargeScaleOperations(size)
    })
  }
})

describe("BloomFilter TTL", () => {
  let filter: BloomFilter<string>

  afterEach(async () => {
    if (filter) {
      await filter.delete()
    }
  })

  it("should not expire when TTL is 0", async () => {
    const size = 1000
    const items = produceRandomBigIntString(size)
    filter = new BloomFilter({
      redis: {
        url: "redis://localhost:6379",
      },
      name: "test-ttl-0",
      ttlSeconds: 0, // No TTL
    })
    expect(await filter.tryInit(size, 0.01)).toBe(true)

    const addedCount = await filter.addAll(items)
    expect(addedCount).toBeGreaterThanOrEqual(size - size * 0.05)
    expect(addedCount).toBeLessThanOrEqual(size + size * 0.05)

    // Check TTL directly from Redis
    const redisClient = new Redis("redis://localhost:6379")
    const ttl = await redisClient.ttl("test-ttl-0")
    expect(ttl).toBe(-1) // -1 indicates no TTL set
  })

  it("should expire after TTL", async () => {
    const size = 1000
    const items = produceRandomBigIntString(size)
    filter = new BloomFilter({
      redis: {
        url: "redis://localhost:6379",
      },
      name: "test-ttl-1",
      ttlSeconds: 1, // 1s TTL
    })
    expect(await filter.tryInit(size, 0.01)).toBe(true)

    const addedCount = await filter.addAll(items)
    expect(addedCount).toBeGreaterThanOrEqual(size - size * 0.05)
    expect(addedCount).toBeLessThanOrEqual(size + size * 0.05)

    // Check TTL directly from Redis
    const redisClient = new Redis("redis://localhost:6379")
    const ttl = await redisClient.ttl("test-ttl-1")
    expect(ttl).toBeGreaterThanOrEqual(0)
    expect(ttl).toBeLessThanOrEqual(1)

    // Wait for TTL to expire
    await new Promise((resolve) => setTimeout(resolve, 1100))

    const count = await filter.count()
    expect(count).toBeFalsy()

    const containsCount = await filter.containsAll(items)
    expect(containsCount).toBeFalsy()
  })
})
