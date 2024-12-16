import { afterAll, beforeAll, bench, describe } from "vitest"
import { BloomFilter } from "../src"

describe("RedisBloomFilter Benchmark", () => {
  const filter = new BloomFilter<string>({
    redis: {
      url: "redis://localhost:6379",
    },
    name: "benchmark-test",
  })

  const items: string[] = []
  for (let i = 0; i < 100000; i++) {
    items.push(Math.random().toString(36).substring(2))
  }

  beforeAll(async () => {
    await filter.tryInit(100000, 0.01)
    await filter.addAll(items)
  })

  afterAll(async () => {
    await filter.delete()
  })

  bench("check existence of 100000 strings", async () => {
    await filter.containsAll(items)
  })
})
