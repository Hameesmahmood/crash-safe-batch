import { defineConfig } from "vitest/config";

export default defineConfig({
  test: {
    // One Postgres container is shared by every test file, so files run one at a time.
    fileParallelism: false,
    globalSetup: ["test/global-setup.ts"],
    testTimeout: 30_000,
    hookTimeout: 120_000,
  },
});
