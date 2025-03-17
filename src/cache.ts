import { createClient, RedisClientType } from "npm:redis@4.6.7";
import { logger } from "./logger.ts";

// Redis client instance
let redisClient: RedisClientType | null = null;

// Redis connection configuration
interface RedisConfig {
  host: string;
  port: number;
  password?: string;
  database?: number;
  tls?: boolean;
}

// Default Redis configuration
const defaultRedisConfig: RedisConfig = {
  host: "127.0.0.1",
  port: 6379,
  database: 0,
  tls: false,
};

/**
 * Initialize the Redis connection
 * @param config Optional Redis configuration
 * @returns Redis client instance
 */
export async function initializeRedisCache(
  config: Partial<RedisConfig> = {},
): Promise<RedisClientType> {
  if (redisClient) {
    return redisClient;
  }

  const redisConfig = { ...defaultRedisConfig, ...config };

  try {
    logger.info(
      `Connecting to Redis at ${redisConfig.host}:${redisConfig.port}`,
    );

    // Create Redis client with npm:redis
    redisClient = createClient({
      socket: {
        host: redisConfig.host,
        port: redisConfig.port,
        tls: redisConfig.tls,
      },
      password: redisConfig.password,
      database: redisConfig.database,
    });

    // Set up error handler
    redisClient.on("error", (err) => {
      logger.error(`Redis client error: ${err.message}`);
    });

    // Connect to Redis
    await redisClient.connect();

    // Test the connection
    const pingResponse = await redisClient.ping();
    logger.info(`Successfully connected to Redis: ${pingResponse}`);

    return redisClient;
  } catch (error) {
    logger.error(`Failed to connect to Redis: ${error.message}`);
    throw error;
  }
}

/**
 * Get the Redis client instance
 * @returns Redis client instance
 */
export function getRedisCache(): RedisClientType {
  if (!redisClient) {
    throw new Error(
      "Redis client not initialized. Call initializeRedisCache first.",
    );
  }
  return redisClient;
}

/**
 * Close the Redis connection
 */
export async function closeRedisCache(): Promise<void> {
  if (redisClient) {
    try {
      await redisClient.quit();
      redisClient = null;
      logger.info("Redis connection closed");
    } catch (error) {
      logger.error(`Error closing Redis connection: ${error.message}`);
      throw error;
    }
  }
}
