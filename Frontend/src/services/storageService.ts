// Storage service for managing responses in localStorage
// Handles persistence, quota management, and error handling

export interface StoredResponse {
  response: {
    success: boolean;
    data: {
      rawData?: unknown[];
      message?: string;
      insights?: string;
      user_input?: string;
      chat_response?: string;
      sql?: string;
      reason?: string;
      tab_id?: string;
      pyspark?: string;
    };
    error?: string;
    reason?: string;
    catalog_options?: string[];
    conflicts?: string[];
  };
  rawData: unknown[];
  chartTypes: { [key: number]: string };
  resultId: string;
  timestamp: number;
  tabId?: string;
  tab_id?: string;
}
export interface MetricBlock {
  rawData?: unknown[];
  message?: string;
  insights?: string;
  user_input?: string;
  chat_response?: string;
  sql?: string;
  pyspark?: string;
  reason?: string;
}
interface ResponseIndex {
  id: string;
  timestamp: number;
  size: number; // Approximate size in bytes
}

const STORAGE_KEY_PREFIX = "iaas_response_";
const INDEX_KEY = "iaas_response_index";
const MAX_RESPONSES = 100; // Maximum number of responses to store

class StorageService {
  private getStorageKey(id: string): string {
    return `${STORAGE_KEY_PREFIX}${id}`;
  }

  private getIndex(): ResponseIndex[] {
    try {
      const indexStr = localStorage.getItem(INDEX_KEY);
      if (!indexStr) return [];
      return JSON.parse(indexStr) as ResponseIndex[];
    } catch (error) {
      console.error("Error reading response index:", error);
      return [];
    }
  }

  private saveIndex(index: ResponseIndex[]): void {
    try {
      localStorage.setItem(INDEX_KEY, JSON.stringify(index));
    } catch (error) {
      console.error("Error saving response index:", error);
    }
  }

  private estimateSize(data: unknown): number {
    try {
      return JSON.stringify(data).length;
    } catch {
      return 0;
    }
  }

  private cleanupOldResponses(): void {
    const index = this.getIndex();
    if (index.length <= MAX_RESPONSES) return;

    // Sort by timestamp (oldest first)
    index.sort((a, b) => a.timestamp - b.timestamp);

    // Remove oldest responses until under limit
    const toRemove = index.slice(0, index.length - MAX_RESPONSES);
    toRemove.forEach((item) => {
      try {
        localStorage.removeItem(this.getStorageKey(item.id));
      } catch (error) {
        console.error(`Error removing response ${item.id}:`, error);
      }
    });

    // Update index
    const remaining = index.slice(index.length - MAX_RESPONSES);
    this.saveIndex(remaining);
  }

  async saveResponse(
    response: Omit<StoredResponse, "timestamp">
  ): Promise<void> {
    try {
      const storedResponse: StoredResponse = {
        ...response,
        timestamp: Date.now(),
      };

      const size = this.estimateSize(storedResponse);
      const index = this.getIndex();

      // Check if response already exists
      const existingIndex = index.findIndex(
        (item) => item.id === response.resultId
      );
      if (existingIndex >= 0) {
        // Update existing
        index[existingIndex] = {
          id: response.resultId,
          timestamp: storedResponse.timestamp,
          size,
        };
      } else {
        // Add new
        index.push({
          id: response.resultId,
          timestamp: storedResponse.timestamp,
          size,
        });
      }

      // Cleanup old responses if needed
      this.cleanupOldResponses();

      // Save the response
      const key = this.getStorageKey(response.resultId);
      localStorage.setItem(key, JSON.stringify(storedResponse));

      // Update index
      const updatedIndex = this.getIndex();
      const itemIndex = updatedIndex.findIndex(
        (item) => item.id === response.resultId
      );
      if (itemIndex >= 0) {
        updatedIndex[itemIndex] = {
          id: response.resultId,
          timestamp: storedResponse.timestamp,
          size,
        };
        this.saveIndex(updatedIndex);
      }
    } catch (error) {
      if (
        error instanceof DOMException &&
        error.name === "QuotaExceededError"
      ) {
        // Try to free up space by removing oldest responses
        this.cleanupOldResponses();
        // Retry once
        try {
          const storedResponse: StoredResponse = {
            ...response,
            timestamp: Date.now(),
          };
          const key = this.getStorageKey(response.resultId);
          localStorage.setItem(key, JSON.stringify(storedResponse));
        } catch (retryError) {
          console.error("Failed to save response after cleanup:", retryError);
          throw new Error(
            "Storage quota exceeded. Please clear some responses."
          );
        }
      } else {
        console.error("Error saving response:", error);
        throw error;
      }
    }
  }

  async loadResponse(id: string): Promise<StoredResponse | null> {
    try {
      const key = this.getStorageKey(id);
      const data = localStorage.getItem(key);
      if (!data) return null;
      return JSON.parse(data) as StoredResponse;
    } catch (error) {
      console.error(`Error loading response ${id}:`, error);
      return null;
    }
  }

  loadAllResponseIds(): string[] {
    const index = this.getIndex();
    // Sort by timestamp (newest first)
    return index
      .sort((a, b) => b.timestamp - a.timestamp)
      .map((item) => item.id);
  }

  async loadRecentResponses(count: number): Promise<StoredResponse[]> {
    const ids = this.loadAllResponseIds();
    const recentIds = ids.slice(0, count);
    const responses: StoredResponse[] = [];

    for (const id of recentIds) {
      const response = await this.loadResponse(id);
      if (response) {
        responses.push(response);
      }
    }

    // Sort by timestamp (newest first)
    return responses.sort((a, b) => b.timestamp - a.timestamp);
  }

  async deleteResponse(id: string): Promise<void> {
    try {
      localStorage.removeItem(this.getStorageKey(id));
      const index = this.getIndex();
      const updatedIndex = index.filter((item) => item.id !== id);
      this.saveIndex(updatedIndex);
    } catch (error) {
      console.error(`Error deleting response ${id}:`, error);
      throw error;
    }
  }

  async clearAllResponses(): Promise<void> {
    try {
      const index = this.getIndex();
      index.forEach((item) => {
        localStorage.removeItem(this.getStorageKey(item.id));
      });
      localStorage.removeItem(INDEX_KEY);
    } catch (error) {
      console.error("Error clearing all responses:", error);
      throw error;
    }
  }

  getStorageStats(): { count: number; size: number; available: boolean } {
    try {
      const index = this.getIndex();
      const totalSize = index.reduce((sum, item) => sum + item.size, 0);
      return {
        count: index.length,
        size: totalSize,
        available: true,
      };
    } catch (error) {
      console.error("Error getting storage stats:", error);
      return {
        count: 0,
        size: 0,
        available: false,
      };
    }
  }
}

export const storageService = new StorageService();
