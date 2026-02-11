import { API_CONFIG, ChatResponse, MetricBlock } from "../config/api";

class ApiService {
  private baseUrl: string;
  private abortController: AbortController | null = null;

  constructor() {
    this.baseUrl = API_CONFIG.BASE_URL;
  }

  async sendChatPrompt(
    prompt: string,
    signal?: AbortSignal,
    id: string = ""
  ): Promise<ChatResponse> {
    // Cancel previous request if it exists
    console.log(this.abortController, signal);

    // Create new AbortController for this request
    const controller = new AbortController();
    this.abortController = controller;

    // Use external signal if provided, otherwise use controller's signal
    // If external signal is aborted, abort the controller too
    console.log(signal, "signal");

    try {
      console.log(signal, "signal");

      const response = await fetch(
        `${this.baseUrl}${API_CONFIG.ENDPOINTS.QUERY}`,
        {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "x-tab-id": id ?? "",
          },
          body: JSON.stringify({
            user_input: prompt,
          }),
          signal: controller.signal,
        }
      );

      // Try to parse JSON regardless of status to capture error bodies with reason/message
      let data: unknown = null;
      try {
        data = await response.json();
      } catch (error) {
        console.error("API Error:", error);

        data = null;
      }
      const isObject = (v: unknown): v is Record<string, unknown> =>
        typeof v === "object" && v !== null;

      // Success cases
      if (response.ok && isObject(data)) {
        const d = data as Record<string, unknown>;

        // Case A: Direct shape { results, insights, user_input, sql }
        if ("results" in d) {
          return {
            success: true,
            data: {
              rawData: Array.isArray(d.results as unknown[])
                ? (d.results as unknown[])
                : [],
              message:
                typeof d.message === "string"
                  ? (d.message as string)
                  : "Data retrieved successfully",
              insights:
                typeof d.insights === "string"
                  ? (d.insights as string)
                  : undefined,
              user_input:
                typeof d.user_input === "string"
                  ? (d.user_input as string)
                  : undefined,
              chat_response:
                typeof d.chat_response === "string"
                  ? (d.chat_response as string)
                  : undefined,
              sql: typeof d.sql === "string" ? (d.sql as string) : undefined,
              tab_id: id,
            },
          };
        }

        // Case A2: Direct shape without results but has chat_response and/or user_input
        if ("chat_response" in d || "user_input" in d) {
          return {
            success:
              typeof d.success === "boolean" ? (d.success as boolean) : true,
            data: {
              rawData: [],
              message:
                typeof d.message === "string"
                  ? (d.message as string)
                  : "Query processed successfully",
              insights:
                typeof d.insights === "string"
                  ? (d.insights as string)
                  : undefined,
              user_input:
                typeof d.user_input === "string"
                  ? (d.user_input as string)
                  : undefined,
              chat_response:
                typeof d.chat_response === "string"
                  ? (d.chat_response as string)
                  : undefined,
              sql: typeof d.sql === "string" ? (d.sql as string) : undefined,
              tab_id: id,
            },
            tab_id: id,
          };
        }

        // Case B: Wrapped shape { success, data: { results, insights, user_input, sql }, message }
        if ("data" in d && isObject(d.data)) {
          const inner = d.data as Record<string, unknown>;
          if ("results" in inner) {
            return {
              success:
                typeof d.success === "boolean" ? (d.success as boolean) : true,
              data: {
                rawData: Array.isArray(inner.results as unknown[])
                  ? (inner.results as unknown[])
                  : [],
                message:
                  typeof d.message === "string"
                    ? (d.message as string)
                    : typeof inner.message === "string"
                      ? (inner.message as string)
                      : "Data retrieved successfully",
                insights:
                  typeof inner.insights === "string"
                    ? (inner.insights as string)
                    : undefined,
                user_input:
                  typeof inner.user_input === "string"
                    ? (inner.user_input as string)
                    : undefined,
                chat_response:
                  typeof inner.chat_response === "string"
                    ? (inner.chat_response as string)
                    : undefined,
                sql:
                  typeof inner.sql === "string"
                    ? (inner.sql as string)
                    : undefined,
                tab_id: id,
              },
              tab_id: id,
            };
          }

          // Case C: Wrapped shape without results but has chat_response and/or user_input
          if ("chat_response" in inner || "user_input" in inner) {
            return {
              success:
                typeof d.success === "boolean" ? (d.success as boolean) : true,
              data: {
                rawData: [],
                message:
                  typeof d.message === "string"
                    ? (d.message as string)
                    : typeof inner.message === "string"
                      ? (inner.message as string)
                      : "Query processed successfully",
                insights:
                  typeof inner.insights === "string"
                    ? (inner.insights as string)
                    : undefined,
                user_input:
                  typeof inner.user_input === "string"
                    ? (inner.user_input as string)
                    : undefined,
                chat_response:
                  typeof inner.chat_response === "string"
                    ? (inner.chat_response as string)
                    : undefined,
                sql:
                  typeof inner.sql === "string"
                    ? (inner.sql as string)
                    : undefined,
                tab_id: id,
              },
              tab_id: id,
            };
          }
        }
      }

      //case D: Detect multi-metric analytics report (same structure as other parser blocks)
      if (response.ok && isObject(data) && isObject(data.data)) {
        const report = data.data as Record<string, unknown>;

        const metrics: Record<string, MetricBlock> = {};

        for (const key of Object.keys(report)) {
          const value = report[key];

          // Only process objects
          if (!value || typeof value !== "object") continue;

          // Must be a metric-style block (it MUST contain a "results" array)
          const block = value as {
            title?: string;
            sql?: string;
            insights?: string;
            results?: unknown[];
          };

          if (!block.results || !Array.isArray(block.results)) continue;

          const results = Array.isArray(block.results) ? block.results : [];

          const columns =
            results.length > 0 && Array.isArray(results[0])
              ? (results[0] as string[])
              : [];

          const rawRows =
            results.length > 1 && Array.isArray(results[1])
              ? (results[1] as unknown[][])
              : [];

          // Convert row arrays to objects if columns exist
          const rawData =
            columns.length > 0 && rawRows.length > 0
              ? rawRows.map((row) =>
                Object.fromEntries(columns.map((c, i) => [c, row[i]]))
              )
              : [];

          metrics[key] = {
            title: block.title,
            user_input: block.title,
            sql: block.sql,
            insights: block.insights,
            results: block.results,
            columns,
            rawData,
          };
        }

        return {
          success: true,

          data: {
            ...metrics, // spread dynamically
            tab_id: id,
            user_input:
              typeof prompt === "string" ? (prompt as string) : undefined,
          },
          tab_id: id,
          user_input:
            typeof prompt === "string" ? (prompt as string) : undefined,
        };
      }

      // Handle catalog conflict response
      if (isObject(data) && "message" in data && "catalog_options" in data) {
        const d = data as Record<string, unknown>;
        return {
          success: false,
          data: { rawData: [], message: "" },
          error:
            typeof d.message === "string"
              ? (d.message as string)
              : "Catalog selection required",
          catalog_options: Array.isArray(d.catalog_options)
            ? (d.catalog_options as string[])
            : undefined,
          conflicts: Array.isArray(d.conflicts)
            ? (d.conflicts as string[])
            : undefined,
          reason:
            typeof d.reason === "string"
              ? (d.reason as string)
              : typeof d.message === "string"
                ? (d.message as string)
                : undefined,
        };
      }

      // Handle error payload with detail.reason structure
      if (
        isObject(data) &&
        "detail" in data &&
        isObject((data as Record<string, unknown>).detail)
      ) {
        const detail = (data as Record<string, unknown>).detail as Record<
          string,
          unknown
        >;
        const hasReason = typeof detail.reason === "string";
        const hasErrorTrue = detail.error === true;
        if (hasReason || hasErrorTrue) {
          return {
            success: false,
            data: { rawData: [], message: "" },
            error: hasReason
              ? (detail.reason as string)
              : typeof detail.message === "string"
                ? (detail.message as string)
                : "Unknown error",
            reason: hasReason ? (detail.reason as string) : undefined,
          };
        }
      }

      // Handle other responses (errors or unknown)
      if (!response.ok) {
        const d = isObject(data)
          ? (data as Record<string, unknown>)
          : undefined;
        return {
          success: false,
          data: { rawData: [], message: "" },
          error:
            (d &&
              (typeof d.error === "string"
                ? (d.error as string)
                : typeof d.message === "string"
                  ? (d.message as string)
                  : undefined)) ||
            `HTTP error ${response.status}`,
          reason:
            d &&
            (typeof d.reason === "string"
              ? (d.reason as string)
              : typeof d.message === "string"
                ? (d.message as string)
                : undefined),
        };
      }

      // Fallback for unexpected but OK responses
      return {
        success: false,
        data: { rawData: [], message: "" },
        error:
          (isObject(data) &&
            (typeof data.error === "string"
              ? (data.error as string)
              : typeof data.message === "string"
                ? (data.message as string)
                : undefined)) ||
          "Unknown error occurred",
        reason:
          isObject(data) && typeof data.reason === "string"
            ? (data.reason as string)
            : undefined,
      };
    } catch (error) {
      // Don't throw error if request was aborted
      console.error("API Error:", error);

      if (error instanceof Error && error.name === "AbortError") {
        throw new Error("Request cancelled");
      }
      console.error("API Error:", error);
      throw error;
    } finally {
      // Clear abort controller if this was the active request
      if (this.abortController === controller) {
        this.abortController = null;
      }
    }
  }

  // Method to cancel current request
  cancelRequest(): void {
    if (this.abortController) {
      this.abortController.abort();
      this.abortController = null;
    }
  }
}

export const apiService = new ApiService();
