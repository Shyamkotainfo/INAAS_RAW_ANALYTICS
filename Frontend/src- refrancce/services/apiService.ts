import { API_CONFIG, ChatResponse } from '../config/api';
 
class ApiService {
  private baseUrl: string;
 
  constructor() {
    this.baseUrl = API_CONFIG.BASE_URL;
  }
 
  async sendChatPrompt(prompt: string, preferredCatalog: string = ''): Promise<ChatResponse> {
    try {
      const response = await fetch(`${this.baseUrl}${API_CONFIG.ENDPOINTS.QUERY}`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          prompt,
          preferred_catalog: preferredCatalog
        }),
      });
 
      // Try to parse JSON regardless of status to capture error bodies with reason/message
      let data: unknown = null;
      try {
        data = await response.json();
      } catch {
        data = null;
      }
      const isObject = (v: unknown): v is Record<string, unknown> => typeof v === 'object' && v !== null;
     
      // Success cases
      if (response.ok && isObject(data)) {
        const d = data as Record<string, unknown>;
 
        // Case A: Direct shape { results, insights, user_input, sql }
        if ('results' in d) {
          return {
            success: true,
            data: {
              rawData: Array.isArray(d.results as unknown[]) ? (d.results as unknown[]) : [],
              message: typeof d.message === 'string' ? (d.message as string) : 'Data retrieved successfully',
              insights: typeof d.insights === 'string' ? (d.insights as string) : undefined,
              user_input: typeof d.user_input === 'string' ? (d.user_input as string) : undefined,
              chat_response: typeof d.chat_response === 'string' ? (d.chat_response as string) : undefined,
              sql: typeof d.sql === 'string' ? (d.sql as string) : undefined
            }
          };
        }

        // Case A2: Direct shape without results but has chat_response and/or user_input
        if ('chat_response' in d || 'user_input' in d) {
          return {
            success: typeof d.success === 'boolean' ? (d.success as boolean) : true,
            data: {
              rawData: [],
              message: typeof d.message === 'string' ? (d.message as string) : 'Query processed successfully',
              insights: typeof d.insights === 'string' ? (d.insights as string) : undefined,
              user_input: typeof d.user_input === 'string' ? (d.user_input as string) : undefined,
              chat_response: typeof d.chat_response === 'string' ? (d.chat_response as string) : undefined,
              sql: typeof d.sql === 'string' ? (d.sql as string) : undefined
            }
          };
        }
 
        // Case B: Wrapped shape { success, data: { results, insights, user_input, sql }, message }
        if ('data' in d && isObject(d.data)) {
          const inner = d.data as Record<string, unknown>;
          if ('results' in inner) {
            return {
              success: typeof d.success === 'boolean' ? (d.success as boolean) : true,
              data: {
                rawData: Array.isArray(inner.results as unknown[]) ? (inner.results as unknown[]) : [],
                message: typeof d.message === 'string' ? (d.message as string) : (typeof inner.message === 'string' ? (inner.message as string) : 'Data retrieved successfully'),
                insights: typeof inner.insights === 'string' ? (inner.insights as string) : undefined,
                user_input: typeof inner.user_input === 'string' ? (inner.user_input as string) : undefined,
                chat_response: typeof inner.chat_response === 'string' ? (inner.chat_response as string) : undefined,
                sql: typeof inner.sql === 'string' ? (inner.sql as string) : undefined
              }
            };
          }

          // Case C: Wrapped shape without results but has chat_response and/or user_input
          if ('chat_response' in inner || 'user_input' in inner) {
            return {
              success: typeof d.success === 'boolean' ? (d.success as boolean) : true,
              data: {
                rawData: [],
                message: typeof d.message === 'string' ? (d.message as string) : (typeof inner.message === 'string' ? (inner.message as string) : 'Query processed successfully'),
                insights: typeof inner.insights === 'string' ? (inner.insights as string) : undefined,
                user_input: typeof inner.user_input === 'string' ? (inner.user_input as string) : undefined,
                chat_response: typeof inner.chat_response === 'string' ? (inner.chat_response as string) : undefined,
                sql: typeof inner.sql === 'string' ? (inner.sql as string) : undefined
              }
            };
          }
        }
      }
     
      // Handle catalog conflict response
      if (isObject(data) && 'message' in data && 'catalog_options' in data) {
        const d = data as Record<string, unknown>;
        return {
          success: false,
          data: { rawData: [], message: '' },
          error: typeof d.message === 'string' ? (d.message as string) : 'Catalog selection required',
          catalog_options: Array.isArray(d.catalog_options) ? (d.catalog_options as string[]) : undefined,
          conflicts: Array.isArray(d.conflicts) ? (d.conflicts as string[]) : undefined,
          reason: typeof d.reason === 'string' ? (d.reason as string) : (typeof d.message === 'string' ? (d.message as string) : undefined)
        };
      }
 
      // Handle error payload with detail.reason structure
      if (isObject(data) && 'detail' in data && isObject((data as Record<string, unknown>).detail)) {
        const detail = (data as Record<string, unknown>).detail as Record<string, unknown>;
        const hasReason = typeof detail.reason === 'string';
        const hasErrorTrue = detail.error === true;
        if (hasReason || hasErrorTrue) {
        return {
          success: false,
          data: { rawData: [], message: '' },
          error: (hasReason ? (detail.reason as string) : (typeof detail.message === 'string' ? (detail.message as string) : 'Unknown error')),
          reason: hasReason ? (detail.reason as string) : undefined
        };
        }
      }
     
      // Handle other responses (errors or unknown)
      if (!response.ok) {
        const d = isObject(data) ? (data as Record<string, unknown>) : undefined;
        return {
          success: false,
          data: { rawData: [], message: '' },
          error: (d && (typeof d.error === 'string' ? (d.error as string) : (typeof d.message === 'string' ? (d.message as string) : undefined))) || `HTTP error ${response.status}`,
          reason: d && (typeof d.reason === 'string' ? (d.reason as string) : (typeof d.message === 'string' ? (d.message as string) : undefined))
        };
      }
 
      // Fallback for unexpected but OK responses
      return {
        success: false,
        data: { rawData: [], message: '' },
        error: (isObject(data) && (typeof data.error === 'string' ? (data.error as string) : (typeof data.message === 'string' ? (data.message as string) : undefined))) || 'Unknown error occurred',
        reason: isObject(data) && typeof data.reason === 'string' ? (data.reason as string) : undefined
      };
    } catch (error) {
      console.error('API Error:', error);
      throw error;
    }
  }
}
 
export const apiService = new ApiService();
 
 