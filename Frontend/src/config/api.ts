import { APP_CONFIG } from "./constants";

// API Configuration (use Next.js rewrite proxy at /api/*)
export const API_CONFIG = {
  // Prefer explicit public env var for direct calls; fallback to Next.js rewrite
  // BASE_URL: (process.env.NEXT_PUBLIC_API_BASE ?? ''),
  BASE_URL: APP_CONFIG.BASE_URL,
  ENDPOINTS: {
    QUERY: "/query",
    Docs:"/docs",
  },
};
export interface MetricBlock {
  title?: string;
  sql?: string;
  rawData: unknown[];
  results?: unknown[];
  columns?: string[];
  message?: string;
  insights?: string;
  user_input?: string;
  chat_response?: string;
  reason?: string;
  tab_id?: string;
}

// API Response Types
export interface ChatResponse {
  success: boolean;
  data: {
    charts?: ChartData[]; // Made optional
    rawData?: unknown[]; // Replaced any[] with unknown[]
    results?: unknown[]; // Optional alternative dataset key
    message?: string;
    insights?: string; // Optional insights text from API
    user_input?: string; // Original user query coming from API
    chat_response?: string; // Assistant/chat reply from API
    reason?: string; // Optional reason message nested under data
    sql?: string; // Optional SQL string returned by API
    tab_id?: string; // Optional tab_id string
    [metricName: string]: MetricBlock | string | unknown | undefined;
  };

  error?: string;
  reason?: string; // Optional reason at top-level for errors
  catalog_options?: string[]; // Optional catalog options on conflict
  conflicts?: string[]; // Optional conflicts list on catalog conflict
  tab_id?: string;
  insights?: string;
  user_input?: string;
  show?:boolean;
  // Optional tab_id string
}
// export interface AnalyticsReport {
//   user_prompt?: string;
//   churn?: MetricBlock | null;
//   reconnect?: MetricBlock | null;
//   subscription?: MetricBlock | null;
//   insights?: string;
// }
export interface AnalyticsResponse {
  user_prompt?: string;
  metrics: Record<string, MetricBlock | null>;
}
export interface ChartData {
  type:
    | "line"
    | "bar"
    | "pie"
    | "doughnut"
    | "area"
    | "radar"
    | "polarArea"
    | "bubble"
    | "scatter";
  title: string;
  data: Record<string, unknown>; // Replaced any with Record<string, unknown>
  options?: Record<string, unknown>; // Replaced any with Record<string, unknown>
}
