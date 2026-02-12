"use client";
import React, {
  useState,
  useRef,
  useEffect,
  useMemo,
  useCallback,
} from "react";
import Image from "next/image";
import { apiService } from "../services/apiService";
import { storageService } from "../services/storageService";
import { ChatResponse, ChartData, MetricBlock } from "../config/api";
import ChartRenderer from "./ChartRenderer";
import DocumentIcon from "../asserts/Download-icon.svg";
// import logo from "@/asserts/info-sevices-logo.png";
import { Mic, Send } from "lucide-react";
import {
  Dialog,
  DialogContent,
  DialogActions,
  Button,
  Box,
  Typography,
} from "@mui/material";
import Prism from "prismjs";
import "prismjs/themes/prism.css"; // You can change theme later
import "prismjs/components/prism-sql"; // Enable SQL syntax highlighting

import { usePersistentState } from "../hooks/usePersistentState";

// Minimal typings for Web Speech API to avoid using `any`
interface MinimalSpeechRecognitionEvent {
  results: ArrayLike<{ 0: { transcript: string }; isFinal: boolean }>;
}

interface MinimalSpeechRecognition {
  continuous: boolean;
  interimResults: boolean;
  lang: string;
  maxAlternatives: number;
  onstart: (() => void) | null;
  onresult: ((event: MinimalSpeechRecognitionEvent) => void) | null;
  onend: (() => void) | null;
  onerror: ((event: { error: string }) => void) | null;
  start: () => void;
  stop: () => void;
}

type SpeechRecognitionConstructor = new () => MinimalSpeechRecognition;

// Safely access browser constructors without `any`
const getSpeechRecognitionCtor = ():
  | SpeechRecognitionConstructor
  | undefined => {
  if (typeof window === "undefined") return undefined;
  const w = window as unknown as {
    SpeechRecognition?: SpeechRecognitionConstructor;
    webkitSpeechRecognition?: SpeechRecognitionConstructor;
  };
  return w.SpeechRecognition || w.webkitSpeechRecognition;
};

const MAX_IN_MEMORY_RESPONSES = 15; // Maximum responses to keep in React state

type ResponseEntry = {
  response: ChatResponse;
  rawData: unknown[];
  chartTypes: { [key: number]: ChartData["type"] };
  resultId: string;
  tab_id?: string;
};

type TabSnapshot = {
  responses: ResponseEntry[];
  isInputUsed: boolean;
  name?: string;
};

const ChatInterface: React.FC = () => {
  const initialTabIdRef = useRef<string | null>(null);
  if (initialTabIdRef.current === null) {
    initialTabIdRef.current = Date.now().toString();
  }
  const initialTabId = initialTabIdRef.current!;

  const [prompt, setPrompt] = useState("");
  const [isLoading, setIsLoading] = useState(false);
  // const [responses, setResponses] = useState<
  //   Array<{
  //     response: ChatResponse;
  //     rawData: unknown[]; // replaced any[] with unknown[]
  //     chartTypes: { [key: number]: ChartData['type'] };
  //     resultId: string;
  //   }>
  // >([]);

  const [responses, setResponses, isLoaded] = usePersistentState<
    ResponseEntry[]
  >("chatResponses", []);
  const [isInputUsed, setIsInputUsed] = useState(false);

  // UI state for showing result
  const [statResults, setStatResults] = useState<Record<string, string>>({});

  type Tab = {
    id: string;
    name: string;
  };

  const [tabs, setTabs] = useState<Tab[]>([{ id: initialTabId, name: "" }]);
  const [activeTab, setActiveTab] = useState<string>(initialTabId);
  const tabStateRef = useRef<Record<string, TabSnapshot>>({});
  const [editingTab, setEditingTab] = useState<string | null>(null);
  const [editingValue, setEditingValue] = useState("");
  const [isLoadingOlder, setIsLoadingOlder] = useState(false);
  const [hasMoreResponses, setHasMoreResponses] = useState(false);
  const abortControllerRef = useRef<AbortController | null>(null);
  // const [selectedChartType] = useState<string>('bar');
  const [, setGlobalChartIndex] = useState(0);
  const [openMenuResultId, setOpenMenuResultId] = useState<string | null>(null);
  const [openDownloadResultId, setOpenDownloadResultId] = useState<
    string | null
  >(null);
  const [openSqlResultId, setOpenSqlResultId] = useState<string | null>(null);
  const [showTableInChat, setShowTableInChat] = useState<string | null>(null);
  const autoTableShownRef = useRef<Set<string>>(new Set());
  const [isGlobalDownloadOpen, setIsGlobalDownloadOpen] =
    useState<boolean>(false);
  const [bulkSelectedIds, setBulkSelectedIds] = useState<
    Record<string, boolean>
  >({});
  const [isBulkSelectAll, setIsBulkSelectAll] = useState<boolean>(false);
  const [randomQuote, setRandomQuote] = useState("");
  const [quickPage, setQuickPage] = useState(0);
  const [axisSelections, setAxisSelections] = useState<
    Record<string, { x?: string; y?: string }>
  >({});

  const [isListening, setIsListening] = useState(false);
  const [recognition, setRecognition] =
    useState<MinimalSpeechRecognition | null>(null);
  const [hasInterimResults, setHasInterimResults] = useState(false);
  // const [showCatalogModal, setShowCatalogModal] = useState(false);
  // const [catalogOptions, setCatalogOptions] = useState<string[]>([]);
  // const [, setConflicts] = useState<string[]>([]);
  // const [selectedCatalog, setSelectedCatalog] = useState<string>("");
  // const [pendingPrompt, setPendingPrompt] = useState<string>("");
  // const [chartIndex, setChartIndex] = useState(0);
  const inputRef = useRef<HTMLTextAreaElement>(null);
  const scrollContainerRef = useRef<HTMLDivElement>(null);
  const [isErrorOpen, setIsErrorOpen] = useState(false);
  const [errorMessage, setErrorMessage] = useState("");
  const [copiedId, setCopiedId] = useState<string | null>(null);
  const [dialogApiData, setDialogApiData] = useState<{
    user_input: string;
    chat_response: string;
  }>({ user_input: "", chat_response: "" });
  const activeTabRef = useRef(activeTab);
  useEffect(() => {
    activeTabRef.current = activeTab;
  }, [activeTab]);
  useEffect(() => {
    setOpenMenuResultId(null);
    setOpenDownloadResultId(null);
    setOpenSqlResultId(null);
    setShowTableInChat(null);
    setBulkSelectedIds({});
  }, [activeTab]);

  // useEffect(() => {
  //   tabStateRef.current[activeTab] = {
  //     responses,
  //     isInputUsed,
  //   };
  // }, [activeTab, responses, isInputUsed]);
  useEffect(() => {
    const tabData = tabStateRef.current[activeTab];
    if (tabData) {
      setResponses([...tabData.responses]);
    } else {
      setResponses([]);
    }
  }, [activeTab]);

  const handleTabSwitch = useCallback(
    (tabId: string) => {
      if (tabId === activeTab) return;
      tabStateRef.current[activeTab] = {
        responses,
        isInputUsed,
      };
      const nextState = tabStateRef.current[tabId] ?? {
        responses: [],
        isInputUsed: false,
      };
      tabStateRef.current[tabId] = nextState;
      setResponses([...nextState.responses]);
      setIsInputUsed(nextState.isInputUsed);

      console.log("tabStateRef.current[requestTab]", tabId, activeTab);
      setActiveTab(tabId);
    },
    [activeTab, responses, isInputUsed, setResponses, setIsInputUsed]
  );

  const addTab = useCallback(() => {
    tabStateRef.current[activeTab] = {
      responses,
      isInputUsed,
    };
    const newId = Date.now().toString();
    tabStateRef.current[newId] = {
      responses: [],
      isInputUsed: false,
      name: "Untitled",
    };
    setTabs((prev) => [...prev, { id: newId, name: "" }]);
    setResponses([]);
    setIsInputUsed(false);
    setActiveTab(newId);
  }, [
    activeTab,
    responses,
    isInputUsed,
    setTabs,
    setResponses,
    setIsInputUsed,
  ]);

  const removeTab = useCallback(
    (tabId: string) => {
      if (tabs.length <= 1) return;
      const filtered = tabs.filter((t) => t.id !== tabId);
      delete tabStateRef.current[tabId];
      setTabs(filtered);
      if (activeTab === tabId && filtered.length > 0) {
        const fallbackId = filtered[filtered.length - 1].id;
        const fallbackState = tabStateRef.current[fallbackId] ?? {
          responses: [],
          isInputUsed: false,
        };
        tabStateRef.current[fallbackId] = fallbackState;
        setResponses([...fallbackState.responses]);
        setIsInputUsed(fallbackState.isInputUsed);
        setActiveTab(fallbackId);
      }
    },
    [tabs, activeTab, setTabs, setResponses, setIsInputUsed]
  );

  const latestErrorMessage = useMemo(() => {
    if (!responses.length) return "";

    const last = responses[responses.length - 1];

    if (last?.response && last.response.success === false) {
      return (
        last.response.data?.reason ||
        last.response.reason ||
        last.response.error ||
        last.response.data?.message ||
        "An error occurred while processing your request."
      );
    }

    return "";
  }, [responses]);

  const latestApiData = useMemo(() => {
    if (!responses.length) {
      return { user_input: "", chat_response: "" };
    }

    const lastResponse = responses[responses.length - 1]
      ?.response as unknown as { data?: Record<string, unknown> };
    const data = (lastResponse?.data ?? {}) as Record<string, unknown>;
    const user_input = (data["user_input"] as string) ?? "";
    const chat_response = (data["chat_response"] as string) ?? "";

    return { user_input, chat_response };
  }, [responses]);

  // Landing page is shown when there are no responses and not currently loading
  const isLanding = responses.length === 0 && !isLoading;

  // Load recent responses from localStorage on mount
  useEffect(() => {
    const loadInitialResponses = async () => {
      try {
        const storedResponses = await storageService.loadRecentResponses(
          MAX_IN_MEMORY_RESPONSES
        );
        if (storedResponses.length > 0) {
          // Convert stored responses to the format expected by the component
          const formattedResponses = storedResponses.map((stored) => ({
            response: stored.response,
            rawData: stored.rawData,
            tab_id: stored.tab_id,
            chartTypes: stored.chartTypes as {
              [key: number]: ChartData["type"];
            },
            resultId: stored.resultId,
          }));
          setResponses(formattedResponses);

          // Check if there are more responses in storage
          const allIds = storageService.loadAllResponseIds();
          setHasMoreResponses(allIds.length > MAX_IN_MEMORY_RESPONSES);
        }
      } catch (error) {
        console.error("Error loading initial responses:", error);
      }
    };

    loadInitialResponses();
  }, []);
  useEffect(() => {
    return () => {
      apiService.cancelRequest();
    };
  }, []); // only once on unmount

  // Cleanup: Cancel requests and save responses on unmount
  useEffect(() => {
    return () => {
      // Cancel any in-flight requests
      // if (abortControllerRef.current) {
      //   abortControllerRef.current.abort();
      // }
      // apiService.cancelRequest();

      // Save current responses to localStorage
      responses.forEach((response) => {
        storageService
          .saveResponse({
            response: response.response,
            rawData: response.rawData,
            chartTypes: response.chartTypes as { [key: number]: string },
            resultId: response.resultId,
            tab_id: response.tab_id,
          })
          .catch((error) => {
            console.error("Error saving response on unmount:", error);
          });
      });
    };
  }, [responses]);

  // Function to manage response limit - move oldest to localStorage when limit reached
  const manageResponseLimit = useCallback(
    async (newResponses: typeof responses) => {
      if (newResponses.length > MAX_IN_MEMORY_RESPONSES) {
        // Move oldest responses to localStorage
        const toMove = newResponses.slice(
          0,
          newResponses.length - MAX_IN_MEMORY_RESPONSES
        );
        const toKeep = newResponses.slice(
          newResponses.length - MAX_IN_MEMORY_RESPONSES
        );

        // Save oldest responses to localStorage (fire and forget)
        toMove.forEach((response) => {
          storageService
            .saveResponse({
              response: response.response,
              rawData: response.rawData,
              chartTypes: response.chartTypes as { [key: number]: string },
              resultId: response.resultId,
              tab_id: response.tab_id,
            })
            .catch((error) => {
              console.error("Error saving response to localStorage:", error);
            });
        });

        // Update state with only the most recent responses
        setResponses(toKeep);
        setHasMoreResponses(true);
      }
    },
    []
  );

  useEffect(() => {
    if (!isLoading && (errorMessage || latestErrorMessage)) {
      setIsErrorOpen(true);
    }
  }, [isLoading, errorMessage, latestErrorMessage]);

  const chartTypeOptions: Array<{
    value: ChartData["type"];
    label: string;
    description: string;
    icon: string;
  }> = [
      {
        value: "bar",
        label: "Bar Chart",
        description: "For categorical comparisons",
        icon: "📊",
      },
      {
        value: "line",
        label: "Line Chart",
        description: "For time series and trends",
        icon: "📈",
      },
      {
        value: "pie",
        label: "Pie Chart",
        description: "For proportional data",
        icon: "🥧",
      },
      {
        value: "doughnut",
        label: "Doughnut Chart",
        description: "For proportional data with center space",
        icon: "🍩",
      },
      {
        value: "radar",
        label: "Radar Chart",
        description: "For multi-dimensional data comparison",
        icon: "🕸️",
      },
      {
        value: "polarArea",
        label: "Polar Area Chart",
        description: "For circular proportional data",
        icon: "⚪",
      },
      {
        value: "bubble",
        label: "Bubble Chart",
        description: "For 3D data visualization",
        icon: "🫧",
      },
      {
        value: "scatter",
        label: "Scatter Chart",
        description: "For correlation analysis",
        icon: "🔍",
      },
    ];

  const quickQueries = useMemo(
    () => [
      "Monthly Analytics Report",
      "Weekly Analytics Report",
      "Daily Analytics Report",
      "Monthly Churn Trends ",
      "Monthly Reconnect Trends ",
      "Daily Churn Trends ",
      "Daily Reconnect Trends ",
      "Weekly Churn Trends ",
      "Weekly Reconnect Trends",
    ],
    []
  );

  const quickQueryColors = [
    "#2563eb",
    "#16a34a",
    "#f59e0b",
    "#2563eb",
    "#16a34a",
    "#f59e0b",
    "#ef4444",
    "#8b5cf6",
    "#06b6d4",
  ];

  const quickPageSize = 5;
  const quickTotalPages = Math.max(
    1,
    Math.ceil(quickQueries.length / quickPageSize)
  );
  const visibleQuickQueries = useMemo(() => {
    const start = quickPage * quickPageSize;
    const end = start + quickPageSize;
    return quickQueries.slice(start, end);
  }, [quickPage, quickQueries]);

  const quotes = useMemo(
    () => [
      "Without data, you’re just another person with an opinion. — W. Edwards Deming",
      "Data is the new oil, but it’s only valuable when refined.",
      "In God we trust. All others must bring data. — W. Edwards Deming",
      "Data beats emotions. — Sean Rad",
      "Numbers have an important story to tell. They rely on you to give them a voice. — Stephen Few",
      "The goal is to turn data into information, and information into insight. — Carly Fiorina",
      "Visualization is daydreaming with a purpose. — Bo Bennett",
      "Graphs give us the power to see what our eyes cannot.",
      "Good design is making something intelligible and memorable. — Dieter Rams",
      "Charts aren’t just pictures, they are stories waiting to be told.",
      "The future belongs to those who can see patterns in data.",
      "Every dataset hides a story — visualization is how we uncover it.",
      "Discovery consists of seeing what everybody has seen and thinking what nobody has thought. — Albert Szent-Györgyi",
      "Curiosity is the engine of achievement. — Ken Robinson",
      "Data is precious, but insights are priceless.",
      "Simplicity is the ultimate sophistication. — Leonardo da Vinci",
      "Knowledge is power. Visualization is empowerment.",
      "Small insights lead to big changes.",
      "Great ideas come from looking at the same data differently.",
      "Every chart is a window into understanding.",
    ],
    []
  );

  useEffect(() => {
    if (isLoading) {
      const pick = quotes[Math.floor(Math.random() * quotes.length)];
      setRandomQuote(pick);
    }
  }, [isLoading, quotes]);

  //SQL Syntax Highlighter
  useEffect(() => {
    Prism.highlightAll();
  }, [openSqlResultId]);

  // Initialize speech recognition
  useEffect(() => {
    const initSpeechRecognition = () => {
      try {
        // Check for browser support
        const SpeechRecognition = getSpeechRecognitionCtor();

        if (!SpeechRecognition) {
          console.log("Speech recognition not supported in this browser");
          return;
        }

        const recognitionInstance = new SpeechRecognition();
        recognitionInstance.continuous = true; // Keep listening for multiple phrases
        recognitionInstance.interimResults = true; // Show live transcription as you speak
        recognitionInstance.lang = "en-US";
        recognitionInstance.maxAlternatives = 1;

        // Event handlers
        recognitionInstance.onstart = () => {
          setIsListening(true);
          console.log("Speech recognition started");
        };

        recognitionInstance.onresult = (
          event: MinimalSpeechRecognitionEvent
        ) => {
          let finalTranscript = "";
          let interimTranscript = "";

          // Process all results to separate final and interim
          for (let i = 0; i < event.results.length; i++) {
            const transcript = event.results[i][0].transcript;

            if (event.results[i].isFinal) {
              finalTranscript += transcript + " ";
            } else {
              interimTranscript += transcript;
            }
          }

          // Update the input with live transcription
          const currentText = (finalTranscript + interimTranscript).trim();
          setPrompt(currentText);

          // Track if we have interim results for styling
          setHasInterimResults(interimTranscript.length > 0);

          // Focus the input
          if (inputRef.current) {
            inputRef.current.focus();
          }

          console.log("Final:", finalTranscript, "Interim:", interimTranscript);
        };

        recognitionInstance.onend = () => {
          setIsListening(false);
          setHasInterimResults(false);
          console.log("Speech recognition ended");
        };

        recognitionInstance.onerror = (event: { error: string }) => {
          console.error("Speech recognition error:", event.error);
          setIsListening(false);
          setHasInterimResults(false);

          // Show user-friendly error messages (ignore 'no-speech' for continuous mode)
          if (event.error !== "no-speech") {
            let errorMessage = "Speech recognition error. ";
            switch (event.error) {
              case "audio-capture":
                errorMessage +=
                  "Microphone not accessible. Please check permissions.";
                break;
              case "not-allowed":
                errorMessage +=
                  "Microphone access denied. Please allow microphone access.";
                break;
              case "network":
                errorMessage += "Network error. Please check your connection.";
                break;
              default:
                errorMessage += "Please try again.";
            }
            alert(errorMessage);
          }
        };

        setRecognition(recognitionInstance);
      } catch (error) {
        console.error("Error initializing speech recognition:", error);
      }
    };

    initSpeechRecognition();
  }, []);

  // Auto-scroll to bottom of results container when loading or new responses arrive
  useEffect(() => {
    const el = scrollContainerRef.current;
    if (!el) return;
    // allow DOM to paint before scrolling
    requestAnimationFrame(() => {
      try {
        el.scrollTo({ top: el.scrollHeight, behavior: "smooth" });
      } catch {
        el.scrollTop = el.scrollHeight;
      }
    });
  }, [isLoading, responses.length]);

  // Close the chart type dropdown, data panel, download menu and SQL menu on outside click
  useEffect(() => {
    const handleOutsideClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (!target?.closest('[data-chart-menu-container="true"]')) {
        setOpenMenuResultId(null);
        setOpenDownloadResultId(null);
        if (!target?.closest('[data-global-download-container="true"]')) {
          setIsGlobalDownloadOpen(false);
        }
      }
      if (!target?.closest('[data-sql-menu-container="true"]')) {
        setOpenSqlResultId(null);
      }
    };
    document.addEventListener("mousedown", handleOutsideClick);
    return () => document.removeEventListener("mousedown", handleOutsideClick);
  }, []);

  // Handle speech recognition
  const handleSpeechRecognition = () => {
    if (!recognition) {
      alert(
        "Speech recognition is not available in this browser. Please try Chrome, Safari, or Edge."
      );
      return;
    }

    if (isListening) {
      // Stop listening
      recognition.stop();
    } else {
      // Start listening
      try {
        recognition.start();
      } catch (error) {
        console.error("Error starting speech recognition:", error);
        alert("Error starting speech recognition. Please try again.");
      }
    }
  };
  function isMetricBlock(value: unknown): value is MetricBlock {
    return (
      typeof value === "object" &&
      value !== null &&
      ("results" in (value as Record<string, unknown>) ||
        "rawData" in (value as Record<string, unknown>))
    );
  }
  const handleSubmit = async (
    e: React.FormEvent,
    overridePrompt?: string
  ) => {
    e.preventDefault();
    const requestTab = activeTabRef.current;
    console.log(
      tabStateRef.current[requestTab],
      activeTabRef.current,
      requestTab,
      "tabStateRef.current[requestTab]"
    );

    const currentPrompt = (overridePrompt ?? prompt).trim();
    if (!currentPrompt || isLoading) return;

    // Cancel previous request if it exists
    // if (abortControllerRef.current) {
    //   abortControllerRef.current.abort();
    // }
    apiService.cancelRequest();
    // mark tab as used
    tabStateRef.current[requestTab] = tabStateRef.current[requestTab] || {
      responses: [],
      isInputUsed: false,
    };
    tabStateRef.current[requestTab].isInputUsed = true;

    // Create new AbortController for this request
    const controller = new AbortController();
    abortControllerRef.current = controller;

    setPrompt("");
    setIsLoading(true);
    setIsInputUsed(true);


    try {
      // Use real API service
      const chatResponse = await apiService.sendChatPrompt(
        currentPrompt,
        controller.signal,
        requestTab
      );

      // If API returned an error (non-conflict), show modal but do not create a result card
      if (!chatResponse.success) {
        const reason =
          chatResponse.data?.reason ||
          chatResponse.reason ||
          chatResponse.error ||
          "An error occurred while processing your request.";
        setErrorMessage(reason);
        return;
      }
      console.log(chatResponse, "chatResponseresponseDatachatResponse");

      // 🧠 Detect dynamic metric blocks in chatResponse.data
      if (chatResponse.data && typeof chatResponse.data === "object") {
        const metricKeys = Object.keys(chatResponse.data).filter((key) =>
          isMetricBlock(chatResponse.data[key as keyof ChatResponse["data"]])
        ) as (keyof ChatResponse["data"])[];
        if (metricKeys.length > 0) {
          const headingText =
            chatResponse.data.user_prompt ||
            chatResponse.data.user_input ||
            null;
          console.log(headingText, "chatResponseresponseDatachatResponse");

          if (headingText) {
            const headingResult = {
              response: {
                data: {
                  user_input:
                    typeof headingText === "string"
                      ? (headingText as string)
                      : undefined,
                },
                success: true,
                tab_id: requestTab,
                show: false,
              },
              sql: "",
              title: headingText,
              chartTypes: {},
              rawData: [],
              resultId: `heading-${Date.now()}`,
              tab_id: requestTab,
              section: "heading",
            };

            if (!tabStateRef.current[requestTab]) {
              tabStateRef.current[requestTab] = {
                responses: [],
                isInputUsed: true,
              };
            }
            console.log(headingResult, "chatResponseresponseDatachatResponse");

            tabStateRef.current[requestTab].responses.push(headingResult);
          }
          metricKeys.forEach((sec) => {
            const block = chatResponse.data[sec] as MetricBlock;

            if (!block) return;
            console.log(block, "chatResponseresponseDatachatResponse");

            const rawDataArray = Array.isArray(block.rawData)
              ? block.rawData
              : undefined;
            const resultsArray = Array.isArray(block.results)
              ? block.results
              : undefined;

            const hasRawData = !!rawDataArray && rawDataArray.length > 0;
            const hasResultsData = !!resultsArray && resultsArray.length > 0;

            const newResult = {
              response: {
                data: { ...block, show: true },
                success: true,
                tab_id: requestTab,
                show: true,
              },

              rawData: hasRawData
                ? rawDataArray ?? []
                : hasResultsData
                  ? resultsArray ?? []
                  : [],

              insights: block.insights,
              sql: block.sql,
              title: block.title,
              // user_input: block.title,
              section: sec,
              chartTypes: {},
              resultId: `result-${Date.now()}-${Math.random()
                .toString(36)
                .substring(2, 9)}`,
              tab_id: requestTab,
            };
            console.log(newResult, "chatResponseresponseDatachatResponse");

            if (!tabStateRef.current[requestTab]) {
              tabStateRef.current[requestTab] = {
                responses: [],
                isInputUsed: true,
              };
            }

            tabStateRef.current[requestTab].responses.push(newResult);
          });
        }

        // Update UI only for active tab
        if (activeTabRef.current === requestTab) {
          setResponses([...tabStateRef.current[requestTab].responses]);
        }
      }

      const responseData = chatResponse.data ?? {};
      const rawDataArray = Array.isArray(responseData.rawData)
        ? responseData.rawData
        : undefined;
      const resultsCandidate = (responseData as { results?: unknown[] })
        .results;
      const resultsArray = Array.isArray(resultsCandidate)
        ? resultsCandidate
        : undefined;
      const hasRawData = !!rawDataArray && rawDataArray.length > 0;
      const hasResultsData = !!resultsArray && resultsArray.length > 0;
      const insightsValue =
        typeof responseData.insights === "string"
          ? responseData.insights.trim()
          : "";
      const hasInsights = insightsValue.length > 0;
      const shouldCreateResultCard =
        chatResponse.success && (hasRawData || hasResultsData || hasInsights);
      chatResponse.show = true;
      // Add new result card if we have data to visualize or insights to share
      if (shouldCreateResultCard) {
        // Clear any previous error modal/state on success
        setErrorMessage("");
        // Open dialog if chat_response is present to display conversation response
        if (chatResponse.data?.chat_response) {
          setDialogApiData({
            user_input: chatResponse.data.user_input || "",
            chat_response: chatResponse.data.chat_response || "",
          });
          setIsErrorOpen(true);
        } else {
          setIsErrorOpen(false);
        }
        const newResult = {
          response: { ...chatResponse, tab_id: requestTab, show: true },
          rawData: hasRawData
            ? rawDataArray ?? []
            : hasResultsData
              ? resultsArray ?? []
              : [],
          chartTypes: {},
          resultId: `result-${Date.now()}-${Math.random()
            .toString(36)
            .substr(2, 9)}`,
          tab_id: chatResponse.tab_id,
        };
        console.log(newResult, "chatResponseresponseData");

        // Add new response and manage limit
        // setResponses((prev) => {
        //   const updated = [...prev, newResult];
        //   // Save to localStorage immediately
        //   storageService
        //     .saveResponse({
        //       response: newResult.response,
        //       rawData: newResult.rawData,
        //       chartTypes: newResult.chartTypes as { [key: number]: string },
        //       resultId: newResult.resultId,
        //       tab_id: newResult.tab_id,
        //     })
        //     .catch((error) => {
        //       console.error("Error saving response:", error);
        //     });

        //   // Manage limit asynchronously
        //   if (updated.length > MAX_IN_MEMORY_RESPONSES) {
        //     manageResponseLimit(updated);
        //     // Return only the most recent responses for immediate UI update
        //     return updated.slice(updated.length - MAX_IN_MEMORY_RESPONSES);
        //   }
        //   return updated;
        // });
        // Insert only inside correct tab
        console.log(
          tabStateRef.current[requestTab],
          activeTabRef.current,
          requestTab,
          "tabStateRef.current[requestTab]"
        );
        if (!tabStateRef.current[requestTab]) {
          tabStateRef.current[requestTab] = {
            responses: [],
            isInputUsed: true,
          };
        }

        // tabStateRef.current[requestTab].responses = [
        //   ...tabStateRef.current[requestTab].responses,
        //   newResult,
        // ];
        // // Update UI ONLY for activeTab
        // if (activeTabRef.current === requestTab) {
        //   setResponses([...tabStateRef.current[requestTab].responses]);
        // }
        // --- Insert new per-tab response logic ---
        const tabBucket = tabStateRef.current[requestTab];

        const currentList = tabBucket.responses;
        const updated = [...currentList, newResult];

        // Save to storage
        storageService.saveResponse({
          response: newResult.response,
          rawData: newResult.rawData,
          chartTypes: newResult.chartTypes as { [key: number]: string },
          resultId: newResult.resultId,
          tab_id: requestTab,
        }).catch((err) => console.error("Error saving response:", err));

        // Enforce per-tab memory limit
        if (updated.length > MAX_IN_MEMORY_RESPONSES) {
          manageResponseLimit(updated);
          tabBucket.responses =
            updated.slice(updated.length - MAX_IN_MEMORY_RESPONSES);
        } else {
          tabBucket.responses = updated;
        }

        // Update UI only if active tab matches
        if (activeTabRef.current === requestTab) {
          setResponses([...tabBucket.responses]);
        }

        setGlobalChartIndex((prev) => prev + 1);
      } else if (chatResponse.success && chatResponse.data?.chat_response) {
        // Conversation-only: open dialog, do not create a card
        setErrorMessage("");
        setDialogApiData({
          user_input: chatResponse.data.user_input || "",
          chat_response: chatResponse.data.chat_response || "",
        });
        setIsErrorOpen(true);
      }
    } catch (error) {
      // Don't show error if request was cancelled
      if (error instanceof Error && error.message === "Request cancelled") {
        return;
      }
      console.error("Error sending prompt:", error);
      setErrorMessage("Failed to get response");
      setIsErrorOpen(true);
    } finally {
      setIsLoading(false);
      // Clear abort controller reference
      if (abortControllerRef.current === controller) {
        abortControllerRef.current = null;
      }
    }
  };



  // const handleChartTypeChange = (resultId: string, chartIndex: number, newType: string) => {
  //   setResponses(prev => prev.map(result =>
  //     result.resultId === resultId
  //       ? { ...result, chartTypes: { ...result.chartTypes, [chartIndex]: newType } }
  //       : result
  //   ));
  // };

  const handleChartTypeChangeAll = (
    resultId: string,
    chartsCount: number,
    newType: ChartData["type"]
  ) => {
    setResponses((prev) =>
      prev.map((result) => {
        if (result.resultId !== resultId) return result;
        const updatedTypes: { [key: number]: ChartData["type"] } = {
          ...result.chartTypes,
        };
        for (let i = 0; i < chartsCount; i++) {
          updatedTypes[i] = newType;
        }
        return { ...result, chartTypes: updatedTypes };
      })
    );
    // Close table view when chart type is changed
    setShowTableInChat(null);
  };

  // Auto-show table first when a dataset has more than two columns, but only once per result.
  // This only triggers when no table is currently open, preserving user interactions.
  useEffect(() => {
    if (showTableInChat !== null) return;
    for (const res of responses) {
      const rows = res?.rawData;
      if (
        Array.isArray(rows) &&
        rows.length > 0 &&
        typeof rows[0] === "object"
      ) {
        const headers = Object.keys(rows[0] as Record<string, unknown>);
        if (
          headers.length > 2 &&
          !autoTableShownRef.current.has(res.resultId)
        ) {
          setShowTableInChat(res.resultId);
          autoTableShownRef.current.add(res.resultId);
          break;
        }
      }
    }
  }, [responses, showTableInChat]);

  // If all responses are cleared, reset the memory to allow future auto-open.
  useEffect(() => {
    if (responses.length === 0) {
      autoTableShownRef.current.clear();
    }
  }, [responses.length]);

  const handleCloseResult = (resultId: string) => {
    setResponses((prev) => {
      const updated = prev.filter((result) => result.resultId !== resultId);
      // If no results remain, revert to landing layout
      if (updated.length === 0) {
        setIsInputUsed(false);
      }
      return updated;
    });
    // Also delete from localStorage
    storageService.deleteResponse(resultId).catch((error) => {
      console.error("Error deleting response from storage:", error);
    });
    // Update global chart index
    setGlobalChartIndex((prev) => Math.max(0, prev - 1));
  };

  const handleClearAll = async () => {
    if (
      !confirm(
        "Are you sure you want to clear all responses? This cannot be undone."
      )
    ) {
      return;
    }
    try {
      await storageService.clearAllResponses();
      setResponses([]);
      setIsInputUsed(false);
      setHasMoreResponses(false);
      setGlobalChartIndex(0);
    } catch (error) {
      console.error("Error clearing all responses:", error);
      alert("Failed to clear all responses. Please try again.");
    }
  };

  const handleClearRecent = () => {
    if (
      !confirm(
        "Clear recent responses from memory? They will remain in storage."
      )
    ) {
      return;
    }
    setResponses([]);
    setIsInputUsed(false);
    // Keep hasMoreResponses as true since storage still has responses
    const allIds = storageService.loadAllResponseIds();
    setHasMoreResponses(allIds.length > 0);
  };

  const handleLoadMore = async () => {
    if (isLoadingOlder || !hasMoreResponses) return;

    setIsLoadingOlder(true);
    try {
      const allIds = storageService.loadAllResponseIds();
      const currentIds = new Set(responses.map((r) => r.resultId));
      const idsToLoad = allIds.filter((id) => !currentIds.has(id));

      if (idsToLoad.length === 0) {
        setHasMoreResponses(false);
        setIsLoadingOlder(false);
        return;
      }

      // Load next batch (up to MAX_IN_MEMORY_RESPONSES)
      const batchSize = Math.min(MAX_IN_MEMORY_RESPONSES, idsToLoad.length);
      const batchIds = idsToLoad.slice(0, batchSize);

      const loadedResponses = await Promise.all(
        batchIds.map((id) => storageService.loadResponse(id))
      );

      const validResponses = loadedResponses
        .filter((r): r is NonNullable<typeof r> => r !== null)
        .map((stored) => ({
          response: stored.response,
          rawData: stored.rawData,
          chartTypes: stored.chartTypes as { [key: number]: ChartData["type"] },
          resultId: stored.resultId,
          tab_id: stored.tab_id,
        }));

      if (validResponses.length > 0) {
        setResponses((prev) => [...prev, ...validResponses]);
        setHasMoreResponses(idsToLoad.length > batchSize);
      } else {
        setHasMoreResponses(false);
      }
    } catch (error) {
      console.error("Error loading more responses:", error);
      alert("Failed to load more responses. Please try again.");
    } finally {
      setIsLoadingOlder(false);
    }
  };

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      handleSubmit(e);
    }

    // ESC key to stop speech recognition
    if (e.key === "Escape" && isListening) {
      e.preventDefault();
      recognition?.stop();
    }
  };

  const generateChartsFromData = (
    rawData: unknown[],
    resultChartTypes: { [key: number]: ChartData["type"] },
    _response: ChatResponse,
    baseIndex: number,
    axisSelections?: { x?: string; y?: string }
  ): ChartData[] => {
    if (!rawData.length) return [];

    // Analyze the data structure to determine what we're working with
    const sampleItem = rawData[0] as Record<string, unknown>;
    const keys = Object.keys(sampleItem);

    // Find numeric and string keys
    const numericKeys = keys.filter(
      (key) => typeof sampleItem[key] === "number"
    );
    const stringKeys = keys.filter(
      (key) => typeof sampleItem[key] === "string"
    );

    // Heuristics to detect time-like labels for better default chart type selection
    const isDateLikeValue = (value: unknown): boolean => {
      if (typeof value !== "string") return false;
      const t = Date.parse(value);
      return Number.isFinite(t);
    };

    const isTimeLabelKey = (key: string): boolean =>
      /^(period|date|day|week|month|year|time)$/i.test(key) ||
      /date|day|period|week|month|year|time/i.test(key);

    // Create charts based on the data structure
    const charts: ChartData[] = [];

    if (numericKeys.length > 0 && stringKeys.length > 0) {
      // Apply the same filtering logic as the table
      let labelKey: string;
      let valueKey: string;

      if (axisSelections?.y === "reconnecting_users" && axisSelections?.x) {
        // Special case: reconnecting_users with X selection - use both
        labelKey = axisSelections.x;
        valueKey = "reconnecting_users";
      } else if (axisSelections?.x && axisSelections?.y) {
        // Both X and Y selected - use both
        labelKey = axisSelections.x;
        valueKey = axisSelections.y;
      } else if (axisSelections?.x && !axisSelections?.y) {
        // Only X selected - use X as label, find a numeric value
        labelKey = axisSelections.x;
        const preferredNumericOrder = [
          "reconnecting_users",
          "churned_users",
          "count",
          "value",
        ];
        valueKey =
          preferredNumericOrder.find((k) => numericKeys.includes(k)) ||
          numericKeys[0];
      } else if (axisSelections?.y && !axisSelections?.x) {
        // Only Y selected
        if (axisSelections.y && stringKeys.includes(axisSelections.y)) {
          // If Y is a string key (e.g., period), use it as labels and pick a numeric value
          labelKey = axisSelections.y;
          const preferredNumericOrder = [
            "reconnecting_users",
            "churned_users",
            "count",
            "value",
          ];
          valueKey =
            preferredNumericOrder.find((k) => numericKeys.includes(k)) ||
            numericKeys[0];
        } else {
          // Y is numeric - use it as value and find a string label
          valueKey = axisSelections.y;
          labelKey = stringKeys.find((k) => isTimeLabelKey(k)) || stringKeys[0];
        }
      } else {
        // No selections - use default logic
        labelKey = stringKeys.find((k) => isTimeLabelKey(k)) || stringKeys[0];
        const preferredNumericOrder = [
          "reconnecting_users",
          "churned_users",
          "count",
          "value",
        ];
        valueKey =
          preferredNumericOrder.find((k) => numericKeys.includes(k)) ||
          numericKeys[0];
      }

      // Default to bar unless explicitly selected by the user
      const firstLabelValue = (rawData[0] as Record<string, unknown>)[labelKey];
      const looksTimeSeries =
        isTimeLabelKey(labelKey) || isDateLikeValue(firstLabelValue);
      const mainChartType: ChartData["type"] = resultChartTypes[0] || "bar";

      // Filter data based on axis selections
      // Key fix: Always use axisSelections.x for chartLabels when present, regardless of type
      let chartLabels: string[] = [];
      let chartData: number[] = [];

      if (axisSelections?.x && !axisSelections?.y) {
        // Only X selected - use actual X values as labels
        chartLabels = rawData.map((item) =>
          String((item as Record<string, unknown>)[axisSelections.x!])
        );
        // Provide a simple sequential value so the chart can render while emphasizing labels
        chartData = rawData.map((_, index) => index + 1);
      } else if (axisSelections?.y && !axisSelections?.x) {
        // Only Y selected - show only Y axis data
        chartLabels = rawData.map((_, index) => `Item ${index + 1}`);
        chartData = rawData.map((item) =>
          Number((item as Record<string, unknown>)[axisSelections.y!])
        );
      } else if (axisSelections?.x && axisSelections?.y) {
        // Both selected - handle combinations of numeric/string
        const xIsNumeric = numericKeys.includes(axisSelections.x);
        const yIsString = stringKeys.includes(axisSelections.y);

        if (xIsNumeric && yIsString) {
          // X numeric, Y categorical (e.g., reconnecting_users vs period)
          const swappedLabelKey = axisSelections.y;
          const swappedValueKey = axisSelections.x;

          if (mainChartType === "line") {
            // Build {x,y} points and set axis types for a line chart
            const points = rawData.map((item) => ({
              x: Number((item as Record<string, unknown>)[swappedValueKey]),
              y: String((item as Record<string, unknown>)[swappedLabelKey]),
            }));
            charts.push({
              type: "line",
              title: `Chat ${baseIndex + 1} - Data Analysis`,
              data: {
                labels: [], // not used for category Y with object points
                datasets: [
                  {
                    label: swappedValueKey
                      .replace(/_/g, " ")
                      .replace(/\b\w/g, (l) => l.toUpperCase()),
                    data: points as unknown as number[],
                    borderColor: "rgba(44,90,160,1)",
                    backgroundColor: "rgba(44,90,160,0.2)",
                    borderWidth: 2,
                    fill: false,
                  },
                ],
              },
              options: {
                parsing: true,
                scales: {
                  x: { type: "linear", beginAtZero: true },
                  y: { type: "category" },
                },
              },
            });
            return charts;
          } else {
            // Default to horizontal bar for readability
            chartLabels = rawData.map((item) =>
              String((item as Record<string, unknown>)[swappedLabelKey])
            );
            chartData = rawData.map((item) =>
              Number((item as Record<string, unknown>)[swappedValueKey])
            );
            charts.push({
              type: "bar",
              title: `Chat ${baseIndex + 1} - Data Analysis`,
              data: {
                labels: chartLabels,
                datasets: [
                  {
                    label: swappedValueKey
                      .replace(/_/g, " ")
                      .replace(/\b\w/g, (l) => l.toUpperCase()),
                    data: chartData,
                    backgroundColor: chartData.map(
                      (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 60%)`
                    ),
                    borderColor: chartData.map(
                      (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 50%)`
                    ),
                    borderWidth: 2,
                  },
                ],
              },
              options: { indexAxis: "y" },
            });
            // Skip the generic push below since we've already pushed a tailored chart
            return charts;
          }
        } else if (
          stringKeys.includes(axisSelections.x) &&
          stringKeys.includes(axisSelections.y) &&
          axisSelections.x === axisSelections.y &&
          isTimeLabelKey(axisSelections.x)
        ) {
          // Both X and Y are the same time-like field (e.g., period vs period)
          const key = axisSelections.x;
          const labels = rawData.map((item) =>
            String((item as Record<string, unknown>)[key])
          );
          const points = labels.map((lbl) => ({ x: lbl, y: lbl }));
          charts.push({
            type: "line",
            title: `Chat ${baseIndex + 1} - Period vs Period`,
            data: {
              labels,
              datasets: [
                {
                  label: key
                    .replace(/_/g, " ")
                    .replace(/\b\w/g, (l) => l.toUpperCase()),
                  data: points as unknown as number[],
                  borderColor: "rgba(44,90,160,1)",
                  backgroundColor: "rgba(44,90,160,0.2)",
                  borderWidth: 2,
                  fill: false,
                },
              ],
            },
            options: {
              parsing: true,
              scales: {
                x: { type: "category" },
                y: { type: "category" },
              },
            },
          });
          return charts;
        } else {
          // Normal case: X categorical, Y numeric
          // User expects: X selection on horizontal axis, Y selection on vertical axis
          const xIsString = stringKeys.includes(axisSelections.x!);
          const yIsNumeric = numericKeys.includes(axisSelections.y!);

          if (xIsString && yIsNumeric && mainChartType === "bar") {
            // For horizontal bar chart: labels go on Y-axis (vertical), data on X-axis (horizontal)
            // User selected: X=series_name (wants horizontal), Y=episode_count (wants vertical)
            // So: put Y selection in labels (vertical Y-axis), X selection in data (horizontal X-axis)
            // But wait - that doesn't work because data must be numeric
            // Actually: For horizontal bars, we want X selection (series_name) on horizontal axis
            // In horizontal bar: data goes horizontal, labels go vertical
            // So: X selection should be in data (horizontal), Y selection should be in labels (vertical)
            // But labels must be strings and data must be numeric, so we can't swap them
            // Solution: Keep X in labels (which will appear on Y-axis in horizontal chart)
            // But the user wants X on horizontal, so we need to NOT use horizontal bars
            // OR: Use vertical bars when user explicitly selects X and Y

            // Actually, let's use vertical bars to match user expectation:
            // X selection (series_name) in labels -> X-axis (horizontal) ✓
            // Y selection (episode_count) in data -> Y-axis (vertical) ✓
            chartLabels = rawData.map((item) =>
              String((item as Record<string, unknown>)[axisSelections.x!])
            );
            chartData = rawData.map((item) =>
              Number((item as Record<string, unknown>)[axisSelections.y!])
            );
            // Use vertical bar chart (default) to match user's axis expectations
          } else {
            // Standard vertical bar or other chart types - X selection goes to labels (X-axis), Y to data (Y-axis)
            chartLabels = rawData.map((item) =>
              String((item as Record<string, unknown>)[axisSelections.x!])
            );
            chartData = rawData.map((item) =>
              Number((item as Record<string, unknown>)[axisSelections.y!])
            );
          }
        }
      } else {
        // No selections - use default
        chartLabels = rawData.map((item) =>
          String((item as Record<string, unknown>)[labelKey])
        );
        chartData = rawData.map((item) =>
          Number((item as Record<string, unknown>)[valueKey])
        );
      }

      // When both X and Y are explicitly selected, ensure chart respects axis mapping
      // For bar charts: if both axes selected, use vertical bars so X selection appears on horizontal axis
      const chartOptions: Record<string, unknown> = {};
      if (axisSelections?.x && axisSelections?.y && mainChartType === "bar") {
        // Don't set indexAxis: 'y' - use vertical bars to match user's axis expectations
        // X selection in labels -> X-axis (horizontal) ✓
        // Y selection in data -> Y-axis (vertical) ✓
      }

      const primaryChart: ChartData = {
        type: mainChartType,
        title: `Chat ${baseIndex + 1} - Data Analysis`,
        data: {
          labels: chartLabels,
          datasets: [
            {
              label:
                axisSelections?.x && axisSelections?.y
                  ? axisSelections.y
                    .replace(/_/g, " ")
                    .replace(/\b\w/g, (l) => l.toUpperCase())
                  : axisSelections?.x && !axisSelections?.y
                    ? labelKey
                      .replace(/_/g, " ")
                      .replace(/\b\w/g, (l) => l.toUpperCase())
                    : valueKey
                      .replace(/_/g, " ")
                      .replace(/\b\w/g, (l) => l.toUpperCase()),
              data: chartData,
              backgroundColor: chartData.map(
                (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 60%)`
              ),
              borderColor: chartData.map(
                (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 50%)`
              ),
              borderWidth: 2,
              // Hint to renderer: draw labels from x-axis labels instead of numeric values when only X is selected
              dataLabelFromLabel: !!(axisSelections?.x && !axisSelections?.y),
            },
          ],
        },
        options:
          Object.keys(chartOptions).length > 0 ? chartOptions : undefined,
      };

      // Only show comparison chart if multiple numeric keys AND no Y axis selection
      // When user selects Y axis, they want a single chart based on their selection
      const shouldShowComparisonChart =
        numericKeys.length > 1 && !axisSelections?.y;

      if (shouldShowComparisonChart) {
        const comparisonChartType: ChartData["type"] =
          resultChartTypes[0] || (looksTimeSeries ? "line" : "bar");
        const comparisonChart: ChartData = {
          type: comparisonChartType,
          title: `Chat ${baseIndex + 1} - Multiple Metrics`,
          data: {
            labels: rawData.map(
              (item) => (item as Record<string, unknown>)[labelKey]
            ),
            datasets: numericKeys.map((key, index) => ({
              label: key
                .replace(/_/g, " ")
                .replace(/\b\w/g, (l) => l.toUpperCase()),
              data: rawData.map((item) =>
                Number((item as Record<string, unknown>)[key])
              ),
              backgroundColor: `hsl(${(index * 120) % 360}, 70%, 60%)`,
              borderColor: `hsl(${(index * 120) % 360}, 70%, 50%)`,
              borderWidth: 2,
            })),
          },
        };
        charts.push(comparisonChart);
      } else {
        charts.push(primaryChart);
      }
    } else if (numericKeys.length > 0) {
      // Only numeric data - good for pie/doughnut charts
      const distributionChartType: ChartData["type"] =
        resultChartTypes[0] || "pie";
      charts.push({
        type: distributionChartType,
        title: `Chat ${baseIndex + 1} - Data Distribution`,
        data: {
          labels: rawData.map((_, index) => `Item ${index + 1}`),
          datasets: [
            {
              data: rawData.map((item) =>
                numericKeys[0]
                  ? Number((item as Record<string, unknown>)[numericKeys[0]])
                  : 0
              ),
              backgroundColor: rawData.map(
                (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 60%)`
              ),
              borderColor: rawData.map(
                (_, index) => `hsl(${(index * 137.5) % 360}, 70%, 50%)`
              ),
              borderWidth: 2,
            },
          ],
        },
      });
    }

    return charts;
  };

  // Convert insights markdown-ish text to plain text for download
  const formatInsightsToText = (raw: string): string => {
    if (!raw) return "";
    const lines = raw
      .split("\n")
      .map((l) => l.trim())
      .filter((l) => l.length > 0)
      .map((line) =>
        line
          .replace(/^[-•*]\s*/, "")
          .replace(/\*\*(.*?)\*\*:/g, "$1:")
          .replace(/\*\*(.*?)\*\*/g, "$1")
      );
    return lines.join("\n");
  };

  // Trigger a client-side file download
  const triggerDownload = (
    filename: string,
    content: string,
    mimeType: string = "text/plain;charset=utf-8"
  ) => {
    try {
      const blob = new Blob([content], { type: mimeType });
      const url = URL.createObjectURL(blob);
      const anchor = document.createElement("a");
      anchor.href = url;
      anchor.download = filename;
      document.body.appendChild(anchor);
      anchor.click();
      document.body.removeChild(anchor);
      URL.revokeObjectURL(url);
    } catch (error) {
      console.error("Download failed:", error);
      alert("Could not download the file.");
    }
  };

  const handleDownloadSummary = (result: {
    response?: { data?: Record<string, unknown> };
    resultId?: string;
  }) => {
    const insightsRaw = result?.response?.data?.insights || "";
    const insightsText = formatInsightsToText(String(insightsRaw));
    const header = "IAAS - BI BOT Summary\n";
    const timestamp = `Generated: ${new Date().toLocaleString()}\n\n`;
    const content = `${header}${timestamp}${insightsText}\n`;
    const filename = `summary-${result?.resultId || "export"}.txt`;
    triggerDownload(filename, content);
  };

  const handleDownloadData = (result: {
    rawData?: unknown[];
    resultId?: string;
  }) => {
    const data: Array<Record<string, unknown>> = Array.isArray(result?.rawData)
      ? (result.rawData as Array<Record<string, unknown>>)
      : [];
    if (!data.length || typeof data[0] !== "object") {
      alert("No tabular data available to download.");
      return;
    }

    const headers = Object.keys(data[0] as Record<string, unknown>);
    const escapeCsv = (value: unknown) => {
      const str = value === null || value === undefined ? "" : String(value);
      if (/[",\n\r]/.test(str)) {
        return '"' + str.replace(/"/g, '""') + '"';
      }
      return str;
    };

    const rows = data.map((row: Record<string, unknown>) =>
      headers
        .map((h) =>
          escapeCsv(
            typeof row[h] === "object" ? JSON.stringify(row[h]) : row[h]
          )
        )
        .join(",")
    );
    const csv = [headers.join(","), ...rows].join("\r\n");
    const filename = `data-${result?.resultId || "export"}.csv`;
    triggerDownload(filename, csv, "text/csv;charset=utf-8");
  };
  const startEditingTab = (tabId: string, currentName: string) => {
    setEditingTab(tabId);
    setEditingValue(currentName);
  };
  const saveTabName = (tabId: string) => {
    const newName = editingValue.trim() || `Untitled ${tabId}`;

    // 1. Save into tabStateRef
    tabStateRef.current[tabId] = {
      ...tabStateRef.current[tabId],
      name: newName,
    };

    // 2. Update the tabs[] array so React re-renders correctly
    setTabs((prevTabs) =>
      prevTabs.map((t) =>
        t.id === tabId
          ? { ...t, name: newName } // <-- FIX
          : t
      )
    );

    setEditingTab(null);
    setEditingValue("");
  };

  const handleDownloadAll = async (result: {
    resultId?: string;
    response?: { data?: Record<string, unknown> };
    rawData?: unknown[];
  }) => {
    try {
      if (!(window as unknown as { jspdf?: unknown }).jspdf) {
        await new Promise<void>((resolve, reject) => {
          const script = document.createElement("script");
          script.src =
            "https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js";
          script.async = true;
          script.onload = () => resolve();
          script.onerror = () => reject(new Error("Failed to load jsPDF"));
          document.body.appendChild(script);
        });
      }

      type JsPDFInstance = {
        internal: { pageSize: { getWidth(): number; getHeight(): number } };
        setFontSize(size: number): void;
        setFont(font: string, style?: string): void;
        text(
          text: string,
          x: number,
          y: number,
          options?: { align?: "left" | "center" | "right" }
        ): void;
        addPage(): void;
        getTextWidth(text: string): number;
        splitTextToSize(text: string, size: number): string[] | string;
        addImage(...args: unknown[]): void;
        setTextColor(r: number, g?: number, b?: number): void;
        save(filename: string): void;
      };
      type JsPDFConstructor = new (options?: {
        unit?: string;
        format?: string | [number, number];
      }) => JsPDFInstance;

      const jspdfNs = (window as unknown as { jspdf?: unknown }).jspdf || {};
      const jsPDF = (jspdfNs as Record<string, unknown>)["jsPDF"] as unknown;
      const JsPDFCtor = jsPDF as JsPDFConstructor;
      if (typeof JsPDFCtor !== "function") {
        alert("PDF generator not available.");
        return;
      }

      // Ensure AutoTable plugin is available
      const ensureAutoTable = async () => {
        const api = (
          JsPDFCtor as unknown as { API?: { autoTable?: unknown } } | undefined
        )?.API;
        const hasPlugin = typeof api?.autoTable === "function";
        if (!hasPlugin) {
          await new Promise<void>((resolve, reject) => {
            const script = document.createElement("script");
            script.src =
              "https://cdn.jsdelivr.net/npm/jspdf-autotable@3.8.1/dist/jspdf.plugin.autotable.min.js";
            script.async = true;
            script.onload = () => resolve();
            script.onerror = () =>
              reject(new Error("Failed to load jsPDF-AutoTable"));
            document.body.appendChild(script);
          });
        }
      };
      await ensureAutoTable();

      const doc = new JsPDFCtor({ unit: "pt", format: "a4" });
      const pageWidth: number = doc.internal.pageSize.getWidth();
      const marginX = 40;
      let cursorY = 20; // very small header at top

      doc.setFontSize(10);
      doc.setFont("helvetica", "bold");
      doc.text("IAAS - BI BOT Report", marginX, cursorY);
      cursorY += 10;
      doc.setFontSize(8);
      doc.setTextColor(100);
      doc.text(`Generated: ${new Date().toLocaleString()}`, marginX, cursorY);
      doc.setTextColor(0);
      cursorY += 10;

      // Centered user input (same as UI title under each card)
      try {
        const rawUserInput =
          (result?.response?.data?.["user_input"] as unknown) ??
          (result?.response?.data?.["title"] as unknown) ??
          "No Query Information Available";
        const formattedUserInput: string = String(rawUserInput)
          .toLowerCase()
          .replace(/\b\w/g, (char: string) => char.toUpperCase());
        // add top margin before the query title
        cursorY += 16;
        doc.setFontSize(16);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(44, 90, 160);
        doc.text(formattedUserInput, pageWidth / 2, cursorY, {
          align: "center",
        });
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        // Slightly smaller gap before charts
        const selChartPadTop = 12;
        cursorY += selChartPadTop;
      } catch {
        /* ignore formatting errors and continue */
      }

      // Charts section FIRST
      {
        const pageHeight: number = doc.internal.pageSize.getHeight();
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(139, 92, 246); // Purple
        doc.text("Charts", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        // Add explicit top padding before chart image
        const chartPaddingTop = 16;
        const chartPaddingBottom = 24;
        cursorY += chartPaddingTop;
        const container = document.querySelector(
          `[data-result-id="${result?.resultId}"]`
        );
        const canvases = container
          ? Array.from(container.querySelectorAll("canvas"))
          : [];
        const targetCanvas = canvases[2] || canvases[0];
        if (targetCanvas) {
          const dataUrl = (targetCanvas as HTMLCanvasElement).toDataURL(
            "image/png",
            1.0
          );
          const chartScale = 0.7; // reduce visual size
          const maxImgWidth = (pageWidth - marginX * 2) * chartScale;
          const aspect =
            (targetCanvas as HTMLCanvasElement).height /
            (targetCanvas as HTMLCanvasElement).width;
          const availableHeight = Math.max(60, pageHeight - 60 - cursorY);
          let drawWidth = maxImgWidth;
          let drawHeight = drawWidth * aspect;
          if (drawHeight > availableHeight) {
            const scale = availableHeight / drawHeight;
            drawWidth = Math.max(160, drawWidth * scale);
            drawHeight = Math.max(96, drawHeight * scale);
          }
          if (cursorY + drawHeight > pageHeight - 40) {
            doc.addPage();
            cursorY = 50;
            const freshAvailableHeight = pageHeight - 60 - cursorY;
            drawWidth = Math.min(maxImgWidth, drawWidth);
            drawHeight = Math.min(freshAvailableHeight, drawWidth * aspect);
          }
          doc.addImage(
            dataUrl,
            "PNG",
            marginX,
            cursorY,
            drawWidth,
            drawHeight,
            undefined,
            "FAST"
          );
          // Add explicit bottom padding after chart image
          cursorY += drawHeight + chartPaddingBottom;
        } else {
          doc.setFontSize(10);
          doc.text("Chart not available.", marginX, cursorY);
          cursorY += chartPaddingBottom;
        }
      }

      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(44, 90, 160); // Blue
      doc.text("Summary", marginX, cursorY);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(0);
      // Extra space below the Summary heading before content lines
      cursorY += 24;
      const summaryRaw = String(result?.response?.data?.["insights"] ?? "");
      const summaryText =
        formatInsightsToText(summaryRaw) || "No insights provided.";
      const maxWidth = pageWidth - marginX * 2;
      const summaryLinesArray = summaryText.split("\n");
      for (const rawLine of summaryLinesArray) {
        const line = rawLine.trim();
        if (!line) {
          cursorY += 12;
          continue;
        }
        const match = /^(.*?):\s*(.*)$/.exec(line);
        if (match) {
          const label = match[1] + ":";
          const content = match[2] || "";

          // Draw label (bold)
          doc.setFont("helvetica", "bold");
          doc.text(label, marginX, cursorY);
          const labelWidth = doc.getTextWidth(label + " ");

          // Draw content (normal) with wrapping within remaining width
          doc.setFont("helvetica", "normal");
          const wrappedContent = doc.splitTextToSize(
            content,
            Math.max(10, maxWidth - labelWidth)
          );
          if (Array.isArray(wrappedContent) && wrappedContent.length > 0) {
            // First line continues on same line after label
            doc.text(wrappedContent[0], marginX + labelWidth, cursorY);
            cursorY += 14;
            // Remaining lines align with content start
            for (let i = 1; i < wrappedContent.length; i++) {
              // Page break if needed
              if (cursorY > doc.internal.pageSize.getHeight() - 60) {
                doc.addPage();
                cursorY = 50;
              }
              doc.text(wrappedContent[i], marginX + labelWidth, cursorY);
              cursorY += 14;
            }
          } else {
            cursorY += 14;
          }
          // Ensure font reset (normal) for next line
          doc.setFont("helvetica", "normal");
        } else {
          // No colon: draw whole line in normal text wrapped
          doc.setFont("helvetica", "normal");
          const wrapped = doc.splitTextToSize(line, maxWidth);
          const arr = Array.isArray(wrapped) ? wrapped : [wrapped];
          for (const seg of arr) {
            if (cursorY > doc.internal.pageSize.getHeight() - 60) {
              doc.addPage();
              cursorY = 50;
            }
            doc.text(seg, marginX, cursorY);
            cursorY += 14;
          }
        }
        if (cursorY > doc.internal.pageSize.getHeight() - 60) {
          doc.addPage();
          cursorY = 50;
        }
      }

      // Data table right after summary on the same page with extra spacing after summary
      cursorY += 24;
      const dataArr: Array<Record<string, unknown>> = Array.isArray(
        result?.rawData
      )
        ? (result.rawData as Array<Record<string, unknown>>)
        : [];
      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.setTextColor(22, 163, 74); // Green
      doc.text("Data", marginX, cursorY);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(0);
      // Extra space below the Data heading before rendering the table
      cursorY += 16;
      if (dataArr.length > 0 && typeof dataArr[0] === "object") {
        const headers = Object.keys(dataArr[0] as Record<string, unknown>);
        const body = dataArr.map((row: Record<string, unknown>) =>
          headers.map((h) => {
            const raw = row[h as keyof typeof row];
            const value =
              typeof raw === "object" ? JSON.stringify(raw) : raw ?? "";
            const str = String(value);
            return str.length > 120 ? str.slice(0, 117) + "..." : str;
          })
        );
        (doc as unknown as { autoTable: (opts: unknown) => void }).autoTable({
          head: [headers],
          body,
          startY: cursorY,
          margin: { left: marginX, right: marginX },
          styles: { fontSize: 9 },
          headStyles: { fillColor: [44, 90, 160] },
          didDrawPage: (data: { cursor: { y: number } }) => {
            cursorY = data.cursor.y + 10;
          },
        });
      } else {
        doc.setFontSize(10);
        doc.text("No data available.", marginX, cursorY);
        cursorY += 14;
      }

      // Charts already printed above

      const filename = `report-${result?.resultId || "export"}.pdf`;
      doc.save(filename);
    } catch (err) {
      console.error("Failed to generate PDF", err);
      alert("Could not generate the PDF.");
    }
  };
  interface ResponseDataWithResults {
    results?: unknown[];
  }

  function normalizeRows(
    rawData: unknown[],
    responseData: unknown
  ): Array<Record<string, unknown>> {
    // If rawData already contains objects → return as-is
    if (
      Array.isArray(rawData) &&
      rawData.length > 0 &&
      typeof rawData[0] === "object" &&
      rawData[0] !== null &&
      !Array.isArray(rawData[0])
    ) {
      return rawData as Array<Record<string, unknown>>;
    }

    // If responseData contains { results: [columns, rows] }
    const data = responseData as ResponseDataWithResults;

    if (
      data?.results &&
      Array.isArray(data.results) &&
      data.results.length === 2 &&
      Array.isArray(data.results[0]) &&
      Array.isArray(data.results[1])
    ) {
      const columns = data.results[0] as unknown[];
      const rows = data.results[1] as unknown[];

      // Convert rows into objects: { columnName: value }
      return rows.map((row) => {
        if (!Array.isArray(row)) return {};
        return Object.fromEntries(
          columns.map((col, index) => [String(col), row[index]])
        );
      });
    }

    return [];
  }

  const handleDownloadAllResponses = useCallback(async () => {
    if (responses.length === 0) {
      alert("No responses to download.");
      return;
    }

    try {
      if (!(window as unknown as { jspdf?: unknown }).jspdf) {
        await new Promise<void>((resolve, reject) => {
          const script = document.createElement("script");
          script.src =
            "https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js";
          script.async = true;
          script.onload = () => resolve();
          script.onerror = () => reject(new Error("Failed to load jsPDF"));
          document.body.appendChild(script);
        });
      }

      type JsPDFInstance = {
        internal: { pageSize: { getWidth(): number; getHeight(): number } };
        setFontSize(size: number): void;
        setFont(font: string, style?: string): void;
        text(
          text: string,
          x: number,
          y: number,
          options?: { align?: "left" | "center" | "right" }
        ): void;
        addPage(): void;
        getTextWidth(text: string): number;
        splitTextToSize(text: string, size: number): string[] | string;
        addImage(...args: unknown[]): void;
        setTextColor(r: number, g?: number, b?: number): void;
        save(filename: string): void;
      };
      type JsPDFConstructor = new (options?: {
        unit?: string;
        format?: string | [number, number];
      }) => JsPDFInstance;

      const jspdfNs = (window as unknown as { jspdf?: unknown }).jspdf || {};
      const jsPDF = (jspdfNs as Record<string, unknown>)["jsPDF"] as unknown;
      const JsPdfCtor = jsPDF as JsPDFConstructor;
      if (typeof JsPdfCtor !== "function") {
        alert("PDF generator not available.");
        return;
      }

      // Ensure AutoTable plugin is available
      const ensureAutoTable = async () => {
        const api = (
          JsPdfCtor as unknown as { API?: { autoTable?: unknown } } | undefined
        )?.API;
        const hasPlugin = typeof api?.autoTable === "function";
        if (!hasPlugin) {
          await new Promise<void>((resolve, reject) => {
            const script = document.createElement("script");
            script.src =
              "https://cdn.jsdelivr.net/npm/jspdf-autotable@3.8.1/dist/jspdf.plugin.autotable.min.js";
            script.async = true;
            script.onload = () => resolve();
            script.onerror = () =>
              reject(new Error("Failed to load jsPDF-AutoTable"));
            document.body.appendChild(script);
          });
        }
      };
      await ensureAutoTable();

      const doc = new JsPdfCtor({ unit: "pt", format: "a4" });
      const pageWidth: number = doc.internal.pageSize.getWidth();
      const marginX = 40;
      let cursorY = 50;

      // Compact title block
      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.text("IAAS - BI BOT Complete Report", marginX, cursorY);
      cursorY += 12;
      doc.setFontSize(9);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(100);
      doc.text(`Generated: ${new Date().toLocaleString()}`, marginX, cursorY);
      doc.setTextColor(0);
      cursorY += 10;
      doc.setFontSize(9);
      doc.text(`Total Responses: ${responses.length}`, marginX, cursorY);
      cursorY += 14;

      // Process each response
      responses.forEach((result, index) => {
        // Add new page for each response (except the first one which continues on title page)
        if (index > 0) {
          doc.addPage();
          cursorY = 50;
        }

        // Small spacer before the centered query title (no "Response N" label)
        cursorY += 6;

        // Centered user input (blue)
        try {
          const rawUserInput =
            (result?.response?.data?.["user_input"] as unknown) ??
            "No Query Information Available";
          const formattedUserInput: string = String(rawUserInput)
            .toLowerCase()
            .replace(/\b\w/g, (char: string) => char.toUpperCase());
          doc.setFontSize(14);
          doc.setFont("helvetica", "bold");
          doc.setTextColor(44, 90, 160);
          const pageWidthAll: number = doc.internal.pageSize.getWidth();
          doc.text(formattedUserInput, pageWidthAll / 2, cursorY, {
            align: "center",
          });
          doc.setFont("helvetica", "normal");
          doc.setTextColor(0);
          cursorY += 20;
        } catch {
          /* ignore */
        }

        // Charts section (moved first)
        const pageHeight: number = doc.internal.pageSize.getHeight();
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(139, 92, 246); // Purple
        doc.text("Charts", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        // Add padding before chart images in All Responses
        const allChartPadTop = 12;
        const allChartPadBottom = 12;
        cursorY += allChartPadTop;

        // Find and add chart images
        const container = document.querySelector(
          `[data-result-id="${result?.resultId}"]`
        );
        const canvases = container
          ? Array.from(container.querySelectorAll("canvas"))
          : [];

        if (canvases.length > 0) {
          canvases.forEach((canvas) => {
            if (cursorY > pageHeight - 200) {
              doc.addPage();
              cursorY = 50;
            }

            const dataUrl = (canvas as HTMLCanvasElement).toDataURL(
              "image/png",
              1.0
            );
            const chartScale = 0.68; // smaller in all-responses
            const maxImgWidth = (pageWidth - marginX * 2) * chartScale;
            const aspect =
              (canvas as HTMLCanvasElement).height /
              (canvas as HTMLCanvasElement).width;
            const availableHeight = Math.max(60, pageHeight - 60 - cursorY);

            let drawWidth = maxImgWidth;
            let drawHeight = drawWidth * aspect;

            if (drawHeight > availableHeight) {
              const scale = availableHeight / drawHeight;
              drawWidth = Math.max(160, drawWidth * scale);
              drawHeight = Math.max(96, drawHeight * scale);
            }

            if (cursorY + drawHeight > pageHeight - 40) {
              doc.addPage();
              cursorY = 50;
              const freshAvailableHeight = pageHeight - 60 - cursorY;
              drawWidth = Math.min(maxImgWidth, drawWidth);
              drawHeight = Math.min(freshAvailableHeight, drawWidth * aspect);
            }

            doc.addImage(
              dataUrl,
              "PNG",
              marginX,
              cursorY,
              drawWidth,
              drawHeight,
              undefined,
              "FAST"
            );
            cursorY += drawHeight + allChartPadBottom;
          });
        } else {
          doc.setFontSize(10);
          doc.text("Charts not available.", marginX, cursorY);
          cursorY += allChartPadBottom;
        }

        // Summary section (second)
        if (cursorY > doc.internal.pageSize.getHeight() - 120) {
          doc.addPage();
          cursorY = 50;
        }
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(44, 90, 160); // Blue
        doc.text("Summary", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        const selChartPadTop = 12;
        cursorY += selChartPadTop;

        const summaryRaw: string = result?.response?.data?.insights || "";
        const summaryText =
          formatInsightsToText(summaryRaw) || "No insights provided.";
        const maxWidth = pageWidth - marginX * 2;
        const summaryLinesArray = summaryText.split("\n");

        for (const rawLine of summaryLinesArray) {
          const line = rawLine.trim();
          if (!line) {
            cursorY += 12;
            continue;
          }

          const match = /^(.*?):\s*(.*)$/.exec(line);
          if (match) {
            const label = match[1] + ":";
            const content = match[2] || "";

            // Draw label (bold)
            doc.setFont("helvetica", "bold");
            doc.text(label, marginX, cursorY);
            const labelWidth = doc.getTextWidth(label + " ");

            // Draw content (normal) with wrapping
            doc.setFont("helvetica", "normal");
            const wrappedContent = doc.splitTextToSize(
              content,
              Math.max(10, maxWidth - labelWidth)
            );
            if (Array.isArray(wrappedContent) && wrappedContent.length > 0) {
              doc.text(wrappedContent[0], marginX + labelWidth, cursorY);
              cursorY += 14;
              for (let i = 1; i < wrappedContent.length; i++) {
                if (cursorY > doc.internal.pageSize.getHeight() - 60) {
                  doc.addPage();
                  cursorY = 50;
                }
                doc.text(wrappedContent[i], marginX + labelWidth, cursorY);
                cursorY += 14;
              }
            } else {
              cursorY += 14;
            }
            doc.setFont("helvetica", "normal");
          } else {
            doc.setFont("helvetica", "normal");
            const wrapped = doc.splitTextToSize(line, maxWidth);
            const arr = Array.isArray(wrapped) ? wrapped : [wrapped];
            for (const seg of arr) {
              if (cursorY > doc.internal.pageSize.getHeight() - 60) {
                doc.addPage();
                cursorY = 50;
              }
              doc.text(seg, marginX, cursorY);
              cursorY += 14;
            }
          }

          if (cursorY > doc.internal.pageSize.getHeight() - 60) {
            doc.addPage();
            cursorY = 50;
          }
        }

        // Data section (third)
        cursorY += 20;
        const dataArr: Array<Record<string, unknown>> = Array.isArray(
          result?.rawData
        )
          ? (result.rawData as Array<Record<string, unknown>>)
          : [];
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(22, 163, 74); // Green
        doc.text("Data", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        cursorY += 20;

        if (dataArr.length > 0 && typeof dataArr[0] === "object") {
          const headers = Object.keys(dataArr[0] as Record<string, unknown>);
          const body = dataArr.map((row: Record<string, unknown>) =>
            headers.map((h) => {
              const raw = row[h as keyof typeof row];
              const value =
                typeof raw === "object" ? JSON.stringify(raw) : raw ?? "";
              const str = String(value);
              return str.length > 120 ? str.slice(0, 117) + "..." : str;
            })
          );

          (doc as unknown as { autoTable: (opts: unknown) => void }).autoTable({
            head: [headers],
            body,
            startY: cursorY,
            margin: { left: marginX, right: marginX },
            styles: { fontSize: 8 },
            headStyles: { fillColor: [44, 90, 160] },
            didDrawPage: (data: { cursor: { y: number } }) => {
              cursorY = data.cursor.y + 10;
            },
          });
        } else {
          doc.setFontSize(10);
          doc.text("No data available.", marginX, cursorY);
          cursorY += 14;
        }
      });

      const filename = `complete-report-${new Date().toISOString().split("T")[0]
        }.pdf`;
      doc.save(filename);
    } catch (err) {
      console.error("Failed to generate complete PDF", err);
      alert("Could not generate the complete PDF.");
    }
  }, [responses]);

  // Expose for debugging and to satisfy linter usage when no UI button is rendered
  useEffect(() => {
    (
      window as unknown as { __downloadAllResponses?: unknown }
    ).__downloadAllResponses = handleDownloadAllResponses;
    return () => {
      delete (window as unknown as { __downloadAllResponses?: unknown })
        .__downloadAllResponses;
    };
  }, [responses, handleDownloadAllResponses]);

  const formatUserInputTitle = (raw: unknown): string => {
    const fallback = "No Query Information Available";
    try {
      const value = raw ?? fallback;
      return String(value)
        .toLowerCase()
        .replace(/\b\w/g, (char: string) => char.toUpperCase());
    } catch {
      return fallback;
    }
  };

  const openBulkDownload = () => {
    const seed: Record<string, boolean> = {};
    responses.forEach((r) => {
      seed[r.resultId] = false;
    });
    setBulkSelectedIds(seed);
    setIsBulkSelectAll(false);
    setIsGlobalDownloadOpen((prev) => !prev);
  };

  const toggleSelectAllBulk = (checked: boolean) => {
    const next: Record<string, boolean> = {};
    responses.forEach((r) => {
      next[r.resultId] = checked;
    });
    setBulkSelectedIds(next);
    setIsBulkSelectAll(checked);
  };

  const toggleSingleBulk = (resultId: string, checked: boolean) => {
    setBulkSelectedIds((prev) => {
      const updated = { ...prev, [resultId]: checked };
      const allSelected =
        responses.length > 0 && responses.every((r) => updated[r.resultId]);
      setIsBulkSelectAll(allSelected);
      return updated;
    });
  };

  const handleDownloadSelectedResponses = async () => {
    const selected = responses.filter((r) => bulkSelectedIds[r.resultId]);
    if (selected.length === 0) {
      alert("Please select at least one response.");
      return;
    }

    try {
      if (!(window as unknown as { jspdf?: unknown }).jspdf) {
        await new Promise<void>((resolve, reject) => {
          const script = document.createElement("script");
          script.src =
            "https://cdn.jsdelivr.net/npm/jspdf@2.5.1/dist/jspdf.umd.min.js";
          script.async = true;
          script.onload = () => resolve();
          script.onerror = () => reject(new Error("Failed to load jsPDF"));
          document.body.appendChild(script);
        });
      }

      type JsPDFInstance = {
        internal: { pageSize: { getWidth(): number; getHeight(): number } };
        setFontSize(size: number): void;
        setFont(font: string, style?: string): void;
        text(
          text: string,
          x: number,
          y: number,
          options?: { align?: "left" | "center" | "right" }
        ): void;
        addPage(): void;
        getTextWidth(text: string): number;
        splitTextToSize(text: string, size: number): string[] | string;
        addImage(...args: unknown[]): void;
        setTextColor(r: number, g?: number, b?: number): void;
        save(filename: string): void;
      };
      type JsPDFConstructor = new (options?: {
        unit?: string;
        format?: string | [number, number];
      }) => JsPDFInstance;

      const jspdfNs = (window as unknown as { jspdf?: unknown }).jspdf || {};
      const jsPDF = (jspdfNs as Record<string, unknown>)["jsPDF"] as unknown;
      const JsPdfCtor = jsPDF as JsPDFConstructor;
      if (typeof JsPdfCtor !== "function") {
        alert("PDF generator not available.");
        return;
      }

      const ensureAutoTable = async () => {
        const api = (
          JsPdfCtor as unknown as { API?: { autoTable?: unknown } } | undefined
        )?.API;
        const hasPlugin = typeof api?.autoTable === "function";
        if (!hasPlugin) {
          await new Promise<void>((resolve, reject) => {
            const script = document.createElement("script");
            script.src =
              "https://cdn.jsdelivr.net/npm/jspdf-autotable@3.8.1/dist/jspdf.plugin.autotable.min.js";
            script.async = true;
            script.onload = () => resolve();
            script.onerror = () =>
              reject(new Error("Failed to load jsPDF-AutoTable"));
            document.body.appendChild(script);
          });
        }
      };
      await ensureAutoTable();

      const doc = new JsPdfCtor({ unit: "pt", format: "a4" });
      const pageWidth: number = doc.internal.pageSize.getWidth();
      const marginX = 40;
      let cursorY = 50;

      doc.setFontSize(12);
      doc.setFont("helvetica", "bold");
      doc.text("IAAS - BI BOT Selected Report", marginX, cursorY);
      cursorY += 12;
      doc.setFontSize(9);
      doc.setFont("helvetica", "normal");
      doc.setTextColor(100);
      doc.text(`Generated: ${new Date().toLocaleString()}`, marginX, cursorY);
      doc.setTextColor(0);
      cursorY += 14;
      doc.setFontSize(9);
      doc.text(`Selected Responses: ${selected.length}`, marginX, cursorY);
      cursorY += 14;

      selected.forEach((result, index) => {
        if (index > 0) {
          doc.addPage();
          cursorY = 50;
        }

        cursorY += 6;
        try {
          const formattedUserInput = formatUserInputTitle(
            result?.response?.data?.user_input
          );
          doc.setFontSize(14);
          doc.setFont("helvetica", "bold");
          doc.setTextColor(44, 90, 160);
          const pageWidthAll: number = doc.internal.pageSize.getWidth();
          doc.text(formattedUserInput, pageWidthAll / 2, cursorY, {
            align: "center",
          });
          doc.setFont("helvetica", "normal");
          doc.setTextColor(0);
          cursorY += 20;
        } catch { }

        const pageHeight: number = doc.internal.pageSize.getHeight();
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(139, 92, 246);
        doc.text("Charts", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        const selChartPadTop = 12;
        cursorY += selChartPadTop;

        const container = document.querySelector(
          `[data-result-id="${result?.resultId}"]`
        );
        const canvases = container
          ? Array.from(container.querySelectorAll("canvas"))
          : [];

        if (canvases.length > 0) {
          canvases.forEach((canvas) => {
            if (cursorY > pageHeight - 200) {
              doc.addPage();
              cursorY = 50;
            }
            const dataUrl = (canvas as HTMLCanvasElement).toDataURL(
              "image/png",
              1.0
            );
            const chartScale = 0.7;
            const maxImgWidth = (pageWidth - marginX * 2) * chartScale;
            const aspect =
              (canvas as HTMLCanvasElement).height /
              (canvas as HTMLCanvasElement).width;
            const availableHeight = Math.max(60, pageHeight - 60 - cursorY);
            let drawWidth = maxImgWidth;
            let drawHeight = drawWidth * aspect;
            if (drawHeight > availableHeight) {
              const scale = availableHeight / drawHeight;
              drawWidth = Math.max(160, drawWidth * scale);
              drawHeight = Math.max(96, drawHeight * scale);
            }
            if (cursorY + drawHeight > pageHeight - 40) {
              doc.addPage();
              cursorY = 50;
              const freshAvailableHeight = pageHeight - 60 - cursorY;
              drawWidth = Math.min(maxImgWidth, drawWidth);
              drawHeight = Math.min(freshAvailableHeight, drawWidth * aspect);
            }
            doc.addImage(
              dataUrl,
              "PNG",
              marginX,
              cursorY,
              drawWidth,
              drawHeight,
              undefined,
              "FAST"
            );
            cursorY += drawHeight + 12;
          });
        } else {
          doc.setFontSize(10);
          doc.text("Charts not available.", marginX, cursorY);
          cursorY += 12;
        }

        if (cursorY > doc.internal.pageSize.getHeight() - 120) {
          doc.addPage();
          cursorY = 50;
        }
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(44, 90, 160);
        doc.text("Summary", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        cursorY += 20;

        const summaryRaw: string = result?.response?.data?.insights || "";
        const summaryText =
          formatInsightsToText(summaryRaw) || "No insights provided.";
        const maxWidth = pageWidth - marginX * 2;
        const summaryLinesArray = summaryText.split("\n");
        for (const rawLine of summaryLinesArray) {
          const line = rawLine.trim();
          if (!line) {
            cursorY += 12;
            continue;
          }
          const match = /^(.*?):\s*(.*)$/.exec(line);
          if (match) {
            const label = match[1] + ":";
            const content = match[2] || "";
            doc.setFont("helvetica", "bold");
            doc.text(label, marginX, cursorY);
            const labelWidth = doc.getTextWidth(label + " ");
            doc.setFont("helvetica", "normal");
            const wrappedContent = doc.splitTextToSize(
              content,
              Math.max(10, maxWidth - labelWidth)
            );
            if (Array.isArray(wrappedContent) && wrappedContent.length > 0) {
              doc.text(wrappedContent[0], marginX + labelWidth, cursorY);
              cursorY += 14;
              for (let i = 1; i < wrappedContent.length; i++) {
                if (cursorY > doc.internal.pageSize.getHeight() - 60) {
                  doc.addPage();
                  cursorY = 50;
                }
                doc.text(wrappedContent[i], marginX + labelWidth, cursorY);
                cursorY += 14;
              }
            } else {
              cursorY += 14;
            }
            doc.setFont("helvetica", "normal");
          } else {
            doc.setFont("helvetica", "normal");
            const wrapped = doc.splitTextToSize(line, maxWidth);
            const arr = Array.isArray(wrapped) ? wrapped : [wrapped];
            for (const seg of arr) {
              if (cursorY > doc.internal.pageSize.getHeight() - 60) {
                doc.addPage();
                cursorY = 50;
              }
              doc.text(seg, marginX, cursorY);
              cursorY += 14;
            }
          }
          if (cursorY > doc.internal.pageSize.getHeight() - 60) {
            doc.addPage();
            cursorY = 50;
          }
        }

        cursorY += 20;
        const dataArr: Array<Record<string, unknown>> = Array.isArray(
          result?.rawData
        )
          ? (result.rawData as Array<Record<string, unknown>>)
          : [];
        doc.setFontSize(12);
        doc.setFont("helvetica", "bold");
        doc.setTextColor(22, 163, 74);
        doc.text("Data", marginX, cursorY);
        doc.setFont("helvetica", "normal");
        doc.setTextColor(0);
        cursorY += 20;

        if (dataArr.length > 0 && typeof dataArr[0] === "object") {
          const headers = Object.keys(dataArr[0] as Record<string, unknown>);
          const body = dataArr.map((row: Record<string, unknown>) =>
            headers.map((h) => {
              const raw = row[h as keyof typeof row];
              const value =
                typeof raw === "object" ? JSON.stringify(raw) : raw ?? "";
              const str = String(value);
              return str.length > 120 ? str.slice(0, 117) + "..." : str;
            })
          );
          (doc as unknown as { autoTable: (opts: unknown) => void }).autoTable({
            head: [headers],
            body,
            startY: cursorY,
            margin: { left: marginX, right: marginX },
            styles: { fontSize: 8 },
            headStyles: { fillColor: [44, 90, 160] },
            didDrawPage: (data: { cursor: { y: number } }) => {
              cursorY = data.cursor.y + 10;
            },
          });
        } else {
          doc.setFontSize(10);
          doc.text("No data available.", marginX, cursorY);
          cursorY += 14;
        }
      });

      const filename = `selected-report-${new Date().toISOString().split("T")[0]
        }.pdf`;
      doc.save(filename);
      setIsGlobalDownloadOpen(false);
    } catch (err) {
      console.error("Failed to generate selected PDF", err);
      alert("Could not generate the selected PDF.");
    }
  };

  if (isLoaded) {
    return (
      <div
        className="min-h-screen flex flex-col overflow-x-hidden"
        style={{ backgroundColor: "#f5f5f5" }}
      >
        {/* Header */}
        {/* <header className="bg-white shadow-sm sticky top-0 z-30" 
      style={{ borderBottom: '2px solid #e0e0e0' }}>
        <div className="" style={{ padding: '10px' }}>
          <div className="flex items-center space-x-0  ">
            <Image
              src={logo}
              alt="IAAS BI BOT Logo"
              height={35}
            />
          </div>
        </div>
      </header> */}

        {/* Header -> Only Logo + Plus Button */}
        {/* Header (only logo) */}
        <header
          className="bg-white shadow-sm sticky top-0 z-30"
          style={{ borderBottom: "2px solid #e0e0e0" }}
        >
          <div className="flex items-center px-4 py-2">
            {/* Logo */}
            <div className="flex items-center space-x-0">
              {/* <img src={logo.src} alt="IAAS BI BOT Logo" height={35}  /> */}
            </div>
          </div>
        </header>

        {/* Tabs Bar + Add Button */}
        <div
          className="flex items-center w-full px-4 py-2 bg-gray-100 gap-2"
          style={{ borderBottom: "2px solid #e0e0e0" }}
        >
          {/* Tabs */}
          <div className="flex w-full gap-2">
            {tabs.map((tab, index) => (
              <div
                key={tab.id}
                className="flex items-center justify-between"
                style={{
                  flex: "1 1 50%",
                  padding: "6px 12px",
                  height: "40px",
                  borderRadius: "6px",
                  border: "1px solid #d0d0d0",
                  background: activeTab === tab.id ? "#FFFFFF" : "#E0E0E0", // 👈 Updated!
                  cursor: "pointer",
                }}
                onClick={() => handleTabSwitch(tab.id)}
              >


                {editingTab === tab.id ? (
                  <input
                    autoFocus
                    className="tab-edit-input"
                    value={editingValue}
                    onChange={(e) => setEditingValue(e.target.value)}
                    onBlur={() => saveTabName(tab.id)}
                    onKeyDown={(e) => {
                      if (e.key === "Enter") saveTabName(tab.id);
                      if (e.key === "Escape") setEditingTab(null);
                    }}
                  />
                ) : (
                  <span
                    onDoubleClick={(e) => {
                      e.stopPropagation();
                      startEditingTab(
                        tab.id,
                        tab.name || `Untitled ${index + 1}`
                      );
                    }}
                  >
                    {tab.name || `Untitled ${index + 1}`}
                  </span>
                )}

                {tabs.length > 1 && (
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      removeTab(tab.id);
                    }}
                    style={{
                      fontSize: "14px",
                      border: "none",
                      background: "transparent",
                      cursor: "pointer",
                      marginLeft: "6px",
                    }}
                  >
                    ✕
                  </button>
                )}
              </div>
            ))}
          </div>

          {/* + Button (Now here!) */}
          <button
            onClick={addTab}
            style={{
              width: "32px",
              height: "40px",
              borderRadius: "6px",
              border: "1px solid #d0d0d0",
              background: "#f5f5f5",
              fontSize: "20px",
              cursor: "pointer",
            }}
          >
            +
          </button>
        </div>

        {/* Main Content */}
        <main className="flex-1 flex flex-col">
          {/* Input Section */}
          <div
            className={`transition-all duration-500 ease-in-out ${isLanding
              ? "flex-1 flex items-center justify-center px-6 min-h-0"
              : "px-4 py-2"
              }`}
            style={{
              marginTop: isLanding ? "0px" : "0px",
              minHeight: isLanding ? "calc(100vh - 200px)" : "auto",
              maxHeight: isLanding ? "none" : "140px",
            }}
          >
            <div
              className={`w-full ${isLanding ? "max-w-5xl mx-auto" : "max-w-7xl mx-auto"
                }`}
            >
              {responses.length === 0 && !isLoading ? (
                <div
                  className="rounded-2xl p-6"
                  style={{
                    background:
                      "linear-gradient(135deg, #fff7f7 0%, #fffaf0 25%, #f5fff7 50%, #f0f9ff 75%, #f8f5ff 100%)",
                    boxShadow:
                      "0 6px 16px rgba(59, 130, 246, 0.18), 0 12px 28px rgba(236, 72, 153, 0.16), 0 18px 44px rgba(16, 185, 129, 0.14), 0 24px 64px rgba(99, 102, 241, 0.12)",
                    minHeight: "380px",
                    marginBottom: "30px",
                  }}
                >
                  <div className="flex justify-center">
                    <p
                      className="text-4xl font-semibold uppercase"
                      style={{ color: "#2c5aa0", marginTop: "15px" }}
                    >
                      Insight as a Service
                    </p>
                  </div>
                  <div className="mb-6 text-center mt-2">
                    <h1 className="text-2xl md:text-3xl font-bold text-gray-800">
                      Data to Decisions
                    </h1>

                    <p className="text-xl text-gray-600 max-w-2xl mx-auto">
                      Discover patterns, track trends, and insights
                      effortlessly.
                    </p>
                  </div>

                  <form onSubmit={handleSubmit} className="relative">
                    <div className="flex flex-col sm:flex-row items-stretch sm:items-end gap-3 sm:gap-4 mt-5 max-w-full">
                      <div className="flex-1">
                        <div className="relative">
                          <textarea
                            ref={inputRef}
                            value={prompt}
                            onChange={(e) => setPrompt(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder="Your Thoughts, Transformed into Insights."
                            className={`w-full resize-none border-2 outline-none text-sm leading-6 rounded-lg ${isInputUsed ? "p-2 pr-16" : "p-3 pr-20"
                              } focus:ring-2 transition-all duration-200 ${hasInterimResults ? "italic" : ""
                              }`}
                            style={{
                              minHeight: isInputUsed ? "40px" : "60px",
                              borderColor: hasInterimResults
                                ? "#ff9800"
                                : "#e0e0e0",
                              backgroundColor: hasInterimResults
                                ? "#fff8f0"
                                : "#ffffff",
                              color: "#333",
                              fontSize: "14px",
                            }}
                            rows={isInputUsed ? 2 : 3}
                            onFocus={(e) => {
                              if (!hasInterimResults) {
                                e.target.style.borderColor = "#4CAF50";
                                e.target.style.boxShadow =
                                  "0 0 0 3px rgba(76, 175, 80, 0.1)";
                              }
                            }}
                            onBlur={(e) => {
                              if (!hasInterimResults) {
                                e.target.style.borderColor = "#e0e0e0";
                                e.target.style.boxShadow = "none";
                              }
                            }}
                          />
                          <div
                            className={`absolute right-4 bottom-4 flex items-center gap-3 ${isInputUsed ? "" : ""
                              }`}
                          >
                            <button
                              type="button"
                              onClick={handleSpeechRecognition}
                              title={
                                isListening
                                  ? "Stop listening"
                                  : "Start speech recognition"
                              }
                              className={`p-3 rounded-full transition-colors ${isListening
                                ? "bg-red-500 text-white"
                                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                                }`}
                            >
                              <Mic className="w-5 h-5" />
                            </button>
                            <button
                              type="submit"
                              disabled={isLoading || !prompt.trim()}
                              title="Generate"
                              className={`p-3 rounded-full transition-colors ${isLoading || !prompt.trim()
                                ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                                : "bg-green-600 text-white hover:bg-green-700"
                                }`}
                            >
                              {isLoading ? (
                                <span className="animate-pulse">⋯</span>
                              ) : (
                                <Send className="w-5 h-5" />
                              )}
                            </button>
                          </div>
                        </div>
                        {/* Quick queries row with pagination */}

                        <div className="mt-3 w-full overflow-hidden relative">
                          <style>
                            {`
      @keyframes marquee {
        0%   { transform: translateX(0); }
        100% { transform: translateX(-50%); }
      }
      .marquee-container:hover .marquee-content {
        animation-play-state: paused !important;
      }
      .marquee-content {
        display: flex;
        gap: 0.5rem;
      }
    `}
                          </style>

                          <div
                            className="marquee-container overflow-hidden"
                            style={{ width: "970px" }}
                          >
                            <div
                              className="marquee-content flex gap-2"
                              style={{
                                width: "max-content",
                                animation: "marquee 25s linear infinite",
                              }}
                            >
                              {quickQueries.map((q, idx) => (
                                <button
                                  key={`${q}-${idx}`}
                                  type="button"
                                  className="px-3 py-1.5 text-xs rounded-full border border-blue-200 text-blue-700 bg-blue-50 hover:bg-blue-100 hover:border-blue-300 transition whitespace-nowrap"
                                  onClick={(e) => {
                                    e.preventDefault();
                                    const fakeEvent = {
                                      preventDefault: () => { },
                                    } as unknown as React.FormEvent;
                                    handleSubmit(fakeEvent, q);
                                  }}
                                  title={q}
                                >
                                  <span
                                    className="inline-block w-2 h-2 rounded-full mr-2"
                                    style={{
                                      backgroundColor:
                                        quickQueryColors[
                                        idx % quickQueryColors.length
                                        ],
                                    }}
                                  />
                                  {q}
                                </button>
                              ))}

                              {quickQueries.map((q, idx) => (
                                <button
                                  key={`dup-${q}-${idx}`}
                                  type="button"
                                  className="px-3 py-1.5 text-xs rounded-full border border-blue-200 text-blue-700 bg-blue-50 hover:bg-blue-100 hover:border-blue-300 transition whitespace-nowrap"
                                  onClick={(e) => {
                                    e.preventDefault();
                                    const fakeEvent = {
                                      preventDefault: () => { },
                                    } as unknown as React.FormEvent;
                                    handleSubmit(fakeEvent, q);
                                  }}
                                >
                                  <span
                                    className="inline-block w-2 h-2 rounded-full mr-2"
                                    style={{
                                      backgroundColor:
                                        quickQueryColors[
                                        idx % quickQueryColors.length
                                        ],
                                    }}
                                  />
                                  {q}
                                </button>
                              ))}
                            </div>
                          </div>
                        </div>

                        {isListening && (
                          <div
                            className="flex items-center gap-2 mt-3 p-3 rounded-lg"
                            style={{
                              backgroundColor: "rgba(255, 255, 255, 0.15)",
                              borderLeft: "4px solid #e74c3c",
                            }}
                          >
                            <span className="text-red-500 animate-pulse text-sm">
                              🔴
                            </span>
                            <div className="flex-1">
                              <span
                                className="text-white text-sm font-medium"
                                style={{
                                  textShadow: "0 1px 2px rgba(0,0,0,0.3)",
                                }}
                              >
                                {hasInterimResults
                                  ? "Live transcribing..."
                                  : "Listening..."}
                              </span>
                              {hasInterimResults && (
                                <div
                                  className="text-white text-xs opacity-80 mt-1"
                                  style={{
                                    textShadow: "0 1px 2px rgba(0,0,0,0.3)",
                                  }}
                                >
                                  Text updates as you speak
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </form>
                </div>
              ) : (
                <form onSubmit={handleSubmit} className="relative">
                  <div
                    className={`rounded-2xl shadow-lg ${isInputUsed ? "p-3" : "p-6"
                      }`}
                    style={{
                      background: "linear-gradient(135deg, #eff6ff, #dbeafe)",
                      border: "2px solid #dbeafe",
                    }}
                  >
                    <div className="flex flex-col sm:flex-row items-stretch sm:items-end gap-3 sm:gap-4 max-w-full">
                      <div className="flex-1">
                        <div className="relative">
                          <textarea
                            ref={inputRef}
                            value={prompt}
                            onChange={(e) => setPrompt(e.target.value)}
                            onKeyDown={handleKeyDown}
                            placeholder="Your Thoughts, Transformed into Insights."
                            className={`w-full resize-none border-2 outline-none text-sm leading-5 rounded-lg ${isInputUsed ? "p-1.5 pr-12" : "p-2 pr-14"
                              } focus:ring-2 transition-all duration-200 ${hasInterimResults ? "italic" : ""
                              }`}
                            style={{
                              minHeight: isInputUsed ? "48px" : "40px", // ↓ smaller than before
                              borderColor: hasInterimResults
                                ? "#ff9800"
                                : "#e0e0e0",
                              backgroundColor: hasInterimResults
                                ? "#fff8f0"
                                : "#ffffff",
                              color: "#333",
                              fontSize: "13px", // ↓ slightly smaller text
                            }}
                            rows={isInputUsed ? 1 : 2} // ↓ fewer rows
                            onFocus={(e) => {
                              if (!hasInterimResults) {
                                e.target.style.borderColor = "#4CAF50";
                                e.target.style.boxShadow =
                                  "0 0 0 3px rgba(76, 175, 80, 0.1)";
                              }
                            }}
                            onBlur={(e) => {
                              if (!hasInterimResults) {
                                e.target.style.borderColor = "#e0e0e0";
                                e.target.style.boxShadow = "none";
                              }
                            }}
                          />
                          <div
                            className={`absolute right-3 bottom-3 flex items-center gap-2 ${isInputUsed ? "" : ""
                              }`}
                          >
                            <button
                              type="button"
                              onClick={handleSpeechRecognition}
                              title={
                                isListening
                                  ? "Stop listening"
                                  : "Start speech recognition"
                              }
                              className={`p-2.5 rounded-full transition-colors ${isListening
                                ? "bg-red-500 text-white"
                                : "bg-gray-100 text-gray-600 hover:bg-gray-200"
                                }`}
                            >
                              <Mic className="w-4 h-4" />
                            </button>
                            <button
                              type="submit"
                              disabled={isLoading || !prompt.trim()}
                              title="Generate"
                              className={`p-2.5 rounded-full transition-colors flex items-center justify-center ${isLoading || !prompt.trim()
                                ? "bg-gray-100 text-gray-400 cursor-not-allowed"
                                : "bg-green-600 text-white hover:bg-green-700"
                                }`}
                              style={{ height: "40px", minWidth: "40px" }} // fixed height
                            >
                              {isLoading ? (
                                <span className="animate-pulse text-lg leading-none">
                                  ⋯
                                </span>
                              ) : (
                                <Send className="w-4 h-4" />
                              )}
                            </button>
                          </div>
                        </div>

                        {/* Quick queries row with pagination */}
                        <div className="mt-1">
                          <div className="flex items-center gap-2 w-full">
                            <button
                              type="button"
                              className={`w-7 h-7 flex items-center justify-center text-xs rounded border ${quickPage === 0
                                ? "opacity-40 cursor-not-allowed"
                                : "hover:bg-gray-50"
                                }`}
                              onClick={(e) => {
                                e.preventDefault();
                                setQuickPage((p) => Math.max(0, p - 1));
                              }}
                              disabled={quickPage === 0}
                              title="Previous"
                            >
                              ‹
                            </button>
                            <div className="flex gap-2 flex-1 justify-center">
                              {visibleQuickQueries.map((q, localIdx) => {
                                const idx =
                                  quickPage * quickPageSize + localIdx;
                                return (
                                  <button
                                    key={`${q}-${idx}`}
                                    type="button"
                                    className="px-3 py-1.5 text-xs rounded-full border border-blue-200 text-blue-700 bg-blue-50 hover:bg-blue-100 hover:border-blue-300 transition"
                                    onClick={(e) => {
                                      e.preventDefault();
                                      const fakeEvent = {
                                        preventDefault: () => { },
                                      } as unknown as React.FormEvent;
                                      handleSubmit(fakeEvent, q);
                                    }}
                                    title={q}
                                  >
                                    <span
                                      className="inline-block w-2 h-2 rounded-full mr-2"
                                      style={{
                                        backgroundColor:
                                          quickQueryColors[
                                          idx % quickQueryColors.length
                                          ],
                                      }}
                                    />
                                    {q}
                                  </button>
                                );
                              })}
                            </div>
                            <button
                              type="button"
                              className={`w-7 h-7 flex items-center justify-center text-xs rounded border ${quickPage >= quickTotalPages - 1
                                ? "opacity-40 cursor-not-allowed"
                                : "hover:bg-gray-50"
                                }`}
                              onClick={(e) => {
                                e.preventDefault();
                                setQuickPage((p) =>
                                  Math.min(quickTotalPages - 1, p + 1)
                                );
                              }}
                              disabled={quickPage >= quickTotalPages - 1}
                              title="Next"
                            >
                              ›
                            </button>
                          </div>
                        </div>
                        {isListening && (
                          <div
                            className="flex items-center gap-2 mt-3 p-3 rounded-lg"
                            style={{
                              backgroundColor: "rgba(255, 255, 255, 0.15)",
                              borderLeft: "4px solid #e74c3c",
                            }}
                          >
                            <span className="text-red-500 animate-pulse text-sm">
                              🔴
                            </span>
                            <div className="flex-1">
                              <span
                                className="text-white text-sm font-medium"
                                style={{
                                  textShadow: "0 1px 2px rgba(0,0,0,0.3)",
                                }}
                              >
                                {hasInterimResults
                                  ? "Live transcribing..."
                                  : "Listening..."}
                              </span>
                              {hasInterimResults && (
                                <div
                                  className="text-white text-xs opacity-80 mt-1"
                                  style={{
                                    textShadow: "0 1px 2px rgba(0,0,0,0.3)",
                                  }}
                                >
                                  Text updates as you speak
                                </div>
                              )}
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </form>
              )}
            </div>
          </div>

          {/* Charts Section - Single Card with Scrollable Content */}
          {(responses.length > 0 || isLoading) && (
            <div className="w-full px-6 py-1 mt-7 ">
              <div
                className="max-w-7xl mx-auto bg-white rounded-2xl shadow-lg"
                style={{ border: "2px solid #2c5aa0" }}
              >
                {/* <div className="px-6 border-b" style={{ borderColor: "#e0e0e0", marginTop: "-2px" }}>
                  <div className="flex items-center justify-between" style={{ marginTop: "-8px" }}> */}
                <div
                  className="px-6 border-b pt-3"
                  style={{ borderColor: "#e0e0e0" }}
                >
                  <div className="flex items-center justify-between pt-1 pb-2">
                    {/* Left side - Title */}
                    <h2
                      className="text-lg font-semibold"
                      style={{ color: "#2c5aa0" }}
                    >
                      Insights
                    </h2>

                    {/* Right side - Buttons */}
                    <div className="flex items-center gap-2">
                      {/* Clear All / Clear Recent Buttons */}
                      {responses.length > 0 && (
                        <div className="flex gap-1">
                          <button
                            onClick={handleClearRecent}
                            className="px-3 py-1.5 text-sm rounded border hover:bg-gray-50 transition-colors"
                            style={{ borderColor: "#e0e0e0", color: "#666" }}
                            title="Clear recent from memory"
                          >
                            Clear Recent
                          </button>
                          <button
                            onClick={handleClearAll}
                            className="px-3 py-1.5 text-sm rounded border hover:bg-red-50 transition-colors"
                            style={{ borderColor: "#e0e0e0", color: "#dc2626" }}
                            title="Clear all responses"
                          >
                            Clear All
                          </button>
                        </div>
                      )}

                      {/* Load More Button */}
                      {hasMoreResponses && (
                        <button
                          onClick={handleLoadMore}
                          disabled={isLoadingOlder}
                          className="px-3 py-1.5 text-sm rounded border hover:bg-gray-50 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                          style={{ borderColor: "#e0e0e0", color: "#2c5aa0" }}
                          title="Load older responses"
                        >
                          {isLoadingOlder ? "Loading..." : "Load More"}
                        </button>
                      )}

                      {/* Download Button */}
                      <div
                        className="relative"
                        data-global-download-container="true"
                      >
                        <button
                          title="Download options"
                          className="hover:opacity-80 transition-opacity"
                          onClick={(e) => {
                            e.preventDefault();
                            openBulkDownload();
                          }}
                        >
                          <Image
                            src={DocumentIcon}
                            alt="Download PDF"
                            width={60}
                            height={60}
                          />
                        </button>

                        {isGlobalDownloadOpen && (
                          <div
                            className="absolute right-0 mt-2 bg-white border rounded-lg shadow-lg w-80 text-sm z-20"
                            style={{ borderColor: "#e0e0e0" }}
                          >
                            <div
                              className="px-3 py-2 border-b"
                              style={{ borderColor: "#e0e0e0" }}
                            >
                              <label className="flex items-center gap-2 cursor-pointer select-none">
                                <input
                                  type="checkbox"
                                  checked={isBulkSelectAll}
                                  onChange={(e) =>
                                    toggleSelectAllBulk(e.target.checked)
                                  }
                                  style={{ accentColor: "#2c5aa0" }}
                                />
                                <span className="font-medium">Select All</span>
                              </label>
                            </div>

                            <div className="max-h-36 overflow-auto">
                              {responses.length === 0 ? (
                                <div className="p-3 text-gray-500">
                                  No responses
                                </div>
                              ) : (
                                responses
                                  .slice()
                                  .reverse()
                                  .map((result) => {
                                    const title = formatUserInputTitle(
                                      result.response?.data?.user_input
                                    );
                                    return (
                                      <label
                                        key={`bulk-${result.resultId}`}
                                        className="flex items-start gap-2 px-3 py-2 hover:bg-gray-50 cursor-pointer"
                                      >
                                        <input
                                          type="checkbox"
                                          checked={
                                            !!bulkSelectedIds[result.resultId]
                                          }
                                          onChange={(e) =>
                                            toggleSingleBulk(
                                              result.resultId,
                                              e.target.checked
                                            )
                                          }
                                          style={{ accentColor: "#2c5aa0" }}
                                        />
                                        <div>
                                          <p
                                            className="text-sm font-medium"
                                            style={{ color: "#2c5aa0" }}
                                          >
                                            {title}
                                          </p>
                                        </div>
                                      </label>
                                    );
                                  })
                              )}
                            </div>

                            <div
                              className="flex gap-2 p-3 border-t"
                              style={{ borderColor: "#e0e0e0" }}
                            >
                              <button
                                className="flex-1 px-3 py-2 text-sm rounded border hover:bg-gray-50"
                                onClick={(e) => {
                                  e.preventDefault();
                                  setIsGlobalDownloadOpen(false);
                                }}
                              >
                                Cancel
                              </button>
                              <button
                                className="flex-1 px-3 py-2 text-sm rounded text-white hover:opacity-90"
                                style={{ backgroundColor: "#2c5aa0" }}
                                onClick={(e) => {
                                  e.preventDefault();
                                  handleDownloadSelectedResponses();
                                }}
                              >
                                Download
                              </button>
                            </div>
                          </div>
                        )}
                      </div>
                    </div>
                  </div>
                </div>

                <div
                  className="p-2 space-y-8 max-h-[60vh] overflow-y-auto"
                  ref={scrollContainerRef}
                >
                  {/* Existing Chats - Newest Last (at bottom) */}
                  {responses.slice().map((result, resultIndex) => {
                    const charts = generateChartsFromData(
                      result.rawData,
                      result.chartTypes as { [key: number]: ChartData["type"] },
                      result.response,
                      resultIndex,
                      axisSelections[result.resultId]
                    );
                    const sqlContentRaw = (
                      (
                        result.response?.data as { sql?: string } | undefined
                      )?.sql?.replace(/^sql\s*\n?/, "") || ""
                    ).trim();
                    const sqlContentDisplay =
                      sqlContentRaw || "No SQL available.";

                    return (
                      <div key={result.resultId} className="fade-in-up">
                        {/* User Input Display - Top Center */}
                        <div className="text-center mb-1 -mt-3">
                          <p
                            className="text-lg font-semibold"
                            style={{ color: "#2c5aa0" }}
                          >
                            {result.response?.data?.user_input
                              ? result.response.data.user_input
                                .toLowerCase()
                                .replace(/\b\w/g, (char) =>
                                  char.toUpperCase()
                                )
                              : "No Query Information Available"}
                          </p>
                        </div>

                        {/* Main Content Grid */}
                        {result.response?.show == true ? (
                          <>
                            <div className="grid grid-cols-1 md:grid-cols-[1fr_auto] gap-2 items-stretch">
                              {/* Combined Left + Center: Single Box with Summary and Charts */}
                              <div
                                className="relative h-full"
                                data-result-id={result.resultId}
                              >
                                {/* kebab menu removed */}
                                <div
                                  className="bg-white rounded-2xl shadow-sm p-6 h-full"
                                  style={{ border: "2px solid #e0e0e0" }}
                                >
                                  <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                                    {/* Summary (Left) */}
                                    <div>
                                      <div
                                        className="text-sm text-gray-700"
                                        dangerouslySetInnerHTML={{
                                          __html: (
                                            result.response?.data?.insights ||
                                            "No insights provided."
                                          )
                                            // Replace "- " with line breaks
                                            .replace(/- /g, "<br/>")
                                            // Bold + blue for **Plan Name**
                                            .replace(
                                              /\*\*(.*?)\*\*/g,
                                              '<span style="color:blue; font-weight:bold">$1</span>'
                                            )
                                            // Blue + bold for *churned_users* (single star)
                                            .replace(
                                              /\*(.*?)\*/g,
                                              '<span style="color:blue; font-weight:bold">$1</span>'
                                            )
                                            // Italic for `column_name`
                                            .replace(/`(.*?)`/g, "<i>$1</i>"),
                                        }}
                                      />
                                    </div>

                                    {/* Charts (Center) or Table */}
                                    <div className="flex flex-col gap-4">
                                      {showTableInChat === result.resultId ? (
                                        /* Table Display */
                                        <div className="h-full">
                                          {/* <div className="mb-4">
                                        <h3
                                          className="text-lg font-semibold"
                                          style={{ color: "#2c5aa0" }}
                                        >
                                          Data Table
                                        </h3>
                                      </div> */}
                                          <div className="overflow-auto max-h-80">
                                            {Array.isArray(result.rawData) &&
                                              result.rawData.length > 0 &&
                                              typeof result.rawData[0] ===
                                              "object" ? (
                                              (() => {
                                                const typedRows =
                                                  result.rawData as Array<
                                                    Record<string, unknown>
                                                  >;
                                                const firstRow =
                                                  typedRows[0] as Record<
                                                    string,
                                                    unknown
                                                  >;
                                                const headers =
                                                  Object.keys(firstRow);
                                                const filteredHeaders: string[] =
                                                  headers;
                                                return (
                                                  <table className="min-w-full text-sm border border-gray-300">
                                                    <thead className="bg-gray-50 sticky top-0 z-10">
                                                      <tr>
                                                        {filteredHeaders.map(
                                                          (key) => (
                                                            <th
                                                              key={key}
                                                              className="text-left font-semibold px-3 py-2 text-gray-600 tracking-wide border-l border-r border-gray-300"
                                                            >
                                                              {key
                                                                .replace(
                                                                  /_/g,
                                                                  " "
                                                                ) // replace underscores with spaces
                                                                .replace(
                                                                  /\b\w/g,
                                                                  (c) =>
                                                                    c.toUpperCase()
                                                                )}{" "}
                                                              {/* capitalize each word */}
                                                            </th>
                                                          )
                                                        )}
                                                      </tr>
                                                    </thead>
                                                    <tbody>
                                                      {typedRows.map(
                                                        (row, idx) => (
                                                          <tr
                                                            key={idx}
                                                            className={
                                                              idx % 2 === 0
                                                                ? "bg-white"
                                                                : "bg-gray-50"
                                                            }
                                                          >
                                                            {filteredHeaders.map(
                                                              (key) => (
                                                                <td
                                                                  key={key}
                                                                  className="px-3 py-2 whitespace-nowrap text-gray-700 border-l border-r border-gray-300"
                                                                >
                                                                  {typeof row[
                                                                    key
                                                                  ] === "object"
                                                                    ? JSON.stringify(
                                                                      row[key]
                                                                    )
                                                                    : String(
                                                                      row[
                                                                      key
                                                                      ] ?? ""
                                                                    )}
                                                                </td>
                                                              )
                                                            )}
                                                          </tr>
                                                        )
                                                      )}
                                                    </tbody>
                                                  </table>
                                                );
                                              })()
                                            ) : (
                                              <div className="p-6 text-center text-gray-500">
                                                No data available
                                              </div>
                                            )}
                                          </div>
                                        </div>
                                      ) : (
                                        /* Charts Display */

                                        // charts.map((chartData: ChartData, chartIndex: number) => (
                                        //   <div key={chartIndex}>
                                        //     <ChartRenderer
                                        //       chartData={{
                                        //         ...chartData,
                                        //         type: (result.chartTypes[chartIndex] || chartData.type) as ChartData['type'],
                                        //       }}
                                        //     />
                                        //   </div>
                                        // ))
                                        /* Charts Display */
                                        charts.map(
                                          (
                                            chartData: ChartData,
                                            chartIndex: number
                                          ) => {
                                            const axisSel =
                                              axisSelections[result.resultId] ||
                                              {};
                                            const yKey = axisSel.y;

                                            const rows = normalizeRows(
                                              result.rawData,
                                              result.response?.data
                                            );
                                            let finalYKey = yKey;

                                            if (
                                              !finalYKey ||
                                              !(finalYKey in (rows[0] || {}))
                                            ) {
                                              finalYKey = Object.keys(
                                                rows[0] || {}
                                              ).find((k) => {
                                                const v = (
                                                  rows[0] as Record<
                                                    string,
                                                    unknown
                                                  >
                                                )[k];
                                                return typeof v === "number";
                                              });
                                            }
                                            // Extract numeric values for selected Y-axis
                                            const numericValues =
                                              finalYKey && rows.length > 0
                                                ? rows
                                                  .map((row) => {
                                                    const value =
                                                      row[
                                                      finalYKey as keyof typeof row
                                                      ];
                                                    return typeof value ===
                                                      "number"
                                                      ? value
                                                      : Number(value);
                                                  })
                                                  .filter((v) => !isNaN(v))
                                                : [];

                                            // Calculate statistics
                                            const minValue =
                                              numericValues.length > 0
                                                ? Math.min(...numericValues)
                                                : null;
                                            const maxValue =
                                              numericValues.length > 0
                                                ? Math.max(...numericValues)
                                                : null;
                                            const avgValue =
                                              numericValues.length > 0
                                                ? numericValues.reduce(
                                                  (a, b) => a + b,
                                                  0
                                                ) / numericValues.length
                                                : null;

                                            return (
                                              <div
                                                key={chartIndex}
                                                className="flex flex-col gap-2"
                                              >
                                                {/* Chart */}
                                                <ChartRenderer
                                                  chartData={{
                                                    ...chartData,
                                                    type: (result.chartTypes[
                                                      chartIndex
                                                    ] ||
                                                      chartData.type) as ChartData["type"],
                                                  }}
                                                />

                                                {/* Stat Buttons */}
                                                <div className="flex gap-3 justify-center mt-2">
                                                  <button
                                                    onClick={() =>
                                                      setStatResults(
                                                        (prev) => ({
                                                          ...prev,
                                                          [result.resultId]: `Minimum Value: ${minValue !== null
                                                            ? minValue
                                                            : "N/A"
                                                            }`,
                                                        })
                                                      )
                                                    }
                                                    className="px-3 py-1 bg-blue-100 text-blue-700 rounded-md text-xs font-semibold hover:bg-blue-200"
                                                  >
                                                    Min
                                                  </button>

                                                  <button
                                                    onClick={() =>
                                                      setStatResults(
                                                        (prev) => ({
                                                          ...prev,
                                                          [result.resultId]: `Maximum Value: ${maxValue !== null
                                                            ? maxValue
                                                            : "N/A"
                                                            }`,
                                                        })
                                                      )
                                                    }
                                                    className="px-3 py-1 bg-green-100 text-green-700 rounded-md text-xs font-semibold hover:bg-green-200"
                                                  >
                                                    Max
                                                  </button>

                                                  <button
                                                    onClick={() =>
                                                      setStatResults(
                                                        (prev) => ({
                                                          ...prev,
                                                          [result.resultId]: `Average Value: ${avgValue !== null
                                                            ? avgValue.toFixed(
                                                              2
                                                            )
                                                            : "N/A"
                                                            }`,
                                                        })
                                                      )
                                                    }
                                                    className="px-3 py-1 bg-yellow-100 text-yellow-700 rounded-md text-xs font-semibold hover:bg-yellow-200"
                                                  >
                                                    Avg
                                                  </button>
                                                </div>

                                                {/* Result Display */}
                                                {statResults && (
                                                  <div className="text-center text-sm font-medium text-gray-700 mt-1">
                                                    {
                                                      statResults[
                                                      result.resultId
                                                      ]
                                                    }
                                                  </div>
                                                )}
                                              </div>
                                            );
                                          }
                                        )
                                      )}
                                    </div>
                                  </div>
                                </div>
                                {/* Close outer relative wrapper */}
                              </div>
                              {/* Rightmost icons card (third column) */}
                              <div className="self-stretch h-full">
                                <div
                                  className="bg-white rounded-2xl border shadow-sm p-0 relative h-full flex flex-col justify-start"
                                  style={{ borderColor: "#e0e0e0" }}
                                  data-chart-menu-container="true"
                                >
                                  <div className="flex flex-col items-stretch justify-start text-base p-3 gap-3 w-48">
                                    {/* Delete */}
                                    <button
                                      title="Delete"
                                      className="w-full text-left px-4 py-2 rounded-lg border border-red-200 bg-white text-red-600 shadow-sm hover:bg-red-50 transition-colors"
                                      onClick={() => {
                                        setOpenMenuResultId(null);
                                        setOpenDownloadResultId(null);
                                        handleCloseResult(result.resultId);
                                      }}
                                    >
                                      Delete
                                    </button>

                                    {/* Visualization (label + dropdown) */}

                                    <div className="border border-blue-400 rounded-lg p-3 bg-white shadow-sm space-y-3 hover:bg-blue-50 transition-colors">
                                      <div>
                                        <div className="text-sm font-bold mb-2">
                                          Visualization
                                        </div>
                                        {(() => {
                                          const chartsForCount =
                                            generateChartsFromData(
                                              result.rawData,
                                              result.chartTypes as {
                                                [
                                                key: number
                                                ]: ChartData["type"];
                                              },
                                              result.response,
                                              resultIndex,
                                              axisSelections[result.resultId]
                                            );
                                          const chartsCount =
                                            chartsForCount.length;
                                          const inferredType =
                                            chartsForCount[0]?.type;
                                          const currentType =
                                            (result.chartTypes &&
                                              result.chartTypes[0]) ||
                                            inferredType ||
                                            "bar";
                                          return (
                                            <select
                                              className="w-full border border-gray-300 rounded-lg px-2 py-1 text-xs text-gray-700 bg-white shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-200"
                                              value={
                                                showTableInChat ===
                                                  result.resultId
                                                  ? "table"
                                                  : currentType
                                              }
                                              onChange={(e) => {
                                                const sel = e.target.value;
                                                if (sel === "table") {
                                                  const willShow =
                                                    showTableInChat !==
                                                    result.resultId;
                                                  setShowTableInChat(
                                                    willShow
                                                      ? result.resultId
                                                      : null
                                                  );
                                                  return;
                                                }
                                                // switching chart type hides table if open
                                                if (
                                                  showTableInChat ===
                                                  result.resultId
                                                ) {
                                                  setShowTableInChat(null);
                                                }
                                                handleChartTypeChangeAll(
                                                  result.resultId,
                                                  chartsCount,
                                                  sel as ChartData["type"]
                                                );
                                              }}
                                            >
                                              <option value="table">
                                                🗂️ Table
                                              </option>
                                              {chartTypeOptions.map(
                                                (option) => (
                                                  <option
                                                    key={option.value}
                                                    value={option.value}
                                                    title={option.description}
                                                  >
                                                    {`${option.icon} ${option.label}`}
                                                  </option>
                                                )
                                              )}
                                            </select>
                                          );
                                        })()}
                                      </div>

                                      {/* Axis Dropdowns */}
                                      {Array.isArray(result.rawData) &&
                                        result.rawData.length > 0 &&
                                        typeof result.rawData[0] === "object" &&
                                        (() => {
                                          const typedRows =
                                            result.rawData as Array<
                                              Record<string, unknown>
                                            >;
                                          const sampleRow =
                                            typedRows[0] as Record<
                                              string,
                                              unknown
                                            >;
                                          const allKeys =
                                            Object.keys(sampleRow);
                                          const stringKeys = allKeys.filter(
                                            (k) =>
                                              typeof sampleRow[k] === "string"
                                          );
                                          const numericKeys = allKeys.filter(
                                            (k) =>
                                              typeof sampleRow[k] === "number"
                                          );
                                          const sel =
                                            axisSelections[result.resultId] ||
                                            {};
                                          // Prefer intuitive defaults, but allow any field on either axis
                                          const defaultX =
                                            (allKeys.includes("series_name") &&
                                              "series_name") ||
                                            (allKeys.includes(
                                              "season_number"
                                            ) &&
                                              "season_number") ||
                                            (allKeys.includes(
                                              "episode_number"
                                            ) &&
                                              "episode_number") ||
                                            stringKeys[0] ||
                                            allKeys[0] ||
                                            "";
                                          const defaultY =
                                            (allKeys.includes(
                                              "total_unique_viewers"
                                            ) &&
                                              "total_unique_viewers") ||
                                            (allKeys.includes(
                                              "season_number"
                                            ) &&
                                              "season_number") ||
                                            (allKeys.includes(
                                              "episode_number"
                                            ) &&
                                              "episode_number") ||
                                            numericKeys[0] ||
                                            allKeys.find(
                                              (k) => k !== defaultX
                                            ) ||
                                            "";

                                          return (
                                            <>
                                              {/* X Axis Dropdown */}
                                              <div className="w-full">
                                                <label className="block text-gray-700 text-sm font-bold mb-1">
                                                  X axis
                                                </label>
                                                <select
                                                  className="w-full border border-gray-300 rounded-lg px-2 py-1 text-xs text-gray-700 bg-white shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-200"
                                                  value={sel.x ?? defaultX}
                                                  onChange={(e) =>
                                                    setAxisSelections(
                                                      (
                                                        prev: Record<
                                                          string,
                                                          {
                                                            x?: string;
                                                            y?: string;
                                                          }
                                                        >
                                                      ) => ({
                                                        ...prev,
                                                        [result.resultId]: {
                                                          ...(prev[
                                                            result.resultId
                                                          ] || {}),
                                                          x:
                                                            e.target.value ||
                                                            undefined,
                                                        },
                                                      })
                                                    )
                                                  }
                                                >
                                                  {allKeys.map((k) => (
                                                    <option key={k} value={k}>
                                                      {k}
                                                    </option>
                                                  ))}
                                                </select>
                                              </div>

                                              {/* Y Axis Dropdown */}
                                              <div className="w-full">
                                                <label className="block text-gray-700 text-sm font-bold mb-1">
                                                  Y axis
                                                </label>
                                                <select
                                                  className="w-full border border-gray-300 rounded-lg px-2 py-1 text-xs text-gray-700 bg-white shadow-sm focus:outline-none focus:ring-2 focus:ring-blue-200"
                                                  value={sel.y ?? defaultY}
                                                  onChange={(e) =>
                                                    setAxisSelections(
                                                      (
                                                        prev: Record<
                                                          string,
                                                          {
                                                            x?: string;
                                                            y?: string;
                                                          }
                                                        >
                                                      ) => ({
                                                        ...prev,
                                                        [result.resultId]: {
                                                          ...(prev[
                                                            result.resultId
                                                          ] || {}),
                                                          y:
                                                            e.target.value ||
                                                            undefined,
                                                        },
                                                      })
                                                    )
                                                  }
                                                >
                                                  {allKeys.map((k) => (
                                                    <option key={k} value={k}>
                                                      {k}
                                                    </option>
                                                  ))}
                                                </select>
                                              </div>
                                            </>
                                          );
                                        })()}
                                    </div>

                                    <button
                                      title="sql"
                                      className="w-full text-left px-4 py-2 rounded-lg border border-yellow-400 bg-white text-yellow-600 shadow-sm hover:bg-yellow-50 transition-colors"
                                      onClick={() => {
                                        const willOpen =
                                          openSqlResultId !== result.resultId;
                                        setOpenSqlResultId(
                                          willOpen ? result.resultId : null
                                        );
                                        setOpenMenuResultId(null);
                                        setOpenDownloadResultId(null);
                                      }}
                                    >
                                      SQL
                                    </button>

                                    {/* Download */}
                                    <button
                                      title="Download"
                                      className="w-full text-left px-4 py-2 rounded-lg border border-green-500 bg-white text-green-600 shadow-sm hover:bg-green-50 transition-colors"
                                      onClick={() => {
                                        const willOpen =
                                          openDownloadResultId !==
                                          result.resultId;
                                        setOpenDownloadResultId(
                                          willOpen ? result.resultId : null
                                        );
                                        setOpenMenuResultId(null);
                                      }}
                                    >
                                      Download
                                    </button>
                                  </div>

                                  {/* Visualization Popup */}
                                  {openMenuResultId === result.resultId && (
                                    <div
                                      className="absolute right-full mr-3 top-0 bg-white border rounded-lg shadow-lg w-44 text-sm z-10"
                                      style={{ borderColor: "#e0e0e0" }}
                                    >
                                      <div className="max-h-60 overflow-auto py-2">
                                        <button
                                          type="button"
                                          className="w-full text-left px-3 py-2 hover:bg-gray-50 flex items-center gap-2 text-gray-700"
                                          onClick={() => {
                                            const willShow =
                                              showTableInChat !==
                                              result.resultId;
                                            setShowTableInChat(
                                              willShow ? result.resultId : null
                                            );
                                            setOpenMenuResultId(null);
                                            setOpenDownloadResultId(null);
                                          }}
                                        >
                                          <span className="w-4 h-4 flex items-center justify-center">
                                            🗂️
                                          </span>
                                          <span>Table</span>
                                        </button>
                                        {(() => {
                                          const chartsForCount =
                                            generateChartsFromData(
                                              result.rawData,
                                              result.chartTypes as {
                                                [
                                                key: number
                                                ]: ChartData["type"];
                                              },
                                              result.response,
                                              resultIndex,
                                              axisSelections[result.resultId]
                                            );
                                          const chartsCount =
                                            chartsForCount.length;
                                          return chartTypeOptions.map(
                                            (option) => (
                                              <button
                                                type="button"
                                                key={option.value}
                                                className="w-full text-left px-3 py-2 hover:bg-gray-50 flex items-center gap-2"
                                                title={option.description}
                                                onClick={() => {
                                                  handleChartTypeChangeAll(
                                                    result.resultId,
                                                    chartsCount,
                                                    option.value as ChartData["type"]
                                                  );
                                                  setOpenMenuResultId(null);
                                                }}
                                              >
                                                <span>{option.icon}</span>
                                                <span className="text-gray-700">
                                                  {option.label}
                                                </span>
                                              </button>
                                            )
                                          );
                                        })()}
                                      </div>
                                    </div>
                                  )}

                                  {/* Download Popup */}
                                  {openSqlResultId === result.resultId && (
                                    <div
                                      className="absolute right-full mr-3 top-0 bg-white border rounded-lg shadow-lg w-96 text-sm z-10"
                                      style={{ borderColor: "#e0e0e0" }}
                                      data-sql-menu-container="true"
                                    >
                                      <div className="flex items-center justify-between px-3 py-2 border-b text-gray-700 border border-yellow-400 bg-yellow-50 rounded-t-lg shadow-sm">
                                        <span className="font-medium text-yellow-700">
                                          SQL
                                        </span>
                                        <div className="flex items-center gap-2 relative">
                                          <button
                                            type="button"
                                            className="text-yellow-600 hover:underline relative"
                                            onClick={() => {
                                              if (sqlContentRaw) {
                                                const formattedSql = `-- sql\n${sqlContentRaw}`;

                                                navigator.clipboard
                                                  ?.writeText(formattedSql)
                                                  .then(() => {
                                                    setCopiedId(
                                                      result.resultId
                                                    );
                                                    setTimeout(
                                                      () => setCopiedId(null),
                                                      1500
                                                    ); // hide tooltip after 1.5s
                                                  })
                                                  .catch(() => { });
                                              }
                                            }}
                                          >
                                            Copy
                                          </button>

                                          {/* Tooltip */}
                                          {copiedId === result.resultId && (
                                            <div className="absolute -top-6 left-1/2 transform -translate-x-1/2 bg-yellow-500 text-white text-xs rounded py-1 px-2 shadow">
                                              Copied!
                                            </div>
                                          )}

                                          <button
                                            type="button"
                                            className="text-yellow-600 hover:text-yellow-800"
                                            onClick={() =>
                                              setOpenSqlResultId(null)
                                            }
                                            aria-label="Close SQL"
                                          >
                                            ✕
                                          </button>
                                        </div>
                                      </div>

                                      <div className="max-h-80 overflow-auto p-3">
                                        <pre className="language-sql whitespace-pre-wrap text-xs text-gray-800 rounded-lg border border-yellow-200 bg-yellow-50/40 px-3 py-2">
                                          <code className="language-sql">
                                            {sqlContentDisplay}
                                          </code>
                                        </pre>
                                      </div>
                                    </div>
                                  )}

                                  {/* Download Popup */}
                                  {openDownloadResultId === result.resultId && (
                                    <div
                                      className="absolute right-full mr-3 top-0 bg-white border rounded-lg shadow-lg w-44 text-sm z-10"
                                      style={{ borderColor: "#e0e0e0" }}
                                    >
                                      <div className="max-h-60 overflow-auto py-2">
                                        <button
                                          type="button"
                                          className="w-full flex items-center text-left px-3 py-2 hover:bg-gray-50"
                                          onClick={() => {
                                            handleDownloadSummary(result);
                                            setOpenDownloadResultId(null);
                                          }}
                                        >
                                          <span className="mr-2">📄</span>{" "}
                                          Download summary
                                        </button>

                                        <button
                                          type="button"
                                          className="w-full flex items-center text-left px-3 py-2 hover:bg-gray-50"
                                          onClick={() => {
                                            handleDownloadData(result);
                                            setOpenDownloadResultId(null);
                                          }}
                                        >
                                          <span className="mr-2">📊</span>{" "}
                                          Download data
                                        </button>

                                        <button
                                          type="button"
                                          className="w-full flex items-center text-left px-3 py-2 hover:bg-gray-50"
                                          onClick={() => {
                                            handleDownloadAll(result);
                                            setOpenDownloadResultId(null);
                                          }}
                                        >
                                          <span className="mr-2">📦</span>{" "}
                                          Download all
                                        </button>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              </div>
                            </div>
                          </>
                        ) : (
                          ""
                        )}
                      </div>
                    );
                  })}

                  {/* Loading Skeleton */}
                  {isLoading && (
                    <div className="fade-in-up">
                      <div
                        className="bg-white rounded-2xl shadow-sm "
                        style={{ border: "2px solid #e0e0e0" }}
                      >
                        <div className="p-6">
                          <div className="space-y-4">
                            <div className="h-5 w-32 bg-gray-200 rounded mx-auto animate-pulse"></div>
                            <div className="h-48 bg-gray-100 rounded animate-pulse flex items-center justify-center">
                              <div className="text-center">
                                <div
                                  className="animate-spin rounded-full h-8 w-8 border-b-2 mx-auto mb-2"
                                  style={{ borderColor: "#2c5aa0" }}
                                ></div>
                                <p className="text-lg text-gray-500">
                                  &quot;Generating Insights&quot;
                                </p>
                                {/* Random Quote each time */}
                                <p className="mt-2 text-base italic text-gray-400">
                                  {randomQuote}
                                </p>
                              </div>
                            </div>
                          </div>
                        </div>
                      </div>
                    </div>
                  )}
                </div>
              </div>
            </div>
          )}

          {/* Error Dialog (MUI) */}
          <Dialog
            open={isErrorOpen}
            onClose={() => {
              setIsErrorOpen(false);
              setErrorMessage("");
              setDialogApiData({ user_input: "", chat_response: "" });
            }}
            maxWidth="xs"
            fullWidth
            PaperProps={{
              sx: {
                borderRadius: "12px",
                border: "1px solid #E53E3E", // thin red border
                overflow: "hidden",
                p: 0,
              },
            }}
          >
            <DialogContent sx={{ textAlign: "center", p: 2.5 }}>
              <Box display="flex" gap={1} mb={1}>
                <Typography sx={{ fontSize: "15px", color: "#2D3748" }}>
                  <strong>User Input:</strong>{" "}
                  {dialogApiData.user_input ||
                    latestApiData.user_input ||
                    "Device error"}
                </Typography>
              </Box>

              <p
                style={{
                  fontSize: "15px",
                  lineHeight: 1.4,
                  color: "#2d3748",
                  margin: 0,
                  textAlign: "justify",
                }}
              >
                <strong>Chat Response:</strong>{" "}
                {dialogApiData.chat_response ||
                  latestApiData.chat_response ||
                  errorMessage ||
                  latestErrorMessage}
              </p>
            </DialogContent>

            {/* Close Button */}
            <DialogActions sx={{ justifyContent: "center", pb: 2 }}>
              <Button
                onClick={() => {
                  setIsErrorOpen(false);
                  setErrorMessage("");
                }}
                variant="contained"
                sx={{
                  backgroundColor: "#2B6CB0",
                  "&:hover": { backgroundColor: "#2c5282" },
                  textTransform: "none",
                  borderRadius: "6px",
                  px: 3,
                  fontSize: "14px",
                  fontWeight: 500,
                }}
              >
                Close
              </Button>
            </DialogActions>
          </Dialog>
        </main>

      </div>
    );
  }
};

export default ChatInterface;
