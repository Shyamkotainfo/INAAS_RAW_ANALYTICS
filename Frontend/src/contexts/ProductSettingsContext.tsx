"use client";

import { createContext, useContext, useState, useEffect, ReactNode } from "react";

interface ProductConfig {
  enabled: boolean;
  locked: boolean;
}

interface ProductSettingsContextType {
  productConfigs: Record<string, ProductConfig>;
  toggleEnabled: (productPath: string) => void;
  toggleLocked: (productPath: string) => void;
  isProductEnabled: (productPath: string) => boolean;
  isProductLocked: (productPath: string) => boolean;
  isProductVisible: (productPath: string) => boolean;
}

const ProductSettingsContext = createContext<
  ProductSettingsContextType | undefined
>(undefined);

const DEFAULT_CONFIG: ProductConfig = { enabled: true, locked: false };

const DEFAULT_PRODUCTS: Record<string, ProductConfig> = {
  "/catalog": { enabled: true, locked: false },
  "/lineage": { enabled: true, locked: false },
  "/insights": { enabled: true, locked: false },
  "/cost-insights": { enabled: true, locked: false },
  "/consumption": { enabled: true, locked: false },
  "/quality": { enabled: true, locked: false },
  "/security": { enabled: true, locked: false },
  "/observability": { enabled: true, locked: false },
  "/access": { enabled: true, locked: false },
  "/misc-tools": { enabled: true, locked: false },
};

export function ProductSettingsProvider({ children }: { children: ReactNode }) {
  const [productConfigs, setProductConfigs] =
    useState<Record<string, ProductConfig>>(DEFAULT_PRODUCTS);

  // ✅ Load from localStorage (client only)
  useEffect(() => {
    try {
      const stored = localStorage.getItem("product_configs");
      if (stored) {
        setProductConfigs(JSON.parse(stored));
        return;
      }

      // ---- Migration logic ----
      const oldStates = localStorage.getItem("product_states");
      if (oldStates) {
        const data = JSON.parse(oldStates);
        const migrated: Record<string, ProductConfig> = {};

        Object.keys(data).forEach((key) => {
          if (typeof data[key] === "string") {
            migrated[key] = {
              enabled: data[key] === "enabled" || data[key] === "locked",
              locked: data[key] === "locked",
            };
          }
        });

        setProductConfigs(migrated);
        localStorage.setItem("product_configs", JSON.stringify(migrated));
        localStorage.removeItem("product_states");
        return;
      }

      const oldEnabled = localStorage.getItem("enabled_products");
      if (oldEnabled) {
        const data = JSON.parse(oldEnabled);
        const migrated: Record<string, ProductConfig> = {};

        Object.keys(data).forEach((key) => {
          migrated[key] = { enabled: data[key], locked: false };
        });

        setProductConfigs(migrated);
        localStorage.setItem("product_configs", JSON.stringify(migrated));
        localStorage.removeItem("enabled_products");
      }
    } catch (err) {
      console.error("Failed to load product configs", err);
    }
  }, []);

  // ✅ Persist to localStorage whenever state changes
  useEffect(() => {
    try {
      localStorage.setItem("product_configs", JSON.stringify(productConfigs));
    } catch (err) {
      console.error("Failed to save product configs", err);
    }
  }, [productConfigs]);

  const updateConfig = (
    productPath: string,
    updates: Partial<ProductConfig>,
  ) => {
    setProductConfigs((prev) => {
      const current = prev[productPath] ?? DEFAULT_CONFIG;
      return { ...prev, [productPath]: { ...current, ...updates } };
    });
  };

  const toggleEnabled = (productPath: string) => {
    const current = productConfigs[productPath] ?? DEFAULT_CONFIG;
    updateConfig(productPath, { enabled: !current.enabled });
  };

  const toggleLocked = (productPath: string) => {
    const current = productConfigs[productPath] ?? DEFAULT_CONFIG;
    updateConfig(productPath, { locked: !current.locked });
  };

  const isProductEnabled = (productPath: string): boolean =>
    (productConfigs[productPath] ?? DEFAULT_CONFIG).enabled;

  const isProductLocked = (productPath: string): boolean =>
    (productConfigs[productPath] ?? DEFAULT_CONFIG).locked;

  const isProductVisible = (productPath: string): boolean => {
    const config = productConfigs[productPath] ?? DEFAULT_CONFIG;
    return config.enabled || config.locked;
  };

  return (
    <ProductSettingsContext.Provider
      value={{
        productConfigs,
        toggleEnabled,
        toggleLocked,
        isProductEnabled,
        isProductLocked,
        isProductVisible,
      }}
    >
      {children}
    </ProductSettingsContext.Provider>
  );
}

export function useProductSettings() {
  const context = useContext(ProductSettingsContext);
  if (!context) {
    throw new Error(
      "useProductSettings must be used within a ProductSettingsProvider",
    );
  }
  return context;
}