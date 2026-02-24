// Product settings context for managing sidebar product visibility and lock states
import { createContext, useContext, useState, ReactNode } from "react";

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
  const [productConfigs, setProductConfigs] = useState<
    Record<string, ProductConfig>
  >(() => {
    try {
      const stored = localStorage.getItem("product_configs");
      if (stored) {
        return JSON.parse(stored);
      }
      // Migrate from old formats
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
        localStorage.setItem("product_configs", JSON.stringify(migrated));
        localStorage.removeItem("product_states");
        return migrated;
      }
      const oldEnabled = localStorage.getItem("enabled_products");
      if (oldEnabled) {
        const data = JSON.parse(oldEnabled);
        const migrated: Record<string, ProductConfig> = {};
        Object.keys(data).forEach((key) => {
          migrated[key] = { enabled: data[key], locked: false };
        });
        localStorage.setItem("product_configs", JSON.stringify(migrated));
        localStorage.removeItem("enabled_products");
        return migrated;
      }
      return DEFAULT_PRODUCTS;
    } catch (err) {
      console.error("Failed to load product configs", err);
    }
  });

  const updateConfig = (
    productPath: string,
    updates: Partial<ProductConfig>,
  ) => {
    setProductConfigs((prev) => {
      const current = prev[productPath] ?? DEFAULT_CONFIG;
      const updated = { ...prev, [productPath]: { ...current, ...updates } };
      localStorage.setItem("product_configs", JSON.stringify(updated));
      return updated;
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

  const isProductEnabled = (productPath: string): boolean => {
    return (productConfigs[productPath] ?? DEFAULT_CONFIG).enabled;
  };

  const isProductLocked = (productPath: string): boolean => {
    return (productConfigs[productPath] ?? DEFAULT_CONFIG).locked;
  };

  // Product is visible in sidebar if enabled OR locked
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
  if (context === undefined) {
    throw new Error(
      "useProductSettings must be used within a ProductSettingsProvider",
    );
  }
  return context;
}
