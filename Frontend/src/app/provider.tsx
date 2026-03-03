"use client";
import { AuthProvider } from "@/contexts/AuthContext";
import { TooltipProvider } from "@/components/ui/tooltip";
import { Toaster } from "@/components/ui/toaster";
import { Toaster as Sonner } from "@/components/ui/sonner";
import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { ThemeProvider } from "../hooks/use-theme";
import { ProductSettingsProvider } from "@/contexts/ProductSettingsContext";
import { SplashScreen } from "@/components/SplashScreen";
import { useState } from "react";

export function Providers({ children }: { children: React.ReactNode }) {
  const queryClient = new QueryClient();
  const [showSplash, setShowSplash] = useState(true);

  return (
    <QueryClientProvider client={queryClient}>
      <ThemeProvider>
        <TooltipProvider>
          <AuthProvider>
            <ProductSettingsProvider>
              {showSplash && (
                <SplashScreen onComplete={() => setShowSplash(false)} />
              )}
              <Toaster />
              <Sonner />
              {children}
            </ProductSettingsProvider>
          </AuthProvider>
        </TooltipProvider>
      </ThemeProvider>
    </QueryClientProvider>
  );
}
