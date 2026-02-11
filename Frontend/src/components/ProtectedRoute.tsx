"use client";

import { ReactNode, useEffect } from "react";
import { useRouter, usePathname } from "next/navigation";
import { useAuth } from "@/contexts/AuthContext";

interface ProtectedRouteProps {
  children: ReactNode;
}

export function ProtectedRoute({ children }: ProtectedRouteProps) {
  const router = useRouter();
  const pathname = usePathname();
  const { user, hasAccess, hydrated } = useAuth();
  const isLoginPage = pathname === "/login";

  useEffect(() => {
    if (!hydrated) return; // ✅ Wait for localStorage to load
    if (isLoginPage) return;

    // Not logged in? Go to login
    if (!user) {
      router.replace(`/login?from=${pathname}`);
      return;
    }

    // Logged in but no access? Go to insights
    if (!hasAccess(pathname)) {
      router.replace("/insights");
    }
  }, [hydrated, user, hasAccess, pathname, router]);

  // ✅ Only render once hydrated
  if (!hydrated) {
    return (
      <div className="flex h-screen items-center justify-center">
        Loading...
      </div>
    );
  }

  if ((!user || !hasAccess(pathname)) && !isLoginPage) {
    return null;
  }

  return <>{children}</>;
}
