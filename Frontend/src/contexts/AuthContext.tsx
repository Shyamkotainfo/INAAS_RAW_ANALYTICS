"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
} from "react";

export type UserRole = "admin" | "demo-insight";

export interface User {
  username: string;
  role: UserRole;
}

interface AuthContextType {
  user: User | null;
  hydrated: boolean; // ✅ new flag
  login: (username: string, password: string) => boolean;
  logout: () => void;
  hasAccess: (path: string) => boolean;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

const USERS: Record<string, { password: string; role: UserRole }> = {
  admin: { password: "admin123", role: "admin" },
  "demo-insight": { password: "demo123", role: "demo-insight" },
};

// const DEMO_INSIGHT_ROUTES = [
//   "/",
//   "/insights",
//   "/insights/new",
//   "/insights/dashboard",
// ];

export function AuthProvider({ children }: { children: ReactNode }) {
  const [user, setUser] = useState<User | null>(null);
  const [hydrated, setHydrated] = useState(false); // ✅

  useEffect(() => {
    try {
      const stored = localStorage.getItem("auth_user");
      if (stored) {
        setUser(JSON.parse(stored));
      }
    } catch (err) {
      console.error("Failed to load auth user", err);
    } finally {
      setHydrated(true); // ✅ mark as ready
    }
  }, []);

  const login = (username: string, password: string): boolean => {
    const userData = USERS[username];
    if (userData && userData.password === password) {
      const newUser = { username, role: userData.role };
      setUser(newUser);
      localStorage.setItem("auth_user", JSON.stringify(newUser));
      return true;
    }
    return false;
  };

  const logout = () => {
    setUser(null);
    localStorage.removeItem("auth_user");
  };

  const hasAccess = (path: string): boolean => {
    if (!user && !path.startsWith("/login")) return false;
    if (!user && path.startsWith("/login")) return true;

    // if (user?.role === "admin") return true;
    // return DEMO_INSIGHT_ROUTES.some((route) => path.startsWith(route));
    // return DEMO_INSIGHT_ROUTES.
    if (path === "/") return true;
    if ((path === "/insights")) return true;
    if ((path === "/insights/new")) return true;
    if (path.startsWith("/chat")) return true;
    return false;
  };

  return (
    <AuthContext.Provider value={{ user, hydrated, login, logout, hasAccess }}>
      {children}
    </AuthContext.Provider>
  );
}

export function useAuth() {
  const ctx = useContext(AuthContext);
  if (!ctx) throw new Error("useAuth must be used within an AuthProvider");
  return ctx;
}
