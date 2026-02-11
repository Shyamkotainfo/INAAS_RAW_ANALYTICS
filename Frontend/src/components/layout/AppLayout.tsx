"use client";

import { ReactNode } from "react";
import { AppSidebar } from "./AppSidebar";
import { Header } from "./Header";

interface AppLayoutProps {
  children: ReactNode;
  title: string;
  subtitle?: string;
  headerActions?: ReactNode;
}

export default function AppLayout({ children, title, subtitle, headerActions }: AppLayoutProps) {
  return (
    <div className="flex h-screen w-screen overflow-hidden">
      
      {/* FIXED SIDEBAR */}
      <div className="flex-shrink-0">
        <AppSidebar />
      </div>

      {/* MAIN SECTION */}
      <div className="flex flex-col flex-1 min-w-0">

        {/* FIXED HEADER */}
        <Header title={title} subtitle={subtitle} headerActions={headerActions} />

        {/* SCROLLABLE CONTENT */}
        <main className="flex-1 overflow-y-auto px-6 py-4 bg-background">
          {children}
        </main>
      </div>
    </div>
  );
}
