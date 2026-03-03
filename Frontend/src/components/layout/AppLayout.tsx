import { ReactNode } from "react";
import { AppSidebar } from "./AppSidebar";
import { Header } from "./Header";

interface AppLayoutProps {
  children: ReactNode;
  title: string;
  subtitle?: string;
  headerActions?: ReactNode;
}

export function AppLayout({ children, title, subtitle, headerActions }: AppLayoutProps) {
  return (
    <div className="flex min-h-screen w-full bg-background"> 
      <AppSidebar />
      <div className="flex-1 flex flex-col">
        <Header title={title} subtitle={subtitle} headerActions={headerActions} />
        <main className="flex-1 overflow-y-auto p-6">
          {children}
        </main>
      </div>
    </div>
  );
}
