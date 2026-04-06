'use client';

import { useEffect } from 'react';
import { usePathname, useRouter } from 'next/navigation';

export default function RouteGuard() {
  const router = useRouter();
  const pathname = usePathname();

  useEffect(() => {
    // Avoid running on Next.js internal routes
    if (!pathname) return;

    const isAuthenticated = typeof window !== 'undefined' && localStorage.getItem('isAuthenticated') === 'true';

    // If logged in, force stay on /chat
    if (isAuthenticated && pathname !== '/chat') {
      router.replace('/chat');
      return;
    }

    // If not logged in, force stay on / (login)
    if (!isAuthenticated && pathname !== '/') {
      router.replace('/');
      return;
    }
  }, [pathname, router]);

  return null; 
}