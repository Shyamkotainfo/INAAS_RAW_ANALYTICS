// import { useState, useEffect } from "react";

// export function usePersistentState<T>(key: string, defaultValue: T) {
//   const [state, setState] = useState<T>(defaultValue);
//   const [isLoaded, setIsLoaded] = useState(false);

//   // Read from sessionStorage
//   useEffect(() => {
//     try {
//       const saved = sessionStorage.getItem(key);
//       if (saved) {
//         setState(JSON.parse(saved));
//       }
//     } catch {
//       console.error("sessionStorage read failed");
//     }
//     setIsLoaded(true);
//   }, [key]);

//   // Write to sessionStorage
//   useEffect(() => {
//     if (!isLoaded) return;
//     try {
//       sessionStorage.setItem(key, JSON.stringify(state));
//     } catch {
//       console.error("sessionStorage write failed");
//     }
//   }, [key, state, isLoaded]);

//   return [state, setState] as const;
// }

import { useState, useEffect } from "react";

export function usePersistentState<T>(key: string, defaultValue: T) {
  const [state, setState] = useState<T>(defaultValue);
  const [isLoaded, setIsLoaded] = useState(false);

  useEffect(() => {
    try {
      const saved = sessionStorage.getItem(key);
      if (saved) setState(JSON.parse(saved));
    } catch {
      console.error("sessionStorage read failed");
    }
    setIsLoaded(true);
  }, [key]);

  useEffect(() => {
    if (!isLoaded) return;
    try {
      sessionStorage.setItem(key, JSON.stringify(state));
    } catch {
      console.error("sessionStorage write failed");
    }
  }, [key, state, isLoaded]);

  return [state, setState, isLoaded] as const;
}
