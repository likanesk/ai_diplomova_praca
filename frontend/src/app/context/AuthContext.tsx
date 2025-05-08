"use client";

import {
  createContext,
  useContext,
  useState,
  useEffect,
  ReactNode,
  useCallback,
} from "react";
import { useRouter } from "next/navigation";

interface AuthContextType {
  isAuthenticated: boolean | null;
  login: (token: string) => void;
  logout: () => void;
}

const AuthContext = createContext<AuthContextType | undefined>(undefined);

export const AuthProvider = ({ children }: { children: ReactNode }) => {
  const [isAuthenticated, setIsAuthenticated] = useState<boolean | null>(null);
  const router = useRouter();

  const verifyToken = useCallback(
    async (token: string) => {
      try {
        const response = await fetch("http://localhost:8000/auth/verify", {
          method: "GET",
          headers: {
            "Content-Type": "application/json",
            Authorization: `Bearer ${token}`,
          },
        });

        if (!response.ok) {
          throw new Error("Token is invalid");
        }

        setIsAuthenticated(true);
      } catch {
        localStorage.removeItem("access_token");
        setIsAuthenticated(false);
        router.push("/pages/login");
      }
    },
    [router]
  );

  useEffect(() => {
    const token = localStorage.getItem("access_token");
    if (token) {
      verifyToken(token);
    } else {
      setIsAuthenticated(false);
    }
  }, [verifyToken]);

  const login = (token: string) => {
    localStorage.setItem("access_token", token);
    verifyToken(token);
  };

  const logout = () => {
    localStorage.removeItem("access_token");
    setIsAuthenticated(false);
    router.push("/pages/login");
  };

  return (
    <AuthContext.Provider value={{ isAuthenticated, login, logout }}>
      {children}
    </AuthContext.Provider>
  );
};

export const useAuth = () => {
  const context = useContext(AuthContext);
  if (!context) {
    throw new Error("useAuth must be used within an AuthProvider");
  }
  return context;
};
