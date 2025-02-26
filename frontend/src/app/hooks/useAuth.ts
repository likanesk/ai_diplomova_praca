import { useEffect, useState } from "react";
import { useRouter } from "next/navigation";

export const useAuth = () => {
  const router = useRouter();
  const [isAuthenticated, setIsAuthenticated] = useState<boolean | null>(null);

  useEffect(() => {
    const checkAuth = async () => {
      const token = localStorage.getItem("access_token");

      if (!token) {
        router.push("/pages/login");
        return;
      }

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
        router.push("/pages/login");
      }
    };

    checkAuth();
  }, [router]);

  return isAuthenticated;
};
