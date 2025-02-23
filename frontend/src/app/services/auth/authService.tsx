interface LoginResponse {
  access_token: string;
  token_type: string;
}

interface RegisterResponse {
  message: string;
}

interface ErrorResponse {
  detail: string;
}

export const callLogin = async (
  username: string,
  password: string
): Promise<LoginResponse> => {
  const response = await fetch("http://localhost:8000/auth/login", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) {
    const errorData: ErrorResponse = await response.json();
    throw new Error(errorData.detail || "Login failed");
  }

  return response.json();
};

export const callRegister = async (
  username: string,
  password: string
): Promise<RegisterResponse> => {
  const response = await fetch("http://localhost:8000/auth/register", {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ username, password }),
  });

  if (!response.ok) {
    const errorData: ErrorResponse = await response.json();
    throw new Error(errorData.detail || "Registration failed");
  }

  return response.json();
};
