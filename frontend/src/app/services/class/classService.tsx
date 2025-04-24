export const callGetAllClasses = async (
  bucketName: string,
  databaseName: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const response = await fetch(
    `http://localhost:8000/classes/get-all-classes/${bucketName}/${databaseName}`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to fetch classes");
  }

  return await response.json();
};

export const callDownloadClass = async (
  bucketName: string,
  databaseName: string,
  className: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const response = await fetch(
    `http://localhost:8000/classes/download-class/${bucketName}/${databaseName}/${className}`,
    {
      method: "GET",
      headers: {
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to download class");
  }

  const blob = await response.blob();

  const downloadUrl = window.URL.createObjectURL(blob);

  // Temporary download link
  const a = document.createElement("a");
  a.href = downloadUrl;
  a.download = `${className}.zip`;
  document.body.appendChild(a);

  a.click();

  document.body.removeChild(a);

  window.URL.revokeObjectURL(downloadUrl);
};

export const callRemoveClass = async (
  bucketName: string,
  databaseName: string,
  className: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const response = await fetch(
    `http://localhost:8000/classes/delete-class/${bucketName}/${databaseName}/${className}`,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to delete class: '" + className + "'");
  }

  return await response.json();
};
