export const callGetAllSamples = async (
  bucketName: string,
  databaseName: string,
  className?: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const url = className
    ? `http://localhost:8000/samples/get-all-samples-in-class/${bucketName}/${databaseName}/${className}`
    : `http://localhost:8000/samples/get-all-samples-in-database/${bucketName}/${databaseName}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error("Failed to fetch samples");
  }

  return await response.json();
};

export const callDownloadSample = async (
  bucketName: string,
  databaseName: string,
  sampleName: string,
  className?: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const url = className
    ? `http://localhost:8000/samples/download-sample-classification/${bucketName}/${databaseName}/${className}/${sampleName}`
    : `http://localhost:8000/samples/download-sample-detection-regression/${bucketName}/${databaseName}/${sampleName}`;

  const response = await fetch(url, {
    method: "GET",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    console.error(
      "Failed to download sample. Response status:",
      response.status
    );
    throw new Error("Failed to download sample");
  }

  const blob = await response.blob();

  const downloadUrl = window.URL.createObjectURL(blob);

  // Create a temporary link for download
  const a = document.createElement("a");
  a.href = downloadUrl;
  a.download = sampleName;
  document.body.appendChild(a);

  a.click();

  document.body.removeChild(a);

  window.URL.revokeObjectURL(downloadUrl);
};

export const callRemoveSample = async (
  bucketName: string,
  databaseName: string,
  sampleName: string,
  className?: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const url = className
    ? `http://localhost:8000/samples/delete-sample-classification/${bucketName}/${databaseName}/${className}/${sampleName}`
    : `http://localhost:8000/samples/delete-sample-detection-regression/${bucketName}/${databaseName}/${sampleName}`;

  const response = await fetch(url, {
    method: "DELETE",
    headers: {
      "Content-Type": "application/json",
      Authorization: `Bearer ${token}`,
    },
  });

  if (!response.ok) {
    throw new Error("Failed to delete sample: '" + sampleName + "'");
  }

  return await response.json();
};
