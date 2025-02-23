export const callGetAllSamples = async (
  bucketName: string,
  databaseName: string,
  className: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const response = await fetch(
    `http://localhost:8000/samples/get-all-samples-in-class/${bucketName}/${databaseName}/${className}`,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to fetch samples");
  }

  return await response.json();
};

export const callRemoveSample = async (
  bucketName: string,
  databaseName: string,
  className: string,
  sampleName: string
) => {
  const token = localStorage.getItem("access_token");
  if (!token) {
    throw new Error("User is not authenticated");
  }

  const response = await fetch(
    `http://localhost:8000/samples/delete-sample/${bucketName}/${databaseName}/${className}/${sampleName}`,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
        Authorization: `Bearer ${token}`,
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to delete sample: '" + sampleName + "'");
  }

  return await response.json();
};
