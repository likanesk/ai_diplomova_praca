export const callGetAllDatasets = async (bucketName: string) => {
  const response = await fetch(
    "http://localhost:8000/databases/get-all-databases/" + bucketName,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to fetch datasets");
  }

  return await response.json();
};

export const callRemoveDataset = async (
  bucketName: string,
  databaseName: string
) => {
  const response = await fetch(
    "http://localhost:8000/databases/delete-database/" +
      bucketName +
      "/" +
      databaseName,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to delete dataset: '" + databaseName + "'");
  }

  return await response.json();
};
