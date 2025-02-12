export const callGetAllBuckets = async () => {
  const response = await fetch(
    "http://localhost:8000/buckets/get-all-buckets",
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to fetch buckets");
  }

  return await response.json();
};

export const callRemoveBucket = async (bucketName: string) => {
  const response = await fetch(
    "http://localhost:8000/buckets/delete-bucket/" + bucketName,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to delete bucket: '" + bucketName + "'");
  }

  return await response.json();
};
