export const callGetAllClasses = async (
  bucketName: string,
  databaseName: string
) => {
  const response = await fetch(
    "http://localhost:8000/classes/get-all-classes/" +
      bucketName +
      "/" +
      databaseName,
    {
      method: "GET",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to fetch classes");
  }

  return await response.json();
};

export const callRemoveClass = async (
  bucketName: string,
  databaseName: string,
  className: string
) => {
  const response = await fetch(
    "http://localhost:8000/classes/delete-class/" +
      bucketName +
      "/" +
      databaseName +
      "/" +
      className,
    {
      method: "DELETE",
      headers: {
        "Content-Type": "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error("Failed to delete class: '" + className + "'");
  }

  return await response.json();
};
