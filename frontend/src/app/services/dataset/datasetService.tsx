export const callUploadZip = async (
  bucketName: string,
  expectedNumClasses: number,
  expectedNumFilesPerClass: number,
  file: File
) => {
  const formData = new FormData();
  formData.append("file", file);

  const url = `http://localhost:8000/databases/upload-zip/${bucketName}?expected_num_classes=${expectedNumClasses}&expected_num_files_per_class=${expectedNumFilesPerClass}`;

  const response = await fetch(url, {
    method: "POST",
    body: formData,
  });

  if (!response.ok) {
    throw new Error("Failed to upload dataset");
  }

  return response.json();
};

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
