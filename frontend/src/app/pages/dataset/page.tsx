"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoDownloadOutline, IoTrashOutline } from "react-icons/io5";
import {
  callDownloadDataset,
  callGetAllDatasets,
  callRemoveDataset,
} from "@/app/services/dataset/datasetService";
import {
  callDownloadClass,
  callGetAllClasses,
  callRemoveClass,
} from "@/app/services/class/classService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useRouter, useSearchParams } from "next/navigation";
import Dropdown from "@/app/components/Dropdown";
import { useAuth } from "@/app/hooks/useAuth";
import ErrorMessage from "@/app/components/ErrorMessage";
import Pagination from "@/app/components/Pagination";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function DatasetPage() {
  useAuth();

  const [datasets, setDatasets] = useState<string[]>([]);
  const [classes, setClasses] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isDatasetModalOpen, setIsDatasetModalOpen] = useState(false);
  const [isClassModalOpen, setIsClassModalOpen] = useState(false);
  const [selectedDataset, setSelectedDataset] = useState<string>("");
  const [selectedClass, setSelectedClass] = useState<string>("");
  const [selectedBucket, setSelectedBucket] = useState<string>("");
  const [noDatasetsError, setNoDatasetsError] = useState(false);
  const [noClassesError, setNoClassesError] = useState(false);

  const [currentDatasetPage, setCurrentDatasetPage] = useState(1);
  const [currentClassPage, setCurrentClassPage] = useState(1);
  const [itemsPerPage] = useState(10);

  const searchParams = useSearchParams();
  const router = useRouter();

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await callGetAllBuckets();
      const fetchBuckets = data?.buckets || [];
      setBuckets(fetchBuckets);
    } catch {
      setError("Failed to fetch buckets");
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchDatasets = useCallback(async () => {
    if (!selectedBucket) {
      setDatasets([]);
      return;
    }

    setLoading(true);
    setError("");
    setNoDatasetsError(false);
    try {
      const data = await callGetAllDatasets(selectedBucket);
      const fetchDatasets = data?.databases || [];
      setDatasets(fetchDatasets);

      if (fetchDatasets.length === 0) {
        setNoDatasetsError(true);
      }
    } catch {
      setError("Failed to fetch datasets");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket]);

  const fetchClasses = useCallback(async () => {
    if (!selectedBucket || !selectedDataset) {
      setClasses([]);
      return;
    }

    setLoading(true);
    setError("");
    setNoClassesError(false);

    try {
      const data = await callGetAllClasses(selectedBucket, selectedDataset);
      const fetchedClasses = data?.classes || [];

      setClasses(fetchedClasses);
      if (fetchedClasses.length === 0) {
        setNoClassesError(true);
      }
    } catch {
      setError("Failed to fetch classes");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket, selectedDataset]);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  useEffect(() => {
    setDatasets([]);
    setClasses([]);
    fetchDatasets();
  }, [selectedBucket, fetchDatasets]);

  useEffect(() => {
    fetchClasses();
  }, [fetchClasses, selectedBucket, selectedDataset]);

  useEffect(() => {
    const bucket = searchParams.get("bucket");
    const dataset = searchParams.get("dataset");
    if (bucket) {
      setSelectedBucket(bucket);
    }
    if (dataset) {
      setSelectedDataset(dataset);
    }
  }, [searchParams]);

  const handleDatasetDelete = async () => {
    if (selectedDataset && selectedBucket) {
      try {
        await callRemoveDataset(selectedBucket, selectedDataset);

        setDatasets((prevDatasets) =>
          prevDatasets.filter((dataset) => dataset !== selectedDataset)
        );

        setIsDatasetModalOpen(false);
      } catch {
        setError(`Failed to delete dataset: ${selectedDataset}`);
      }
    }
  };

  const handleClassDelete = async () => {
    if (selectedClass && selectedBucket && selectedDataset) {
      try {
        await callRemoveClass(selectedBucket, selectedDataset, selectedClass);

        setClasses((prevClasses) =>
          prevClasses.filter((cls) => cls !== selectedClass)
        );

        setIsClassModalOpen(false);
      } catch {
        setError(`Failed to delete class: ${selectedClass}`);
      }
    }
  };

  const handleDatasetDownload = async (
    bucketName: string,
    datasetName: string
  ) => {
    try {
      await callDownloadDataset(bucketName, datasetName);
    } catch {
      setError("Failed to download dataset");
    }
  };

  const handleClassDownload = async (
    bucketName: string,
    datasetName: string,
    className: string
  ) => {
    try {
      await callDownloadClass(bucketName, datasetName, className);
    } catch {
      setError("Failed to download class");
    }
  };

  const handleDatasetRowClick = (dataset: string) => {
    setSelectedDataset(dataset);
    setSelectedClass("");
    setNoClassesError(false);

    setTimeout(() => {
      scrollToClassesSection();
    }, 100);
  };

  const handleDatasetChange = (value: string) => {
    setSelectedDataset(value);
    setSelectedClass("");
    setNoClassesError(false);

    setTimeout(() => {
      scrollToClassesSection();
    }, 100);
  };

  const handleClassRowClick = (
    bucketName: string,
    datasetName: string,
    className: string
  ) => {
    setSelectedClass(className);
    router.push(
      `/pages/sample?bucket=${bucketName}&dataset=${datasetName}&class=${className}`
    );
  };

  const scrollToClassesSection = () => {
    const classesSection = document.getElementById("classes-section");
    if (classesSection) {
      classesSection.scrollIntoView({ behavior: "smooth" });
    }
  };

  const indexOfLastDataset = currentDatasetPage * itemsPerPage;
  const indexOfFirstDataset = indexOfLastDataset - itemsPerPage;
  const currentDatasets = datasets.slice(
    indexOfFirstDataset,
    indexOfLastDataset
  );

  const indexOfLastClass = currentClassPage * itemsPerPage;
  const indexOfFirstClass = indexOfLastClass - itemsPerPage;
  const currentClasses = classes.slice(indexOfFirstClass, indexOfLastClass);

  const handleDatasetPageChange = (page: number) => {
    setCurrentDatasetPage(page);
  };

  const handleClassPageChange = (page: number) => {
    setCurrentClassPage(page);
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  return (
    <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
      {error && <ErrorMessage message={error} onClose={() => setError("")} />}

      <div className="max-w-sm mx-auto my-4">
        <h1 className="pb-5 text-center">Dataset page</h1>

        <Dropdown
          id="bucket-select"
          label="Select Bucket"
          value={selectedBucket}
          onChange={(value) => {
            setSelectedBucket(value);
            setSelectedDataset("");
          }}
          options={buckets.map((bucket) => ({
            value: bucket.name,
            label: bucket.name,
          }))}
          placeholder="Choose a bucket"
        />

        <Dropdown
          id="dataset-select"
          label="Select Dataset"
          value={selectedDataset}
          onChange={handleDatasetChange}
          options={datasets.map((dataset) => ({
            value: dataset,
            label: dataset,
          }))}
          placeholder="Choose a dataset"
        />
      </div>

      {noDatasetsError && (
        <ErrorMessage
          message="Selected bucket does not contain any dataset!"
          onClose={() => setNoDatasetsError(false)}
        />
      )}

      {noClassesError && (
        <ErrorMessage
          message="Selected dataset does not contain any class!"
          onClose={() => setNoClassesError(false)}
        />
      )}

      {selectedBucket && !noDatasetsError && datasets.length > 0 && (
        <>
          <Table
            headers={["Dataset Name", "Remove", "Download"]}
            caption="Datasets"
            description="A dataset is a collection of data, typically in a structured format, that can be used for analysis or processing."
          >
            {currentDatasets.map((dataset, index) => (
              <TableRow
                key={index}
                onClick={() => handleDatasetRowClick(dataset)}
              >
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  {dataset}
                </td>
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedDataset(dataset);
                      setIsDatasetModalOpen(true);
                    }}
                    className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                  >
                    <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
                  </button>
                </td>
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDatasetDownload(selectedBucket, dataset);
                    }}
                    className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                  >
                    <IoDownloadOutline className="w-5 h-5 text-blue-500 dark:text-white mr-1" />
                  </button>
                </td>
              </TableRow>
            ))}
          </Table>

          <Pagination
            totalItems={datasets.length}
            itemsPerPage={itemsPerPage}
            currentPage={currentDatasetPage}
            onPageChange={handleDatasetPageChange}
          />
        </>
      )}

      {selectedDataset && !noClassesError && classes.length > 0 && (
        <div id="classes-section">
          <Table
            headers={["Class Name", "Remove", "Download"]}
            caption="Classes"
            description="A class is a collection of data within a dataset, typically representing a specific type or category of data."
          >
            {currentClasses.map((cls, index) => (
              <TableRow
                key={index}
                onClick={() =>
                  handleClassRowClick(selectedBucket, selectedDataset, cls)
                }
              >
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  {cls}
                </td>
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      setSelectedClass(cls);
                      setIsClassModalOpen(true);
                    }}
                    className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                  >
                    <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
                  </button>
                </td>
                <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                  <button
                    onClick={(e) => {
                      e.stopPropagation();
                      handleClassDownload(selectedBucket, selectedDataset, cls);
                    }}
                    className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                  >
                    <IoDownloadOutline className="w-5 h-5 text-blue-500 dark:text-white mr-1" />
                  </button>
                </td>
              </TableRow>
            ))}
          </Table>

          <Pagination
            totalItems={classes.length}
            itemsPerPage={itemsPerPage}
            currentPage={currentClassPage}
            onPageChange={handleClassPageChange}
          />
        </div>
      )}

      <RemoveDialog
        isOpen={isDatasetModalOpen}
        onClose={() => setIsDatasetModalOpen(false)}
        onConfirm={handleDatasetDelete}
        name={selectedDataset || ""}
        error={error}
        onErrorClose={() => setError("")}
      />

      <RemoveDialog
        isOpen={isClassModalOpen}
        onClose={() => setIsClassModalOpen(false)}
        onConfirm={handleClassDelete}
        name={selectedClass || ""}
        error={error}
        onErrorClose={() => setError("")}
      />
    </div>
  );
}
