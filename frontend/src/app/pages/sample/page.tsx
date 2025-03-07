"use client";

import RemoveDialog from "@/app/components/RemoveDialog";
import { useEffect, useState, useCallback } from "react";
import { IoDownloadOutline, IoTrashOutline } from "react-icons/io5";
import {
  callDownloadSample,
  callGetAllSamples,
  callRemoveSample,
} from "@/app/services/sample/sampleService";
import { callGetAllBuckets } from "@/app/services/bucket/bucketService";
import { callGetAllDatasets } from "@/app/services/dataset/datasetService";
import { callGetAllClasses } from "@/app/services/class/classService";
import Table from "@/app/components/Table";
import TableRow from "@/app/components/TableRow";
import { useSearchParams } from "next/navigation";
import Dropdown from "@/app/components/Dropdown";
import { useAuth } from "@/app/hooks/useAuth";

interface Bucket {
  name: string;
  creation_date: Date;
}

export default function SamplePage() {
  useAuth();

  const [samples, setSamples] = useState<string[]>([]);
  const [buckets, setBuckets] = useState<Bucket[]>([]);
  const [datasets, setDatasets] = useState<string[]>([]);
  const [classes, setClasses] = useState<string[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [isModalOpen, setIsModalOpen] = useState(false);
  const [selectedSample, setSelectedSample] = useState<string | null>(null);
  const [selectedBucket, setSelectedBucket] = useState<string>("");
  const [selectedDataset, setSelectedDataset] = useState<string>("");
  const [selectedClass, setSelectedClass] = useState<string>("");
  const [isClassification, setIsClassification] = useState<boolean>(false);

  const searchParams = useSearchParams();

  const fetchBuckets = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      const data = await callGetAllBuckets();
      setBuckets(data?.buckets || []);
    } catch {
      setError("Failed to fetch buckets");
    } finally {
      setLoading(false);
    }
  }, []);

  const fetchDatasets = useCallback(async () => {
    if (!selectedBucket) {
      setDatasets([]);
      setClasses([]);
      setSamples([]);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await callGetAllDatasets(selectedBucket);
      setDatasets(data?.databases || []);
      setClasses([]);
      setSamples([]);
    } catch {
      setError("Failed to fetch datasets");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket]);

  const fetchClasses = useCallback(async () => {
    if (!selectedBucket || !selectedDataset) {
      setClasses([]);
      setSamples([]);
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await callGetAllClasses(selectedBucket, selectedDataset);
      setClasses(data?.classes || []);
      setSamples([]);
      setIsClassification(data?.classes && data.classes.length > 0);
    } catch {
      setError("Failed to fetch classes");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket, selectedDataset]);

  const fetchSamples = useCallback(async () => {
    if (!selectedBucket || !selectedDataset) {
      return;
    }

    setLoading(true);
    setError("");
    try {
      const data = await callGetAllSamples(
        selectedBucket,
        selectedDataset,
        isClassification ? selectedClass : undefined
      );

      const filteredSamples = (data?.samples || []).filter(
        (sample: string) => !sample.includes("/")
      );

      setSamples(filteredSamples);
    } catch {
      setError("Failed to fetch samples");
    } finally {
      setLoading(false);
    }
  }, [selectedBucket, selectedDataset, selectedClass, isClassification]);

  useEffect(() => {
    fetchBuckets();
  }, [fetchBuckets]);

  useEffect(() => {
    if (selectedBucket) {
      fetchDatasets();
    } else {
      setDatasets([]);
      setClasses([]);
      setSamples([]);
    }
  }, [selectedBucket, fetchDatasets]);

  useEffect(() => {
    if (selectedBucket && selectedDataset) {
      fetchClasses();
    } else {
      setClasses([]);
      setSamples([]);
    }
  }, [selectedBucket, selectedDataset, fetchClasses]);

  useEffect(() => {
    if (selectedBucket && selectedDataset) {
      fetchSamples();
    } else {
      setSamples([]);
    }
  }, [
    selectedBucket,
    selectedDataset,
    selectedClass,
    isClassification,
    fetchSamples,
  ]);

  useEffect(() => {
    const bucket = searchParams.get("bucket");
    const dataset = searchParams.get("dataset");
    const cls = searchParams.get("class");
    if (bucket) {
      setSelectedBucket(bucket);
    }
    if (dataset) {
      setSelectedDataset(dataset);
    }
    if (cls) {
      setSelectedClass(cls);
    }
  }, [searchParams]);

  const handleDelete = async () => {
    if (selectedSample && selectedBucket && selectedDataset) {
      try {
        await callRemoveSample(
          selectedBucket,
          selectedDataset,
          selectedSample,
          isClassification ? selectedClass : undefined
        );

        setSamples((prevSamples) =>
          prevSamples.filter((sample) => sample !== selectedSample)
        );

        setIsModalOpen(false);
      } catch {
        setError(`Failed to delete sample: ${selectedSample}`);
      }
    }
  };

  const handleDownload = async (
    bucketName: string,
    datasetName: string,
    sampleName: string,
    className?: string
  ) => {
    if (bucketName && datasetName && sampleName) {
      try {
        await callDownloadSample(
          bucketName,
          datasetName,
          sampleName,
          isClassification ? className : undefined
        );
      } catch {
        setError("Failed to download sample. Please try again.");
      }
    } else {
      console.error("Missing required parameters for download");
    }
  };

  if (loading) {
    return <div>Loading...</div>;
  }

  return (
    <div className="p-5 text-lg font-semibold text-left rtl:text-right text-gray-900 bg-white dark:text-white dark:bg-gray-800">
      <div className="max-w-sm mx-auto my-4">
        <h1 className="pb-5 text-center">Sample page</h1>
        <Dropdown
          id="bucket-select"
          label="Select Bucket"
          value={selectedBucket}
          onChange={(value) => {
            setSelectedBucket(value);
            setSelectedDataset("");
            setSelectedClass("");
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
          onChange={(value) => {
            setSelectedDataset(value);
            setSelectedClass("");
          }}
          options={datasets.map((dataset) => ({
            value: dataset,
            label: dataset,
          }))}
          placeholder="Choose a dataset"
        />

        {isClassification && (
          <Dropdown
            id="class-select"
            label="Select Class"
            value={selectedClass}
            onChange={(value) => setSelectedClass(value)}
            options={classes.map((cls) => ({
              value: cls,
              label: cls,
            }))}
            placeholder="Choose a class"
          />
        )}
      </div>

      {samples.length > 0 && (
        <Table
          headers={["Sample Name", "Remove", "Download"]}
          caption="Samples"
          description={
            isClassification
              ? "A sample represents data within a class, which in our case is an image. Specifically, it is an image that represents the given class."
              : "A sample represents data within a dataset, which in our case is an image or JSON file with metadata about all the images."
          }
        >
          {samples.map((sample, index) => (
            <TableRow key={index}>
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                {sample}
              </td>
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                <button
                  onClick={() => {
                    setSelectedSample(sample);
                    setIsModalOpen(true);
                  }}
                  className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                >
                  <IoTrashOutline className="w-5 h-5 text-red-500 dark:text-white mr-1" />
                </button>
              </td>
              <td className="px-6 py-4 font-medium text-gray-900 whitespace-nowrap dark:text-white">
                <button
                  onClick={() => {
                    handleDownload(
                      selectedBucket,
                      selectedDataset,
                      sample,
                      selectedClass
                    );
                  }}
                  className="font-medium text-blue-600 dark:text-blue-500 hover:underline"
                >
                  <IoDownloadOutline className="w-5 h-5 text-blue-500 dark:text-white mr-1" />
                </button>
              </td>
            </TableRow>
          ))}
        </Table>
      )}

      <RemoveDialog
        isOpen={isModalOpen}
        onClose={() => setIsModalOpen(false)}
        onConfirm={handleDelete}
        name={selectedSample || ""}
        error={error}
        onErrorClose={() => setError("")}
      />
    </div>
  );
}
