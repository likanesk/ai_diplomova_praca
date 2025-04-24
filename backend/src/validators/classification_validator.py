from fastapi import HTTPException
import os

class ClassificationValidator:
    """
    This validator is used for validating data for classification.
    It checks the structure of folders and files in a ZIP archive and verifies whether the files are valid images and if their count matches the expected values.
    """
    def __init__(self, valid_extensions: set):
        self.valid_extensions = valid_extensions

    def is_valid_image(self, file_name: str) -> bool:
        _, ext = os.path.splitext(file_name)
        return ext in self.valid_extensions

    def validate_class_folder(self, class_folder: str, files: list, expected_num_files_per_class: int) -> None:
        """
        Validates the contents of a class folder. 
        All files must be valid images and their count must match with the expected number.
        """
        if not files:
            raise HTTPException(status_code=400, detail=f"Class folder '{class_folder}' contains no files.")

        file_numbers = set(range(1, expected_num_files_per_class + 1))
        found_files = set()

        for file in files:
            base_name, ext = os.path.splitext(file)
            if self.is_valid_image(file):
                try:
                    file_num = int(base_name)
                    if 1 <= file_num <= expected_num_files_per_class:
                        found_files.add(file_num)
                except ValueError:
                    continue  # Skip files that cannot be converted to integer
            else:
                raise HTTPException(status_code=400, detail=f"File '{file}' in class folder '{class_folder}' is not a valid image. Accepted file formats are: {', '.join(self.valid_extensions)}.")

        missing_numbers = file_numbers - found_files
        if missing_numbers:
            raise HTTPException(status_code=400, detail=f"Missing files for expected numbers: {sorted(missing_numbers)} in class folder '{class_folder}'.")

        if len(files) != expected_num_files_per_class:
            raise HTTPException(status_code=400, detail=f"Class folder '{class_folder}' contains {len(files)} files, but {expected_num_files_per_class} were expected.")

    async def validate_classification(self, temp_dir: str, expected_num_classes: int, expected_num_files_per_class: int) -> dict:
        """
        Validates the entire ZIP archive structure for classification.
        It verifies the correct number of classes and if each class contains the expected number of files.
        """
        errors = {"missing_classes": [], "missing_files": [], "invalid_structure": []}
        class_folders = []
        db_folder = None

        for current_folder, subfolders, files_in_current_folder in os.walk(temp_dir):
            rel_path = os.path.relpath(current_folder, temp_dir).replace('\\', '/')

            # Validate top-level folder (DB folder)
            if rel_path == '.':
                if len(subfolders) != 1:
                    errors["invalid_structure"].append("There should be exactly one top-level <DB> folder.")
                    return errors
                db_folder = subfolders[0]

            elif rel_path.count('/') == 0 and os.path.basename(current_folder) == db_folder:
                if not subfolders:
                    errors["invalid_structure"].append("The <DB> folder must contain class folders.")
                    return errors
                class_folders.extend(subfolders)

            # Validate individual class folders and their contents
            elif rel_path.count('/') == 1:
                if subfolders:
                    errors["invalid_structure"].append(f"Class folder '{current_folder}' contains subfolders, which is not allowed.")
                    return errors
                
                try:
                    self.validate_class_folder(current_folder, files_in_current_folder, expected_num_files_per_class)
                except HTTPException as e:
                    errors["missing_files"].append(e.detail)

        # Final check to ensure the correct number of class folders
        if len(class_folders) != expected_num_classes:
            errors["missing_classes"].append(f"Expected {expected_num_classes} class folders, but found {len(class_folders)}.")

        return errors