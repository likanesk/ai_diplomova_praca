import json
import os
from .common_validator import CommonValidator
from typing import Dict, List

class DetectionValidator(CommonValidator):
    """
    This validator is used for validating data for detection. 
    ROI values in the JSON file must be valid.
    """
    def validate_detection_data(self, data: List[dict]) -> Dict[str, List]:
        """
        Validates ROI values in the JSON file. 
        Checks that the ROI values ​​are a list of four integers and that the condition L < R and T < B holds.
        """
        errors = {"invalid_roi": []}
        for item in data:
            roi = item.get("ROI", [])
            if (
                not isinstance(roi, list) or
                len(roi) != 4 or
                not all(isinstance(x, int) for x in roi) or
                roi[0] >= roi[2] or
                roi[1] >= roi[3]
            ):
                errors["invalid_roi"].append(item.get("file", "Unknown file"))
        return errors

    async def validate(self, temp_dir: str) -> Dict[str, List]:
        """
        Validates the entire ZIP archive for detection.
        It uses methods from CommonValidator for structure and content validation and adds specific checks for ROI values.
        """
        errors = await self.validate_zip_contents(temp_dir, is_regression=False)
        
        if any(errors.values()):
            return errors
        
        # Load JSON data
        db_folder = os.listdir(temp_dir)[0]
        db_path = os.path.join(temp_dir, db_folder)
        json_files = [f for f in os.listdir(db_path) if f.endswith(".json")]
        
        json_path = os.path.join(db_path, json_files[0])
        
        with open(json_path, "r", encoding="utf-8") as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError:
                return errors
        
        data_errors = self.validate_detection_data(data)
        errors.update(data_errors)
        return errors