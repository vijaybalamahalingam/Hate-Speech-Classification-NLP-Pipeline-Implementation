import os
import sys
from zipfile import ZipFile
from hate.logger import logging
from hate.exception import CustomException
from hate.configuration.gcloud_syncer import GCloudSync
from hate.entity.config_entity import DataIngestionConfig
from hate.entity.artifact_entity import DataIngestionArtifacts

class DataIngestion:
    def __init__(self, data_ingestion_config: DataIngestionConfig):
        self.data_ingestion_config = data_ingestion_config
        self.gcloud = GCloudSync()

    def get_data_from_gcloud(self) -> None:
        try:
            logging.info("Entered the get_data_from_gcloud method of Data ingestion class")
            os.makedirs(self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR, exist_ok=True)
            self.gcloud.sync_folder_from_gcloud(self.data_ingestion_config.BUCKET_NAME,
                                                self.data_ingestion_config.ZIP_FILE_NAME,
                                                self.data_ingestion_config.DATA_INGESTION_ARTIFACTS_DIR)
            logging.info("Exited the get_data_from_gcloud method of Data ingestion class")
        except Exception as e:
            raise CustomException(e, sys) from e

    def unzip_and_clean(self):
        logging.info("Entered the unzip_and_clean method of Data ingestion class")
        try:
            with ZipFile(self.data_ingestion_config.ZIP_FILE_PATH, 'r') as zip_ref:
                valid_files = [f for f in zip_ref.namelist() if not f.startswith('__MACOSX')]
                for file in valid_files:
                    zip_ref.extract(file, self.data_ingestion_config.ZIP_FILE_DIR)
                    extracted_path = os.path.join(self.data_ingestion_config.ZIP_FILE_DIR, file)
                    if os.path.isfile(extracted_path):
                        new_path = os.path.join(self.data_ingestion_config.ZIP_FILE_DIR, os.path.basename(file))
                        os.rename(extracted_path, new_path)
                        # Clean up directory structure
                        directory = os.path.dirname(extracted_path)
                        if os.path.exists(directory) and not os.listdir(directory):
                            os.rmdir(directory)
            logging.info("Exited the unzip_and_clean method of Data ingestion class")
            return self.data_ingestion_config.DATA_ARTIFACTS_DIR, self.data_ingestion_config.NEW_DATA_ARTIFACTS_DIR
        except Exception as e:
            raise CustomException(e, sys) from e

    def initiate_data_ingestion(self) -> DataIngestionArtifacts:
        logging.info("Entered the initiate_data_ingestion method of Data ingestion class")
        try:
            self.get_data_from_gcloud()
            logging.info("Fetched the data from gcloud bucket")
            imbalance_data_file_path, raw_data_file_path = self.unzip_and_clean()
            logging.info("Unzipped file and split into train and valid")
            data_ingestion_artifacts = DataIngestionArtifacts(
                imbalance_data_file_path=imbalance_data_file_path,
                raw_data_file_path=raw_data_file_path
            )
            logging.info("Exited the initiate_data_ingestion method of Data ingestion class")
            logging.info(f"Data ingestion artifact: {data_ingestion_artifacts}")
            return data_ingestion_artifacts
        except Exception as e:
            raise CustomException(e, sys) from e