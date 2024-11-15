from marketdl.interfaces import Logger, Storage
from marketdl.models import Artifact


class ParquetStorage(Storage):
    """Parquet file storage implementation"""

    def __init__(self, compress: bool, logger: Logger):
        """Initialize storage with compression settings."""
        self.compress = compress
        self.logger = logger
        self._compression = "snappy" if compress else None

    async def save(self, artifact: Artifact) -> None:
        """Save DataFrame to Parquet file."""
        if artifact.is_empty:
            raise ValueError(f"Cannot save empty artifact: {artifact.id}")

        try:
            self.logger.debug(
                "Saving artifact",
                id=artifact.id,
                path=str(artifact.output_path),
            )

            artifact.output_path.parent.mkdir(parents=True, exist_ok=True)

            df_to_save = artifact.data.copy()
            df_to_save.reset_index(drop=True, inplace=True)

            df_to_save.to_parquet(
                artifact.output_path, compression=self._compression, index=False
            )

            self.logger.debug(
                "Successfully saved artifact",
                id=artifact.id,
                size_bytes=artifact.output_path.stat().st_size,
            )

        except Exception as e:
            self.logger.error(
                "Failed to save artifact",
                id=artifact.id,
                error=str(e),
            )
            raise

    def exists(self, artifact: Artifact) -> bool:
        """Check if file exists in storage"""
        exists = artifact.output_path.exists()
        self.logger.debug(
            "Checking artifact existence",
            id=artifact.id,
            path=str(artifact.output_path),
            exists=exists,
        )
        return exists


class CsvStorage(Storage):
    """CSV file storage implementation"""

    def __init__(self, compress: bool, logger: Logger):
        """Initialize storage with compression settings."""
        self.compress = compress
        self.logger = logger
        self._compression = "gzip" if compress else None

    async def save(self, artifact: Artifact) -> None:
        """Save DataFrame to CSV file with optional compression."""
        if artifact.is_empty:
            raise ValueError(f"Cannot save empty artifact: {artifact.id}")

        try:
            self.logger.debug(
                "Saving artifact",
                id=artifact.id,
                path=str(artifact.output_path),
            )

            artifact.output_path.parent.mkdir(parents=True, exist_ok=True)

            df_to_save = artifact.data.copy()
            df_to_save.reset_index(drop=True, inplace=True)

            df_to_save.to_csv(
                artifact.output_path,
                compression=self._compression,
                index=False,
                encoding="utf-8",
            )

            self.logger.debug(
                "Successfully saved artifact",
                id=artifact.id,
                size_bytes=artifact.output_path.stat().st_size,
            )

        except Exception as e:
            self.logger.error(
                "Failed to save artifact",
                id=artifact.id,
                error=str(e),
            )
            raise

    def exists(self, artifact: Artifact) -> bool:
        """Check if file exists in storage"""
        exists = artifact.output_path.exists()
        self.logger.debug(
            "Checking artifact existence",
            id=artifact.id,
            path=str(artifact.output_path),
            exists=exists,
        )
        return exists


class StorageFactory:
    """Factory for storage implementations"""

    @staticmethod
    def create(format: str, compress: bool, logger: Logger) -> Storage:
        """Create appropriate storage implementation"""
        formats = {
            "parquet": ParquetStorage,
            "csv": CsvStorage,
        }

        if format not in formats:
            raise ValueError(f"Unsupported format: {format}")

        return formats[format](compress, logger)
