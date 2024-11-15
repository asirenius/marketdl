import pandas as pd
import pytest

from marketdl.storage import CsvStorage, ParquetStorage, StorageFactory


@pytest.mark.asyncio
async def test_parquet_storage(storage, test_data, test_artifact):
    test_artifact.data = test_data
    await storage.save(test_artifact)
    assert test_artifact.output_path.exists()


@pytest.mark.asyncio
async def test_csv_storage(tmp_path, logger, test_data, test_artifact):
    storage = CsvStorage(compress=True, logger=logger)
    test_artifact.data = test_data
    await storage.save(test_artifact)
    assert test_artifact.output_path.exists()


def test_storage_factory(logger):
    parquet = StorageFactory.create("parquet", compress=True, logger=logger)
    assert isinstance(parquet, ParquetStorage)
    csv = StorageFactory.create("csv", compress=True, logger=logger)
    assert isinstance(csv, CsvStorage)
