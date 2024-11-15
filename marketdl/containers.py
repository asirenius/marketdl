from dependency_injector import containers, providers
from httpx import AsyncClient

from marketdl.logger import TextLogger
from marketdl.market_data import DataSourceFactory
from marketdl.models import DataService
from marketdl.progress import ConsoleProgress
from marketdl.storage import StorageFactory


class Container(containers.DeclarativeContainer):
    """Dependency injection container"""

    config = providers.Configuration()

    logger = providers.Singleton(
        TextLogger,
        name=config.logger.name,
        level=config.logger.level,
        log_file=config.logger.log_file,
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

    http_client = providers.Resource(
        AsyncClient,
        timeout=config.api.timeout,
        verify=True,
    )

    data_source = providers.Singleton(
        DataSourceFactory.create,
        provider=providers.Factory(
            DataService,
            config.api.service,
        ),
        client=http_client,
        api_key=config.api.api_key,
        timeout=config.api.timeout,
        max_retries=config.api.max_retries,
        retry_delay=config.api.retry_delay,
        logger=logger,
    )

    storage = providers.Singleton(
        StorageFactory.create,
        format=config.storage.format,
        compress=config.storage.compress,
        logger=logger,
    )

    progress = providers.Singleton(
        ConsoleProgress,
        logger=logger,
    )
