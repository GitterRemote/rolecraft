import logging
from collections.abc import Callable
from typing import Unpack

from rolecraft import config as _config
from rolecraft import consumer as _consumer
from rolecraft import role_lib as _role
from rolecraft import worker as _worker
from rolecraft import worker_pool as _worker_pool
from rolecraft.config import ConfigStore
from rolecraft.consumer import ConsumerFactory, ConsumerOptions
from rolecraft.queue_factory import BatchBuildOptions, QueueFactory
from rolecraft.role_lib import RoleHanger
from rolecraft.utils import typed_dict as _typed_dict
from rolecraft.worker_pool import WorkerPool

from . import queue_discovery as _queue_discovery
from .queue_discovery import QueueDiscovery
from .service import Service

logger = logging.getLogger(__name__)


class ServiceCreateOptions(BatchBuildOptions, ConsumerOptions, total=False):
    ...


class ServiceFactory:
    def __init__(
        self,
        *,
        queue_factory: QueueFactory | None = None,
        queue_discovery: QueueDiscovery | None = None,
        consumer_factory: ConsumerFactory | None = None,
        worker_pool_factory: Callable[[], WorkerPool] | None = None,
        role_hanger: RoleHanger | None = None,
    ) -> None:
        self.queue_factory = queue_factory or QueueFactory(
            config_fetcher=_config.global_config.get_or_future().fetcher
        )
        self.consumer_factory = (
            consumer_factory or _consumer.DefaultConsumerFactory()
        )
        self.worker_pool_factory = (
            worker_pool_factory or _worker_pool.ThreadWorkerPool
        )
        self.role_hanger = role_hanger or _role.default_role_hanger
        self.queue_discovery = (
            queue_discovery
            or _queue_discovery.DefaultQueueDiscovery(
                role_hanger=self.role_hanger
            )
        )

    def create(
        self,
        *,
        config_store: ConfigStore | None = None,
        **options: Unpack[ServiceCreateOptions],
    ) -> Service:
        """Create the service with a customized configuration and defined queue names.

        The consumer will use the queue names and paired brokers to fetch messages."""

        queue_options = _typed_dict.subset_dict(options, BatchBuildOptions)
        consumer_options = _typed_dict.subset_dict(options, ConsumerOptions)

        if not queue_options:
            queue_options.update(
                **self.queue_discovery(config_store=config_store)
            )

        queues = self.queue_factory.build_queues(
            config_fetcher=config_store.fetcher if config_store else None,
            **queue_options,
        )
        consumer = self.consumer_factory(queues=queues, **consumer_options)
        worker_pool = self.worker_pool_factory()
        worker = _worker.Worker(
            worker_pool=worker_pool,
            consumer=consumer,
            role_hanger=self.role_hanger,
        )
        return Service(
            queues=queues,
            consumer=consumer,
            worker=worker,
            worker_pool=worker_pool,
        )
