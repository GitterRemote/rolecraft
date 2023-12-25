import logging
from collections.abc import Callable

from . import consumer as _consumer
from . import role as _role
from . import worker as _worker
from . import worker_pool as _worker_pool
from .consumer import ConsumerFactory
from .queue_manager import QueueManager
from .role import RoleHanger
from .service import Service
from .worker_pool import WorkerPool

logger = logging.getLogger(__name__)


class ServiceFactory:
    def __init__(
        self,
        *,
        queue_manager: QueueManager,
        consumer_factory: ConsumerFactory | None = None,
        worker_pool_factory: Callable[[], WorkerPool] | None = None,
        role_hanger: RoleHanger | None = None,
    ) -> None:
        self.queue_manager = queue_manager
        self.consumer_factory = (
            consumer_factory or _consumer.DefaultConsumerFactory()
        )
        self.worker_pool_factory = (
            worker_pool_factory or _worker_pool.ThreadWorkerPool
        )
        self.role_hanger = role_hanger or _role.default_role_hanger

    def create(self) -> Service:
        queues = self.queue_manager.build_queues()
        consumer = self.consumer_factory(queues=queues)
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
