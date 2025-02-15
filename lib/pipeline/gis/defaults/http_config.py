from lib.service.http import HostSemaphoreConfig, BackoffConfig, RetryPreference, HostOverride
from ._constants import SPATIAL_NSW_HOST, ENVIRONMENT_NSW_HOST

BACKOFF_CONFIG = BackoffConfig(
    RetryPreference(allowed=16),
    hosts={
        SPATIAL_NSW_HOST: HostOverride(pause_other_requests_while_retrying=False),
        ENVIRONMENT_NSW_HOST: HostOverride(pause_other_requests_while_retrying=True),
    },
)

HOST_SEMAPHORE_CONFIG = [
    HostSemaphoreConfig(host=SPATIAL_NSW_HOST, limit=16),
    HostSemaphoreConfig(host=ENVIRONMENT_NSW_HOST, limit=12),
]
