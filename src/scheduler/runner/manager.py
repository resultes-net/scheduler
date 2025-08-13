import abc as _abc
import collections.abc as _cabc
import contextlib as _ctx
import dataclasses as _dc
import datetime as _dt
import logging as _log
import pathlib as _pl
import time as _time
import typing as _tp

import openstack as _ost
import openstack.connection as _oconn
import resultes_openstack_utils.clouds_yaml as _cyaml

_LOGGER = _log.getLogger(__name__)


class AbstractRunnerManager(_abc.ABC):
    @property
    @_abc.abstractmethod
    def n_max_jobs_per_runner(self) -> int:
        raise NotImplementedError()

    @_abc.abstractmethod
    def create_server_and_get_ip(self) -> str:
        raise NotImplementedError()

    @_abc.abstractmethod
    def delete_servers(self, ip_address: str | None = None) -> None:
        raise NotImplementedError()


@_dc.dataclass
class _Image:
    id: str
    created_at: _dt.datetime

    @staticmethod
    def create(*, id: str, created_at: str) -> "_Image":
        datetime = _dt.datetime.fromisoformat(created_at)
        return _Image(id, datetime)


class _MappedBlockDevice(_tp.TypedDict):
    uuid: str
    source_type: _tp.Literal["volume", "image"]
    destination_type: _tp.Literal["volume", "local"]
    volume_size: _tp.NotRequired[int]
    delete_on_termination: bool
    boot_index: _tp.NotRequired[int | None]


_NETWORK_NAME = "k8s-clusterapi-cluster-pck-cfedjc3-pck-cfedjc3"


class _ServerFactory:
    def __init__(self, connection: _oconn.Connection) -> None:
        self._connection = connection

    def create_server_and_get_ip(self) -> str:
        image_id = self._get_runner_image_id()
        flavor = self._connection.compute.find_flavor("a8-ram16-disk50-perf1")

        network = self._connection.network.find_network(_NETWORK_NAME)

        block_device_mapping = self._get_block_device_mapping()

        server = self._connection.compute.create_server(
            name="runner",
            image_id=image_id,
            flavor_id=flavor.id,
            networks=[{"uuid": network.id}],
            security_groups=[{"name": "runner"}],
            availability_zone="az-2",
            block_device_mapping=block_device_mapping,
        )

        while True:
            server = self._connection.compute.find_server(server.id)

            if server.addresses:
                return _get_ip_address(server)

            seconds = 5.0
            _time.sleep(seconds)

    def _get_runner_image_id(self) -> str:
        image = self._connection.image.find_image("runner-image")
        if not image:
            raise RuntimeError("Runner image not found.")

        return image.id

    def _get_block_device_mapping(self) -> _cabc.Sequence[_MappedBlockDevice]:
        boot_image_id = self._get_runner_image_id()

        # To avoid a race condition between a new image being created in CI and us
        # spinning up a server here, we've arrived at the following scheme:
        #   1. Images are only ever added by CI
        #   2. And they are only ever deleted by the scheduler
        # This makes sure the image that the scheduler has decided to use at any
        # given moment exists at that time.
        disk_image_uuid = self._delete_stale_disk_images_and_get_uuid_of_latest()

        block_device_mapping: _cabc.Sequence[_MappedBlockDevice] = [
            {
                "uuid": boot_image_id,
                "source_type": "image",
                "destination_type": "local",
                "delete_on_termination": True,
                "boot_index": 0,
            },
            {
                "uuid": disk_image_uuid,
                "source_type": "image",
                "destination_type": "volume",
                "volume_size": 2,
                "delete_on_termination": True,
            },
        ]

        return block_device_mapping

    def _delete_stale_disk_images_and_get_uuid_of_latest(self) -> str:
        images = self._get_disk_images()

        def get_created_at(image: _Image) -> _dt.datetime:
            return image.created_at

        sorted_images = sorted(images, key=get_created_at)

        *old_images, current_image = sorted_images

        for old_image in old_images:
            _LOGGER.info("Delete stale runner disk image %s.", old_image.id)
            self._connection.image.delete_image(old_image.id)

        _LOGGER.info("Current runner disk image is %s.", current_image.id)

        return current_image.id

    def _get_disk_images(self) -> list[_Image]:
        disk_images = list(self._connection.image.images(name="runner-disk-image"))

        if not disk_images:
            raise RuntimeError("No `runner-disk-image' image found.")

        images = [_Image.create(id=i.id, created_at=i.created_at) for i in disk_images]

        return images


class RunnerManager(AbstractRunnerManager):
    def __init__(self, os_password: str, clouds_yaml_file_path: _pl.Path) -> None:
        self._os_password = os_password
        self._clouds_yaml_file_path = clouds_yaml_file_path

    @property
    def n_max_jobs_per_runner(self) -> int:
        return 8

    def create_server_and_get_ip(self) -> str:
        with self._create_connection() as connection:
            server_factory = _ServerFactory(connection)

            return server_factory.create_server_and_get_ip()

    def delete_servers(self, ip_address: str | None = None) -> None:
        _log.info("Deleting servers...")

        with self._create_connection() as connection:
            kwargs = {"name": "runner"}
            if ip_address:
                kwargs["ip"] = ip_address

            servers = list(connection.compute.servers(**kwargs))

            if not servers:
                if ip_address:
                    _log.info("...no servers with IP address %s found.", ip_address)
                else:
                    _log.info("...no servers found.")

            for server in servers:
                ip_address = _get_ip_address(server)
                _LOGGER.info(
                    "Deleting runner %s with IP address %s.", server.id, ip_address
                )
                connection.compute.delete_server(server)

            _log.info("...DONE: %i server(s) deleted.", len(servers))

    @_ctx.contextmanager
    def _create_connection(self) -> _cabc.Iterator[_oconn.Connection]:
        data = _cyaml.get_clouds_yaml_openstack_json(self._clouds_yaml_file_path)

        connection = _ost.connect(
            load_yaml_config=False,
            load_envvars=False,
            os_password=self._os_password,
            **data,
        )

        yield connection

        connection.close()


def _get_ip_address(server) -> str:
    ip_address = server.addresses[_NETWORK_NAME][0]["addr"]
    return ip_address


class DummyRunnerManager(AbstractRunnerManager):
    def __init__(self, ip_address: str, n_max_jobs_per_runner: int) -> None:
        self._ip_address = ip_address
        self._n_max_jobs_per_runner = n_max_jobs_per_runner

    @property
    def n_max_jobs_per_runner(self) -> int:
        return self._n_max_jobs_per_runner

    def create_server_and_get_ip(self) -> str:
        return self._ip_address

    def delete_servers(self, ip_address: str | None = None) -> None:
        return
