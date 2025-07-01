import abc as _abc
import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import pathlib as _pl
import logging as _log

import openstack as _ost
import openstack.connection as _oconn
import resultes_openstack_utils.clouds_yaml as _cyaml

_LOG = _log.getLogger(__name__)


class AbstractRunnerManager(_abc.ABC):
    @property
    @_abc.abstractmethod
    def n_max_jobs_per_runner(self) -> int:
        raise NotImplementedError()

    @_abc.abstractmethod
    async def create_server_and_get_ip(self) -> str:
        raise NotImplementedError()

    @_abc.abstractmethod
    def delete_servers(self, ip_address: str | None = None) -> None:
        raise NotImplementedError()


class RunnerManager(AbstractRunnerManager):
    _NETWORK_NAME = "k8s-clusterapi-cluster-pck-cfedjc3-pck-cfedjc3"

    def __init__(
        self, os_password: str, clouds_yaml_file_path: _pl.Path | None = None
    ) -> None:
        self._os_password = os_password
        self._clouds_yaml_file_path = (
            clouds_yaml_file_path
            if clouds_yaml_file_path
            else _cyaml.get_clouds_yaml_file_path()
        )

    @property
    def n_max_jobs_per_runner(self) -> int:
        return 8

    async def create_server_and_get_ip(self) -> str:
        with self._create_connection() as connection:
            image = connection.image.find_image("build-image-server")
            flavor = connection.compute.find_flavor("a8-ram16-disk50-perf1")

            network = connection.network.find_network(self._NETWORK_NAME)

            server = connection.compute.create_server(
                name="runner",
                image_id=image.id,
                flavor_id=flavor.id,
                networks=[{"uuid": network.id}],
                security_groups=[{"name": "runner"}],
                availability_zone="az-2",
            )

        async with _asyncio.timeout(delay=120):
            while True:
                server = connection.compute.find_server(server.id)

                if server.addresses:
                    return self._get_ip_address(server)

                await _asyncio.sleep(delay=5.0)

    def _get_ip_address(self, server) -> str:
        ip_address = server.addresses[self._NETWORK_NAME][0]["addr"]
        return ip_address

    def delete_servers(self, ip_address: str | None = None) -> None:
        return
    
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
                ip_address = self._get_ip_address(server)
                _LOG.info(
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


class DummyRunnerManager(AbstractRunnerManager):
    def __init__(self, ip_address: str, n_max_jobs_per_runner: int) -> None:
        self._ip_address = ip_address
        self._n_max_jobs_per_runner = n_max_jobs_per_runner

    @property
    def n_max_jobs_per_runner(self) -> int:
        return self._n_max_jobs_per_runner

    async def create_server_and_get_ip(self) -> str:
        return self._ip_address

    def delete_servers(self, ip_address: str | None = None) -> None:
        return
