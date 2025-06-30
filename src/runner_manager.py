import abc as _abc
import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import os as _os

import openstack as _ost
import openstack.connection as _oconn


class AbstractRunnerManager(_abc.ABC):
    @_abc.abstractmethod
    async def create_server_and_get_ip(self) -> str:
        raise NotImplementedError()

    @_abc.abstractmethod
    def delete_servers(self, ip_address: str | None = None) -> None:
        raise NotImplementedError()


class RunnerManager(AbstractRunnerManager):
    @_ctx.contextmanager
    @staticmethod
    def create_connection() -> _cabc.Iterator[_oconn.Connection]:
        os_password = _os.environ["OS_PASSWORD"]
        connection = _ost.connect("openstack", os_password=os_password)
        yield connection
        connection.close()

    async def create_server_and_get_ip(self) -> str:
        with self.create_connection() as connection:
            image = connection.image.find_image("build-image-server")
            flavor = connection.compute.find_flavor("a8-ram16-disk50-perf1")

            network_name = "k8s-clusterapi-cluster-pck-cfedjc3-pck-cfedjc3"
            network = connection.network.find_network(network_name)

            server = connection.compute.create_server(
                name="runner",
                image_id=image.id,
                flavor_id=flavor.id,
                networks=[{"uuid": network.id}],
                security_groups=[{"name": "runner"}],
                availability_zone="az-2",
            )

        async with _asyncio.timeout(delay=60):
            while True:
                server = connection.compute.find_server(server.id)

                if server.addresses:
                    ip_address = server.addresses[network_name][0]["addr"]
                    return ip_address

                await _asyncio.sleep(delay=5.0)

    def delete_servers(self, ip_address: str | None = None) -> None:
        with create_connection() as connection:
            kwargs = {name: "runner"}
            if ip_address:
                kwargs["ip"] = ip_address

            servers = connection.compute.servers(**kwargs)
            for server in servers:
                connection.compute.delete_server(server)


class DummyRunnerManager(AbstractRunnerManager):
    def __init__(self, ip_address: str) -> None:
        self._ip_address = ip_address

    async def create_server_and_get_ip(self) -> str:
        return self._ip_address

    def delete_servers(self, ip_address: str | None = None) -> None:
        return
