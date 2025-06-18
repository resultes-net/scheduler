import asyncio as _asyncio
import collections.abc as _cabc
import contextlib as _ctx
import os as _os

import openstack as _ost
import openstack.connection as _oconn
import pytest as _pt

VERSION = "2.1"


@_ctx.contextmanager
def create_connection() -> _cabc.Iterator[_oconn.Connection]:
    os_password = _os.environ["OS_PASSWORD"]
    connection = _ost.connect("openstack", os_password=os_password)
    yield connection
    connection.close()


def test_get_connection() -> None:
    with create_connection() as connection:
        pass


def test_get_servers() -> None:
    with create_connection() as connection:
        servers = connection.compute.servers()
        for server in servers:
            print(server)

            for network_name, addresses in server.addresses.items():
                address = addresses[0]["addr"]
                print(address)


@_pt.mark.asyncio
async def test_create_server() -> None:
    with create_connection() as connection:
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
                print(ip_address)
                return

            await _asyncio.sleep(delay=5.0)


def test_delete_servers() -> None:
    with create_connection() as connection:
        servers = connection.compute.servers(name="runner")
        for server in servers:
            print(f"Deleting server {server.id}")
            connection.compute.delete_server(server)
