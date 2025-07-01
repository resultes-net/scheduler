import pytest as _pt

import runner_manager as _run

VERSION = "2.1"


def test_get_connection() -> None:
    with _run.create_connection() as connection:
        pass


def test_get_servers() -> None:
    with _run.create_connection() as connection:
        servers = connection.compute.servers()
        for server in servers:
            print(server)

            for network_name, addresses in server.addresses.items():
                address = addresses[0]["addr"]
                print(address)


@_pt.mark.asyncio
async def test_create_server() -> None:
    await _run.create_server_and_get_ip()


def test_delete_servers() -> None:
    _run.delete_servers()
