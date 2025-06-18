import collections.abc as _cabc
import contextlib as _ctx
import typing as _tp

import neutronclient.v2_0.client as _ntv2c
import novaclient.client as _nvc
import novaclient.v2.client as _nvv2c
import resultes_openstack_utils.clouds_yaml as _cyaml
import resultes_openstack_utils.keystone as _ks

VERSION = "2.1"


def test_get_servers() -> None:
    with create_nova_client() as client:
        servers = client.servers.list()
        print(servers)


def test_create_server() -> None:
    with create_nova_client() as nova:
        flavor = nova.flavors.find(name="a8-ram16-disk50-perf1")
        image = nova.glance.find_image("build-image-server")

        with create_neutron_client() as neutron:
            network = _get_network(neutron)

            nic = {"net-id": network["id"]}

        server = nova.servers.create(
            name="runner",
            image=image,
            flavor=flavor,
            nics=[nic],
            security_groups=["runner"],
            availability_zone="az-2"
        )

        print(server)


def test_delete_server() -> None:
    with create_nova_client() as nova:
        server = nova.servers.find(
            name="runner",
        )
        
        nova.servers.delete(server)


def _get_network(client: _ntv2c.Client) -> _cabc.Mapping[str, _tp.Any]:
    networks = client.list_networks(
        name="k8s-clusterapi-cluster-pck-cfedjc3-pck-cfedjc3"
    )
    network = networks["networks"][0]
    return network


def _get_security_group(client: _ntv2c.Client) -> _cabc.Mapping[str, _tp.Any]:
    security_groups = client.list_security_groups(name="runner")
    security_group = security_groups["security_groups"][0]
    return security_group


def test_get_neutron_dicts():
    with create_neutron_client() as client:
        network = _get_network(client)
        print(network)

        security_group = _get_security_group(client)
        print(security_group)


@_ctx.contextmanager
def create_nova_client() -> _cabc.Iterator[_nvv2c.Client]:
    with _get_openstack_clients_kwargs() as kwargs:
        client: _nv2c.Client = _nvc.Client(
            VERSION,
            **kwargs,
        )

        yield client


@_ctx.contextmanager
def create_neutron_client() -> _cabc.Iterator[_ntv2c.Client]:
    with _get_openstack_clients_kwargs() as kwargs:
        client = _ntv2c.Client(**kwargs)
        yield client


@_ctx.contextmanager
def _get_openstack_clients_kwargs() -> _cabc.Iterator[_cabc.Mapping[str, _tp.Any]]:
    data = _cyaml.get_clouds_yaml_openstack_json()
    auth = _ks.create_password()

    with _ks.create_session(auth) as session:
        kwargs = {"session": session, "region_name": data["region_name"]}
        yield kwargs


if __name__ == "__main__":
    test_create_server()
