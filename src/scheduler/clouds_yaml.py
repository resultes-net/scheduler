import pathlib as _pl

clouds_yaml_file_path = (
    _pl.Path(__file__).parents[2] / "config" / "secrets" / "openstack-clouds.yaml"
)
