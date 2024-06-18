from __future__ import annotations

import json
import os
from getpass import getpass
from pathlib import Path
from typing import Dict, NamedTuple, Optional, Union

from elabapi_python import Configuration
from tabulate import tabulate

from elabftwcontrol.defaults import DEFAULT_CONFIG_FILE

Pathlike = Union[str, Path]


class AccessConfig(NamedTuple):
    host_url: str
    api_key: str
    verify_ssl: bool
    debug: bool = False

    def get_api_config(self) -> Configuration:
        configuration = Configuration()
        configuration.api_key["api_key"] = self.api_key
        configuration.api_key_prefix["api_key"] = "Authorization"
        configuration.host = self.host_url
        configuration.debug = self.debug
        configuration.verify_ssl = self.verify_ssl
        return configuration


class MultiConfig(NamedTuple):
    data: Dict[str, AccessConfig]

    def has_profile(self, profile: str) -> bool:
        return profile in self.data

    def get_profile(self, profile: str) -> AccessConfig:
        return self.data[profile]

    def set_profile(self, profile: str, accessconfig: AccessConfig) -> None:
        self.data[profile] = accessconfig

    def delete_profile(self, profile: str) -> None:
        del self.data[profile]

    @classmethod
    def from_file(cls, filepath: Pathlike) -> MultiConfig:
        """Read data from a json file"""
        data: Dict[str, AccessConfig] = {}
        with open(filepath, "r") as f:
            for profile, access_parameters in json.load(f).items():
                data[profile] = AccessConfig(**access_parameters)
        return cls(data)

    def to_file(self, filepath: Pathlike) -> None:
        """Export configuration to a json file"""
        with open(filepath, "w") as f:
            f.write(
                json.dumps(
                    {
                        profile: accessconfig._asdict()
                        for profile, accessconfig in self.data.items()
                    }
                )
            )


def yes_no(
    prompt: str,
    default_yes: bool = True,
) -> bool:
    if default_yes:
        return not input(f"{prompt} [Y|n]").lower().strip().startswith("n")
    else:
        return input(f"{prompt} [y|N]").lower().strip().startswith("y")


def create_or_append_configuration_file(
    filepath: Pathlike,
    profile: Optional[str] = None,
    show_keys: bool = False,
) -> None:
    if profile is None:
        profile = "default"
    pass_prompt = "Enter API key: "
    if show_keys:
        api_key = input(pass_prompt).strip()
    else:
        api_key = getpass(pass_prompt).strip()

    host = input("Enter elabAPI endpoint: ").strip()

    verify_ssl = yes_no("Verify SSL?", default_yes=False)

    debug = yes_no("Debug mode?", default_yes=False)

    new_config = AccessConfig(
        host_url=host,
        api_key=api_key,
        verify_ssl=verify_ssl,
        debug=debug,
    )

    if Path(filepath).exists():
        config = MultiConfig.from_file(filepath)
    else:
        config = MultiConfig({})

    if config.has_profile(profile):
        if not yes_no(f"Overwrite existing profile '{profile}'?", default_yes=False):
            print("Aborted.")
            return

    config.set_profile(profile, new_config)
    config.to_file(filepath)

    print(f"Wrote configuration to {filepath}.")


def delete_configuration_file(
    filepath: Pathlike,
    profile: Optional[str] = None,
    show_keys: bool = False,
) -> None:
    if Path(filepath).exists():
        config = MultiConfig.from_file(filepath)
    else:
        print("Config file does not exist.")
        return

    if profile is None:
        if not yes_no(f"Delete config file '{filepath}'?", default_yes=False):
            print("Aborted.")
            return
        os.remove(filepath)
        print(f"Deleted configuration file {filepath}")
    else:
        if not config.has_profile(profile):
            print(f"Profile {profile} does not exist.")
        if not yes_no(f"Delete config profile '{profile}'?", default_yes=False):
            print("Aborted.")
            return
        config.delete_profile(profile)
        config.to_file(filepath)
        print(f"Deleted configuration profile {profile}")


def list_config_profiles(
    filepath: Pathlike,
    profile: Optional[str] = None,
    show_keys: bool = False,
) -> None:
    if not Path(filepath).exists():
        print(f"Config file '{filepath}' does not exist. Can not list profiles.")
        return

    config = MultiConfig.from_file(filepath)
    table = []
    for read_profile, single_config in config.data.items():
        if profile is None or profile == read_profile:
            key = single_config.api_key if show_keys else "***"
            row = [
                read_profile,
                single_config.host_url,
                key,
                single_config.verify_ssl,
                single_config.debug,
            ]
            table.append(row)
    print(
        tabulate(
            table,
            headers=["Profile", "Endpoint", "API key", "Verify SSL", "Debug mode"],
        )
    )


def main() -> None:
    create_or_append_configuration_file(DEFAULT_CONFIG_FILE)


if __name__ == "__main__":
    main()
