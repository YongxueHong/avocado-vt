# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
#
# See LICENSE for more details.
#
# Copyright: Red Hat Inc. 2025
# Authors: Yongxue Hong <yhong@redhat.com>

"""
Module for providing the interface of cluster for virt test.
"""

import os
import json # Replaced pickle
import uuid
import fcntl # For file locking
import logging # Added for logging errors
import copy # For deepcopy

from virttest import data_dir

LOG = logging.getLogger(__name__)


class ClusterError(Exception):
    """The generic cluster error."""
    pass


class _Partition(object):
    """The representation of the partition of the cluster."""

    def __init__(self, uuid_hex=None):
        self._uuid = uuid_hex or uuid.uuid4().hex
        self._pools = dict() # Remains unused in current logic, as per review
        self._nodes = set() # Stores actual Node objects

    @property
    def pools(self):
        return self._pools

    @property
    def uuid(self):
        return self._uuid

    def add_node(self, node):
        """
        Add the node into the partition.

        :param node: The node to be added.
        :type node: object (expected to have a .tag attribute)
        """
        self._nodes.add(node)

    def del_node(self, node):
        """
        Delete the node from the partition.

        :param node: The node to be deleted.
        :type node: object
        """
        self._nodes.discard(node) # Use discard to avoid error if not present

    @property
    def nodes(self):
        return self._nodes

    def to_dict(self):
        """Converts the partition to a JSON-serializable dictionary."""
        return {
            "uuid": self._uuid,
            "pools": self._pools,
            "nodes": [node.tag for node in self._nodes if hasattr(node, 'tag')]
        }

    @classmethod
    def from_dict(cls, p_dict, cluster_nodes_map):
        """
        Creates a _Partition instance from a dictionary.

        :param p_dict: Dictionary representation of the partition.
        :param cluster_nodes_map: A dict mapping node tags to Node objects.
        :return: A _Partition instance.
        """
        partition = cls(uuid_hex=p_dict.get("uuid"))
        # partition._pools = p_dict.get("pools", {}) # If pools become used

        node_tags = p_dict.get("nodes", [])
        for tag in node_tags:
            node = cluster_nodes_map.get(tag)
            if node:
                partition.add_node(node)
            else:
                LOG.warning("Node with tag '%s' not found in cluster map while"
                            " reconstructing partition %s.", tag, partition.uuid)
        return partition


class _Cluster(object):
    """The representation of the cluster."""

    def __init__(self):
        self._filename = os.path.join(data_dir.get_base_backend_dir(), "cluster_env.json")
        self._lock_filename = self._filename + ".lock"
        self._lock_file_handle = None # Changed variable name for clarity

        self._empty_data = {
            "logger_server_host": "",
            "logger_server_port": 0,
            "partitions": [],
        }

        self._data = {}
        self._data["nodes"] = {}

        self._load()


    def _acquire_lock(self):
        # Ensure lock file exists for flock, then open it.
        # Using 'w' mode for lock file itself isn't standard for flock; flock locks the file descriptor.
        # A common pattern is to open the main data file (or a dedicated .lock file) and lock that descriptor.
        # Let's use self._filename itself for locking, which is simpler if the OS supports it well.
        # If not, a separate self._lock_filename opened appropriately is better.
        # For simplicity with fcntl, will lock self._filename.
        # However, can't hold self._filename open for read/write and also for lock easily.
        # So, using a separate lock file is more robust.

        self._lock_file_handle = open(self._lock_filename, 'w') # Open for writing to create if not exists
        try:
            fcntl.flock(self._lock_file_handle.fileno(), fcntl.LOCK_EX)
        except (IOError, OSError) as e:
            LOG.error("Failed to acquire lock on %s: %s", self._lock_filename, e)
            if self._lock_file_handle:
                self._lock_file_handle.close()
                self._lock_file_handle = None
            raise ClusterError(f"Failed to acquire cluster lock: {e}")


    def _release_lock(self):
        if self._lock_file_handle:
            try:
                fcntl.flock(self._lock_file_handle.fileno(), fcntl.LOCK_UN)
                self._lock_file_handle.close()
            except (IOError, OSError) as e:
                LOG.error("Failed to release lock on %s: %s", self._lock_filename, e)
            finally:
                self._lock_file_handle = None

    def _save(self):
        self._acquire_lock()
        try:
            data_to_serialize = {
                "logger_server_host": self._data.get("logger_server_host", ""),
                "logger_server_port": self._data.get("logger_server_port", 0),
                "partitions": [p.to_dict() for p in self._data.get("partitions", [])],
            }
            with open(self._filename, "w") as f:
                json.dump(data_to_serialize, f, indent=4)
        except (IOError, TypeError) as e:
            LOG.error("Failed to save cluster data to %s: %s", self._filename, e)
        finally:
            self._release_lock()

    def _load(self):
        self._acquire_lock()
        try:
            if not os.path.isfile(self._filename):
                self._data = copy.deepcopy(self._empty_data)
                # self._data["nodes"] is already initialized and should persist across loads within same _Cluster instance
                if "nodes" not in self._data: self._data["nodes"] = {} # ensure it
                if "partitions" not in self._data: self._data["partitions"] = []
                return

            with open(self._filename, "r") as f:
                try:
                    loaded_json_data = json.load(f)
                except json.JSONDecodeError as e:
                    LOG.error("Failed to decode JSON from %s: %s. Initializing with empty data.", self._filename, e)
                    self._data = copy.deepcopy(self._empty_data)
                    if "nodes" not in self._data: self._data["nodes"] = {}
                    if "partitions" not in self._data: self._data["partitions"] = []
                    return

            self._data["logger_server_host"] = loaded_json_data.get("logger_server_host", "")
            self._data["logger_server_port"] = loaded_json_data.get("logger_server_port", 0)

            raw_partitions_data = loaded_json_data.get("partitions", [])
            current_partitions = []
            for p_dict in raw_partitions_data:
                # self._data["nodes"] is the map of tag -> Node object
                partition_obj = _Partition.from_dict(p_dict, self._data["nodes"])
                current_partitions.append(partition_obj)
            self._data["partitions"] = current_partitions

        except IOError as e:
            LOG.error("Failed to load cluster data from %s: %s. Initializing with empty data.", self._filename, e)
            self._data = copy.deepcopy(self._empty_data)
            if "nodes" not in self._data: self._data["nodes"] = {}
            if "partitions" not in self._data: self._data["partitions"] = []
        finally:
            self._release_lock()


    def cleanup_env(self):
        self._acquire_lock()
        try:
            self._data = copy.deepcopy(self._empty_data)
            self._data["nodes"] = {}
            if "partitions" not in self._data: self._data["partitions"] = []

            if os.path.isfile(self._filename):
                try:
                    os.unlink(self._filename)
                except OSError as e:
                    LOG.error("Failed to delete cluster file %s: %s", self._filename, e)
            # Do not save here, cleanup means the file is gone. Next load will use empty.
        finally:
            self._release_lock()


    def register_node(self, name, node):
        if not hasattr(node, 'tag') or name != node.tag:
            LOG.warning("Registering node with name '%s' but its tag property might be '%s' or missing.", name, getattr(node, 'tag', 'N/A'))

        # No lock here as this only modifies in-memory self._data["nodes"].
        # If this registration should also trigger a save of partitions (e.g. if a node was added to one implicitly)
        # then locking and saving would be needed. Current model assumes explicit partition management.
        self._data["nodes"][name] = node


    def unregister_node(self, name):
        node_to_remove = self._data["nodes"].pop(name, None)
        if node_to_remove:
            partitions_changed = False
            for partition in self._data.get("partitions", []):
                if node_to_remove in partition.nodes: # Check if node is in partition
                    partition.del_node(node_to_remove)
                    partitions_changed = True
            if partitions_changed:
                self._save() # Save if partitions were modified
        else:
            LOG.warning("Attempted to unregister non-existent node: %s", name)


    def get_node_by_tag(self, tag):
        return self._data["nodes"].get(tag)

    def get_node(self, name):
        return self._data["nodes"].get(name)

    def get_all_nodes(self):
        return list(self._data["nodes"].values())

    def assign_logger_server_host(self, host="localhost"):
        if self._data.get("logger_server_host") != host:
            self._data["logger_server_host"] = host
            self._save()

    @property
    def logger_server_host(self):
        return self._data.get("logger_server_host")

    def assign_logger_server_port(self, port=9999):
        if self._data.get("logger_server_port") != port:
            self._data["logger_server_port"] = port
            self._save()

    @property
    def logger_server_port(self):
        return self._data.get("logger_server_port")

    @property
    def metadata_file(self):
        return os.path.join(data_dir.get_base_backend_dir(), "cluster_metadata.json")

    def create_partition(self):
        partition = _Partition()
        if "partitions" not in self._data or not isinstance(self._data["partitions"], list):
            self._data["partitions"] = []
        self._data["partitions"].append(partition)
        self._save()
        return partition

    def clear_partition(self, partition_to_clear):
        if "partitions" in self._data and isinstance(self._data["partitions"], list):
            original_len = len(self._data["partitions"])
            self._data["partitions"] = [p for p in self._data["partitions"] if p.uuid != partition_to_clear.uuid]
            if len(self._data["partitions"]) < original_len:
                self._save()
            else:
                LOG.warning("Partition with UUID %s not found for clearing.", partition_to_clear.uuid)

    @property
    def free_nodes(self):
        all_nodes_set = set(self.get_all_nodes())
        nodes_in_partitions = set()
        for p in self._data.get("partitions", []):
            nodes_in_partitions.update(p.nodes)

        return list(all_nodes_set - nodes_in_partitions)

    @property
    def partition(self):
        current_partitions = self._data.get("partitions", [])
        if not current_partitions:
            # Raise error or return None based on stricter contract.
            # For now, to match old behavior of potential IndexError:
            # return current_partitions[0] # This would fail if empty.
            # A safer approach:
            raise ClusterError("No partition available in the current context.")

        if len(current_partitions) > 1:
            LOG.warning("Multiple partitions found (%s), but 'partition' property"
                        " accesses only the first one by design.", len(current_partitions))

        return current_partitions[0]


cluster = _Cluster()
