#!/usr/bin/env python3

import io
import os
import stat
import errno
import fuse
import kubernetes
import importlib
import yaml
from kubernetes.client import ApiextensionsV1Api
from kubernetes.client import ApiextensionsV1Api, ApisApi, CoreV1Api
from kubernetes.client import ApiClient, Configuration, DiscoveryV1Api
from kubernetes.dynamic import DynamicClient
from kubernetes.client import ApiregistrationV1Api
from kubernetes.client import CoreV1Api, ApiextensionsV1Api
from kubernetes.client import ApiextensionsV1Api, ApisApi, ResourceApi
from functools import lru_cache

class KubernetesFUSE(fuse.Operations):
    def __init__(self, kubeconfig):
        self.write_buffers = {}
        self.kubeconfig = kubeconfig
        self.client = self._create_client()
        self.dynamic_client = DynamicClient(self.client)
        self.discovery_client = kubernetes.client.DiscoveryV1Api(self.client)

    def _create_client(self):
        kubernetes.config.load_kube_config()
        return ApiClient()

    @lru_cache
    def _get_groups(self):
        api_instance = kubernetes.client.ApisApi(self.client)
        api_response = api_instance.get_api_versions()
        print(api_response)
        return {group.name: group for group in api_response.groups}

    @lru_cache
    def _get_versions(self, group):
        g = self._get_groups()[group]
        return {version.version: version for version in g.versions}


    @lru_cache
    def _get_kinds(self, group, version=None):
        kinds = []

        # Retrieve resource kinds for the specified API group using the Kubernetes API server
        if group == "":
            # Handle kubernetes/api/v1
            api_instance = CoreV1Api(self.client)
            resources = api_instance.get_api_resources()
            kinds.extend([resource.kind for resource in resources.resources if not resource.namespaced])
        else:
            g = self._get_groups()[group]
            v = self._get_versions(group)[version] if version else g.preferred_version
            group_response = self._query_api(v.group_version)
            print(group_response)
            for resource in group_response['resources']:
                if "kind" in resource:
                    kinds.append(resource['kind'])

        return set(kinds)

    @lru_cache
    def _get_namespaces(self):
        api_instance = CoreV1Api(self.client)
        print(api_instance.list_namespace().items)
        return [namespace.metadata.name for namespace in api_instance.list_namespace().items]

    def _query_api(self, api_group):
        api_client = self.client
        headers = {}

        url = f"/apis/{api_group}"
        response = api_client.call_api(
            url,
            "GET",
            auth_settings=["BearerToken"],
            response_type="object",
            _return_http_data_only=True,
        )

        return response
    def _get_resources(self, group, version, kind, namespace=None):
        api_client = self.client
        headers = {}

        if namespace:
            url = f"/apis/{group}/{version}/namespaces/{namespace}/{kind.lower()}s"
        else:
            url = f"/apis/{group}/{version}/{kind.lower()}s"

        response = api_client.call_api(
            url,
            "GET",
            auth_settings=["BearerToken"],
            response_type="object",
            _return_http_data_only=True,
        )

        resources = {}
        for item in response["items"]:
            resource_name = item["metadata"]["name"]
            resources[resource_name] = item

        return resources

    def _get_resource(self, group, version, kind, namespace, resource_name):
        api_client = self.client
        headers = {}

        url = f"/apis/{group}/{version}/namespaces/{namespace}/{kind.lower()}s/{resource_name}"
        response = api_client.call_api(
            url,
            "GET",
            auth_settings=["BearerToken"],
            response_type="object",
            _return_http_data_only=True,
        )

        return response

    def _minimal_resource(self, group, version, kind, namespace, resource_name):
        return {
            "apiVersion": f"{group}/{version}",
            "kind": kind,
            "metadata": {
                "name": resource_name,
                "namespace": namespace
            }
        }

    def getattr(self, path, fh=None):
        if path == '/':
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)

        parts = path.strip('/').split('/')
        if len(parts) == 1 and parts[0] in self._get_groups():
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(parts) == 2 and parts[1] in self._get_versions(parts[0]):
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(parts) == 3 and parts[2] in self._get_kinds(parts[0], parts[1]):
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(parts) == 4 and parts[3] in self._get_namespaces():
            return dict(st_mode=(stat.S_IFDIR | 0o755), st_nlink=2)
        elif len(parts) == 5 and parts[4] in self._get_resources(parts[0], parts[1], parts[2], parts[3]):
            return dict(st_mode=(stat.S_IFREG | 0o644), st_size=len(yaml.dump(self._get_resource(parts[0], parts[1], parts[2], parts[3], parts[4]))))

        raise fuse.FuseOSError(errno.ENOENT)

    def readdir(self, path, fh):
        dirents = ['.', '..']
        parts = path.strip('/').split('/')

        if not parts or not parts[0]:
            dirents.extend(self._get_groups().keys())
        elif len(parts) == 1:
            group, = parts
            print(group)
            dirents.extend(self._get_versions(group).keys())
            print(dirents)
        elif len(parts) == 2:
            group, version = parts
            print(group, version)
            dirents.extend(self._get_kinds(group, version))
            print(dirents)
        elif len(parts) == 3:
            group, version, kind = parts
            namespaces = self._get_namespaces()
            dirents.extend(namespaces)
        elif len(parts) == 4:
            group, version, kind, namespace = parts
            resources = self._get_resources(group, version, kind, namespace)
            dirents.extend(resources.keys())

        return dirents

    def read(self, path, size, offset, fh):
        parts = path.strip('/').split('/')
        if len(parts) == 5:
            group, version, kind, namespace, resource_name = parts
            resource = self._get_resource(group, version, kind, namespace, resource_name)
            if resource:
                data = yaml.dump(resource)
                return data[offset:offset + size].encode('utf-8')

        raise fuse.FuseOSError(errno.ENOENT)
    def _update_resource(self, group, version, kind, namespace, resource_name, data):
        api_client = self.client
        headers = {}

        url = f"/apis/{group}/{version}/namespaces/{namespace}/{kind.lower()}s/{resource_name}"
        response = api_client.call_api(
            url,
            "PUT",
            auth_settings=["BearerToken"],
            body=data,
            response_type="object",
            _return_http_data_only=True,
        )

        return response

    def _create_resource(self, group, version, kind, namespace, data):
        api_client = self.client
        headers = {}

        url = f"/apis/{group}/{version}/namespaces/{namespace}/{kind.lower()}s"
        response = api_client.call_api(
            url,
            "POST",
            auth_settings=["BearerToken"],
            body=data,
            response_type="object",
            _return_http_data_only=True,
        )

        return response

    def write(self, path, data, offset, fh):
        print(f"write: {path} {offset} {data}")
        if path not in self.write_buffers:
            self.write_buffers[path] = bytearray()

        self.write_buffers[path][offset:offset + len(data)] = data
        return len(data)

    def open(self, path, flags):
        parts = path.strip('/').split('/')
        print(f"open: {parts}")
        if len(parts) == 5:
            group, version, kind, namespace, resource_name = parts
            resource = self._get_resource(group, version, kind, namespace, resource_name)
            if resource:
                if flags & os.O_WRONLY or flags & os.O_RDWR:
                    # File is opened for writing
                    return 0
                else:
                    # File is opened for reading
                    return 0
        print(f"open: {parts} failed")
        raise fuse.FuseOSError(errno.ENOENT)

    def release(self, path, fh):
        print(f"release: {path}")
        if path in self.write_buffers:
            data = io.BytesIO(self.write_buffers[path])
            parts = path.strip('/').split('/')
            if len(parts) == 5:
                group, version, kind, namespace, resource_name = parts
                resource = self._get_resource(group, version, kind, namespace, resource_name)
                if resource:
                    updated_data = yaml.safe_load(data)
                    self._update_resource(group, version, kind, namespace, resource_name, updated_data)
                    del self.write_buffers[path]
                    return 0

        raise fuse.FuseOSError(errno.ENOENT)

    def _merge_dicts(self, dict1, dict2):
        for k, v in dict2.items():
            if isinstance(v, dict):
                dict1[k] = self._merge_dicts(dict1.get(k, {}), v)
            else:
                dict1[k] = v
        return dict1
    def truncate(self, path, length, fh=None):
        print(f"truncate: {path} {length}")
        parts = path.strip('/').split('/')
        if len(parts) == 5:
            group, version, kind, namespace, resource_name = parts
            resource = self._get_resource(group, version, kind, namespace, resource_name)
            if resource:
                data = yaml.dump(resource)
                truncated_data = data[:length]
                updated_data = yaml.safe_load(truncated_data) or {}
                minimal_resource = self._minimal_resource(group, version, kind, namespace, resource_name)
                merged_data = {**minimal_resource, **updated_data}
                try:
                    self._update_resource(group, version, kind, namespace, resource_name, merged_data)
                finally:
                    # most truncates in practice are editors, which will write the file back immediately...
                    # TODO: not this
                    return 0

    def create(self, path, mode):
        self.write_buffers[path] = bytearray()
        return 0


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('mountpoint', help='Path to the mount point')
    args = parser.parse_args()

    fuse.FUSE(KubernetesFUSE(os.getenv("KUBECONFIG",'/home/oz/.kube/config')), args.mountpoint, nothreads=True, foreground=True, rw=True)
