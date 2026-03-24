import glob
import inspect
import os
import tempfile
import testflows.settings as settings
import threading
import time
from testflows.asserts import error
from testflows.connect import Shell as ShellBase
from testflows.core import *
from testflows.uexpect import ExpectTimeoutError

import docker
from testcontainers.core.container import DockerContainer
from testcontainers.core.network import Network

MESSAGES_TO_RETRY = [
    "DB::Exception: ZooKeeper session has been expired",
    "DB::Exception: Connection loss",
    "Coordination::Exception: Session expired",
    "Coordination::Exception: Connection loss",
    "Coordination::Exception: Operation timeout",
    "DB::Exception: Operation timeout",
    "Operation timed out",
    "ConnectionPoolWithFailover: Connection failed at try",
    "DB::Exception: New table appeared in database being dropped or detached. Try again",
    "is already started to be removing by another replica right now",
    # happens in SYSTEM SYNC REPLICA query if session with ZooKeeper is being reinitialized.
    "Shutdown is called for table",
    # distributed TTL timeout message
    "is executing longer than distributed_ddl_task_timeout"
]


class Shell(ShellBase):
    def __exit__(self, shell_type, shell_value, traceback):
        # send exit and Ctrl-D repeatedly
        # to terminate any open shell commands.
        # This is needed for example
        # to solve a problem with
        # 'docker exec {name} bash --noediting'
        # that does not clean up open bash processes
        # if not exited normally
        for i in range(10):
            if self.child is not None:
                try:
                    self.send('exit\r', eol='')
                    self.send('\x04\r', eol='')
                except OSError:
                    pass
        return super(Shell, self).__exit__(shell_type, shell_value, traceback)


class QueryRuntimeException(Exception):
    """Exception during query execution on the server.
    """
    pass


class Node(object):
    """Generic cluster node.
    """
    config_d_dir = "/etc/clickhouse-server/config.d/"

    def __init__(self, cluster, node_name):
        self.cluster = cluster
        self.name = node_name

    def repr(self):
        return f"Node(name='{self.name}')"

    def close_bashes(self):
        """Close all active bashes to the node.
        """
        with self.cluster.lock:
            for key in list(self.cluster.shells.keys()):
                if key.endswith(f"-{self.name}"):
                    shell = self.cluster.shells.pop(key)
                    shell.__exit__(None, None, None)

    def restart(self, timeout=300, num_retries=5, safe=True):
        """Restart node.
        """
        self.close_bashes()

        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker restart {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

    def start(self, timeout=300, num_retries=5):
        """Start node.
        """
        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker start {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

    def stop(self, timeout=300, num_retries=5, safe=True):
        """Stop node.
        """
        self.close_bashes()

        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker stop {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

    def command(self, *nargs, **kwargs):
        return self.cluster.command(self.name, *nargs, **kwargs)

    def cmd(self, cmd, expect_message=None, exitcode=0, steps=True, shell_command="bash --noediting", no_checks=False,
            step=By, *nargs, **kwargs):

        command = f"{cmd}"
        with step("executing command", description=command, format_description=False) if steps else NullStep():
            try:
                r = self.cluster.bash(self.name, command=shell_command)(command, *nargs, **kwargs)
            except ExpectTimeoutError:
                self.cluster.close_bash(self.name)
                raise

        if no_checks:
            return r

        if exitcode is not None:
            with Then(f"exitcode should be {exitcode}") if steps else NullStep():
                assert r.exitcode == exitcode, error(r.output)

        if expect_message is not None:
            with Then(f"output should contain message", description=expect_message) if steps else NullStep():
                assert expect_message in r.output, error(r.output)

        return r


class ClickHouseNode(Node):
    """Node with ClickHouse server.
    """

    def wait_healthy(self, timeout=300):
        with By(f"waiting until container {self.name} is healthy"):
            self.cluster.node_wait_healthy(self.name)
            start_time = time.time()
            while True:
                # sleep before to avoid false positive during /docker-entrypoint.initdb.d/ processing
                time.sleep(1)
                if self.query("select version()", no_checks=True, timeout=300, steps=False).exitcode == 0:
                    break
                if time.time() - start_time < timeout:
                    time.sleep(3)
                    continue
                assert False, "container is not healthy"

    def stop(self, timeout=300, safe=True, num_retries=5):
        """Stop node.
        """
        if safe:
            self.query("SYSTEM STOP MOVES")
            self.query("SYSTEM STOP MERGES")
            self.query("SYSTEM FLUSH LOGS")
            with By("waiting for 5 sec for moves and merges to stop"):
                time.sleep(5)
            with And("forcing to sync everything to disk"):
                self.command("sync", timeout=300)

        self.close_bashes()

        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker stop {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

    def start(self, timeout=300, wait_healthy=True, num_retries=5):
        """Start node.
        """
        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker start {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

        if wait_healthy:
            self.wait_healthy(timeout)

    def restart(self, timeout=300, safe=True, wait_healthy=True, num_retries=5):
        """Restart node.
        """
        if safe:
            self.query("SYSTEM STOP MOVES")
            self.query("SYSTEM STOP MERGES")
            self.query("SYSTEM FLUSH LOGS")
            with By("waiting for 5 sec for moves and merges to stop"):
                time.sleep(5)
            with And("forcing to sync everything to disk"):
                self.command("sync", timeout=300)

        self.close_bashes()

        for _ in range(num_retries):
            r = self.cluster.command(None, f'docker restart {self.cluster.get_container_id(self.name)}', timeout=timeout)
            if r.exitcode == 0:
                break

        if wait_healthy:
            self.wait_healthy(timeout)

    def hash_query(self, sql, hash_utility="sha1sum", steps=True, step=By,
                   query_settings=None, secure=False, *nargs, **kwargs):
        """Execute sql query inside the container and return the hash of the output.

        :param secure:
        :param step:
        :param steps:
        :param query_settings:
        :param sql: sql query
        :param hash_utility: hash function which used to compute hash
        """
        query_settings = list(query_settings or [])
        query_settings = list(query_settings)

        if hasattr(current().context, "default_query_settings"):
            query_settings += current().context.default_query_settings

        client = "clickhouse client -n"
        if secure:
            client += " -s"

        if len(sql) > 1024:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8") as query:
                query.write(sql)
                query.flush()
                command = f"set -o pipefail && cat \"{query.name}\" | {self.cluster.docker_exec(self.name)} {client} | {hash_utility}"
                for setting in query_settings:
                    setting_name, setting_value = setting
                    command += f" --{setting_name} \"{setting_value}\""
                description = f"""
                            echo -e \"{sql[:100]}...\" > {query.name}
                            {command}
                        """
                with step("executing command", description=description,
                          format_description=False) if steps else NullStep():
                    try:
                        r = self.cluster.bash(None)(command=command, *nargs, **kwargs)
                    except ExpectTimeoutError:
                        self.cluster.close_bash(None)
        else:
            command = f"set -o pipefail && echo -e \"{sql}\" | {client} | {hash_utility}"
            for setting in query_settings:
                setting_name, setting_value = setting
                command += f" --{setting_name} \"{setting_value}\""
            with step("executing command", description=command,
                      format_description=False) if steps else NullStep():
                try:
                    r = self.cluster.bash(self.name)(command=command, *nargs, **kwargs)
                except ExpectTimeoutError:
                    self.cluster.close_bash(self.name)

        with Then(f"exitcode should be 0") if steps else NullStep():
            assert r.exitcode == 0, error(r.output)

        return r.output

    def diff_query(self, sql, expected_output, steps=True, step=By,
                   query_settings=None, secure=False, *nargs, **kwargs):
        """Execute inside the container but from the host and compare its output
        to file that is located on the host.

        For example:
            diff <(echo "SELECT * FROM my_ints FORMAT CSVWithNames" | clickhouse-client -mn) select.out

        :param step:
        :param steps:
        :param secure:
        :param query_settings:
        :param sql: sql query
        :param expected_output: path to the expected output
        """
        query_settings = list(query_settings or [])
        query_settings = list(query_settings)

        if hasattr(current().context, "default_query_settings"):
            query_settings += current().context.default_query_settings

        client = "clickhouse client -n"
        if secure:
            client += " -s"

        if len(sql) > 1024:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8") as query:
                query.write(sql)
                query.flush()
                command = f"diff <(cat \"{query.name}\" | {self.cluster.docker_exec(self.name)} {client}) {expected_output}"
                for setting in query_settings:
                    setting_name, setting_value = setting
                    command += f" --{setting_name} \"{setting_value}\""
                description = f"""
                    echo -e \"{sql[:100]}...\" > {query.name}
                    {command}
                """
                with step("executing command", description=description, format_description=False) if steps else NullStep():
                    try:
                        r = self.cluster.bash(None)(command, *nargs, **kwargs)
                    except ExpectTimeoutError:
                        self.cluster.close_bash(None)
        else:
            command = f"diff <(echo -e \"{sql}\" | {self.cluster.docker_exec(self.name)} {client}) {expected_output}"
            for setting in query_settings:
                setting_name, setting_value = setting
                command += f" --{setting_name} \"{setting_value}\""
            with step("executing command", description=command,
                      format_description=False) if steps else NullStep():
                try:
                    r = self.cluster.bash(None)(command, *nargs, **kwargs)
                except ExpectTimeoutError:
                    self.cluster.close_bash(None)

        with Then(f"exitcode should be 0") if steps else NullStep():
            assert r.exitcode == 0, error(r.output)

    def query(self, sql, expect_message=None, exitcode=None, steps=True, no_checks=False,
              raise_on_exception=False, step=By, query_settings=None,
              num_retries=5, messages_to_retry=None, retry_delay=5.0, secure=False,
              *nargs, **kwargs):
        """Execute and check query
        :param no_checks:
        :param raise_on_exception:
        :param sql: sql query
        :param expect_message: expected message that should be in the output, default: None
        :param exitcode: expected exitcode, default: None
        :param steps: wrap query execution in a step, default: True
        :param step: wrapping step class, default: By
        :param query_settings: list of settings to be used for the query in the form [(name, value),...], default: None
        :param num_retries: number of retries, default: 5
        :param messages_to_retry: list of messages in the query output for
               which retry should be triggered, default: MESSAGES_TO_RETRY
        :param retry_delay: number of seconds to sleep before retry, default: 5
        :param secure: use secure connection, default: False
        """
        num_retries = max(0, int(num_retries))
        retry_delay = max(0.0, float(retry_delay))
        query_settings = list(query_settings or [])
        query_settings = list(query_settings)

        if messages_to_retry is None:
            messages_to_retry = MESSAGES_TO_RETRY

        if hasattr(current().context, "default_query_settings"):
            query_settings += current().context.default_query_settings

        client = "clickhouse client -n"
        if secure:
            client += " -s"

        if len(sql) > 1024:
            with tempfile.NamedTemporaryFile("w", encoding="utf-8") as query:
                query.write(sql)
                query.flush()
                command = f"cat \"{query.name}\" | {self.cluster.docker_exec(self.name)} {client}"
                for setting in query_settings:
                    setting_name, setting_value = setting
                    command += f" --{setting_name} \"{setting_value}\""
                description = f"""
                    echo -e \"{sql[:100]}...\" > {query.name}
                    {command}
                """
                with step("executing command", description=description, format_description=False) if steps else NullStep():
                    try:
                        r = self.cluster.bash(None)(command, *nargs, **kwargs)
                    except ExpectTimeoutError:
                        self.cluster.close_bash(None)
        else:
            command = f"{client} -q \"{sql}\""
            for setting in query_settings:
                setting_name, setting_value = setting
                command += f" --{setting_name} \"{setting_value}\""
            with step("executing command", description=command, format_description=False) if steps else NullStep():
                try:
                    r = self.cluster.bash(self.name)(command, *nargs, **kwargs)
                except ExpectTimeoutError:
                    self.cluster.close_bash(self.name)
                    raise

        if num_retries and num_retries > 0:
            if any(msg in r.output for msg in messages_to_retry):
                time.sleep(retry_delay)
                return self.query(sql=sql, expect_message=expect_message, exitcode=exitcode,
                                  steps=steps, no_checks=no_checks,
                                  raise_on_exception=raise_on_exception, step=step, query_settings=query_settings,
                                  num_retries=num_retries - 1, messages_to_retry=messages_to_retry,
                                  *nargs, **kwargs)

        if no_checks:
            return r

        if exitcode is not None:
            with Then(f"exitcode should be {exitcode}") if steps else NullStep():
                assert r.exitcode == exitcode, error(r.output)

        if expect_message is not None:
            with Then(f"output should contain message", description=expect_message) if steps else NullStep():
                assert expect_message in r.output, error(r.output)

        if expect_message is None or "Exception:" not in expect_message:
            with Then("check if output has exception") if steps else NullStep():
                if "Exception:" in r.output:
                    if raise_on_exception:
                        raise QueryRuntimeException(r.output)
                    assert False, error(r.output)

        return r


def _create_container(image, hostname, network, env=None, volumes=None, ports=None,
                      entrypoint=None, command=None, cap_add=None, healthcheck=None):
    """Helper to create a DockerContainer with common settings."""
    container = DockerContainer(image)
    container.with_network(network)
    container.with_kwargs(
        hostname=hostname,
        network_aliases={network.name: [hostname]} if hasattr(network, 'name') else {},
    )
    if env:
        for k, v in env.items():
            container.with_env(k, v)
    if volumes:
        for host_path, container_path in volumes:
            container.with_volume_mapping(host_path, container_path)
    if ports:
        for port in ports:
            container.with_exposed_ports(port)
    if entrypoint is not None:
        container.with_kwargs(entrypoint=entrypoint)
    if command is not None:
        container.with_command(command)
    if cap_add:
        container.with_kwargs(cap_add=cap_add)
    if healthcheck:
        container.with_kwargs(healthcheck=healthcheck)
    return container


def _wait_for_container_healthy(docker_client, container_id, timeout=120, poll_interval=3):
    """Wait until a container's health check reports 'healthy'."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            info = docker_client.api.inspect_container(container_id)
            health = info.get("State", {}).get("Health", {})
            status = health.get("Status", "none")
            if status == "healthy":
                return True
        except Exception:
            pass
        time.sleep(poll_interval)
    raise RuntimeError(f"Container {container_id} did not become healthy within {timeout}s")


class Cluster(object):
    """Cluster managed by testcontainers-python."""

    def __init__(self, local=False,
                 configs_dir=None,
                 nodes=None,
                 docker_dir=None,
                 environ=None):

        self.shells = {}
        self._control_shell = None
        self.environ = {} if (environ is None) else environ
        self.configs_dir = configs_dir
        self.local = local
        self.nodes = nodes or {}
        self._containers = {}      # name -> started DockerContainer
        self._container_ids = {}   # name -> container_id string
        self._network = None
        self._network_name = None
        self._docker_client = None
        self._shared_volumes = []  # named volume names for cleanup

        frame = inspect.currentframe().f_back
        caller_dir = os.path.dirname(os.path.abspath(frame.f_globals["__file__"]))

        # auto set configs directory
        if self.configs_dir is None:
            caller_configs_dir = caller_dir
            if os.path.exists(caller_configs_dir):
                self.configs_dir = caller_configs_dir

        if not os.path.exists(self.configs_dir):
            raise TypeError(f"configs directory '{self.configs_dir}' does not exist")

        # docker dir contains scripts like custom_entrypoint.sh
        if docker_dir is None:
            caller_docker_dir = os.path.join(caller_dir, "docker")
            if os.path.exists(caller_docker_dir):
                docker_dir = caller_docker_dir
        self._docker_dir = docker_dir

        self.lock = threading.Lock()

    def docker_exec(self, node_name):
        """Return a 'docker exec <container_id>' prefix string for piping commands."""
        container_id = self.get_container_id(node_name)
        return f"docker exec -i {container_id}"

    def get_container_id(self, node_name):
        """Get Docker container ID for a node."""
        cid = self._container_ids.get(node_name)
        if cid:
            return cid
        raise RuntimeError(f"No container found for node '{node_name}'")

    def get_mapped_port(self, node_name, container_port):
        """Get the host port mapped to a container port."""
        cid = self.get_container_id(node_name)
        info = self._docker_client.api.inspect_container(cid)
        port_key = f"{container_port}/tcp"
        bindings = info.get("NetworkSettings", {}).get("Ports", {}).get(port_key)
        if bindings and len(bindings) > 0:
            return int(bindings[0]["HostPort"])
        raise RuntimeError(f"No host port mapped for {node_name}:{container_port}")

    @property
    def control_shell(self, timeout=300):
        """Must be called with self.lock.acquired.
        """
        if self._control_shell is not None:
            return self._control_shell

        time_start = time.time()
        while True:
            shell = Shell()
            shell.timeout = 30
            try:
                shell("echo 1")
                break
            except IOError:
                raise
            except Exception:
                shell.__exit__(None, None, None)
                if time.time() - time_start > timeout:
                    raise RuntimeError(f"failed to open control shell")
        self._control_shell = shell
        return self._control_shell

    def close_control_shell(self):
        """Must be called with self.lock.acquired.
        """
        if self._control_shell is None:
            return
        shell = self._control_shell
        self._control_shell = None
        shell.__exit__(None, None, None)

    def node_container_id(self, node, timeout=300):
        """Get container ID for a node. Uses cached ID from testcontainers."""
        cid = self._container_ids.get(node)
        if cid:
            return cid
        # Fallback: try docker inspect
        time_start = time.time()
        while True:
            try:
                c = self.control_shell(f"docker inspect --format '{{{{.Id}}}}' {node}", timeout=timeout)
                container_id = c.output.strip()
                if c.exitcode == 0 and len(container_id) > 1:
                    return container_id
            except IOError:
                raise
            except ExpectTimeoutError:
                self.close_control_shell()
                timeout = timeout - (time.time() - time_start)
                if timeout <= 0:
                    raise RuntimeError(f"failed to get docker container id for the {node} service")

    def node_wait_healthy(self, node, timeout=300):
        """Wait for a container to become healthy."""
        container_id = self._container_ids.get(node)
        if container_id and self._docker_client:
            _wait_for_container_healthy(self._docker_client, container_id, timeout=timeout)
            return
        # Fallback via shell
        time_start = time.time()
        while True:
            try:
                c = self.control_shell(f"docker inspect --format '{{{{.State.Health.Status}}}}' {self.get_container_id(node)}", timeout=timeout)
                if c.exitcode == 0 and 'healthy' in c.output:
                    return
            except IOError:
                raise
            except ExpectTimeoutError:
                self.close_control_shell()
                timeout = timeout - (time.time() - time_start)
                if timeout <= 0:
                    raise RuntimeError(f"failed to get docker container healthy status for the {node} service")

    def shell(self, node, timeout=300):
        """Returns unique shell terminal to be used.
        """
        container_id = None

        if node is not None:
            with self.lock:
                container_id = self.node_container_id(node=node, timeout=timeout)

        time_start = time.time()
        while True:
            if node is None:
                shell = Shell()
            else:
                shell = Shell(command=[
                    "/bin/bash", "--noediting", "-c", f"docker exec -it {container_id} bash --noediting"
                ], name=node)
            shell.timeout = 30
            try:
                shell("echo 1")
                break
            except IOError:
                raise
            except Exception:
                shell.__exit__(None, None, None)
                if time.time() - time_start > timeout:
                    raise RuntimeError(f"failed to open bash to node {node}")

        shell.timeout = timeout
        return shell

    def bash(self, node, timeout=600, command="bash --noediting"):
        """Returns thread-local bash terminal
        to a specific node.
        :param command:
        :param timeout:
        :param node: name of the service
        """

        current_thread = threading.current_thread()
        shell_id = f"{current_thread.name}-{node}"

        with self.lock:
            if self.shells.get(shell_id) is None:
                if node is not None:
                    container_id = self.node_container_id(node=node, timeout=timeout)

                time_start = time.time()
                while True:
                    try:
                        if node is None:
                            self.shells[shell_id] = Shell()
                        else:
                            self.shells[shell_id] = Shell(command=[
                                "/bin/bash", "--noediting", "-c", f"docker exec -it {container_id} {command}"
                            ], name=node).__enter__()
                        self.shells[shell_id].timeout = 30
                        self.shells[shell_id]("echo 1")
                        break
                    except IOError:
                        raise
                    except Exception:
                        self.shells[shell_id].__exit__(None, None, None)
                        if time.time() - time_start > timeout:
                            raise RuntimeError(f"failed to open bash to node {node}")

                if node is None:
                    for env_name, env_value in self.environ.items():
                        self.shells[shell_id](f"export {env_name}=\"{env_value}\"")

                self.shells[shell_id].timeout = timeout

                # clean up any stale open shells for threads that have exited
                active_thread_names = {thread.name for thread in threading.enumerate()}

                for bash_id in list(self.shells.keys()):
                    thread_name, node_name = bash_id.rsplit("-", 1)
                    if thread_name not in active_thread_names:
                        self.shells[bash_id].__exit__(None, None, None)
                        del self.shells[bash_id]

            return self.shells[shell_id]

    def close_bash(self, node):
        current_thread = threading.current_thread()
        shell_id = f"{current_thread.name}-{node}"

        with self.lock:
            if self.shells.get(shell_id) is None:
                return
            self.shells[shell_id].__exit__(None, None, None)
            del self.shells[shell_id]

    def __enter__(self):
        with Given("testcontainers cluster"):
            self.up()
        return self

    def __exit__(self, type, value, traceback):
        try:
            with Finally("I clean up"):
                self.down()
        finally:
            with self.lock:
                for shell in self.shells.values():
                    shell.__exit__(type, value, traceback)

    def node(self, node_name):
        """Get object with node bound methods
        :param node_name: name of service name
        """
        if node_name.startswith("clickhouse"):
            return ClickHouseNode(self, node_name)
        return Node(self, node_name)

    def _create_network(self):
        """Create a Docker network for inter-container communication."""
        self._docker_client = docker.from_env()
        self._network_name = f"testflows_{os.getpid()}"
        self._docker_client.networks.create(self._network_name, driver="bridge")

    def _remove_network(self):
        """Remove the Docker network."""
        if self._docker_client and self._network_name:
            try:
                net = self._docker_client.networks.get(self._network_name)
                net.remove()
            except Exception:
                pass

    def _start_container(self, name, image, hostname=None, env=None, volumes=None,
                         ports=None, entrypoint=None, command=None, cap_add=None,
                         healthcheck=None, volumes_from_name=None):
        """Start a single container via Docker SDK and connect to network."""
        hostname = hostname or name
        docker_volumes = {}
        binds = []

        if volumes:
            for host_path, container_path in volumes:
                binds.append(f"{host_path}:{container_path}")

        # Handle volumes_from: replicate all mounts (volumes and bind mounts) from source container
        if volumes_from_name and volumes_from_name in self._containers:
            source_info = self._docker_client.api.inspect_container(
                self._container_ids[volumes_from_name]
            )
            for m in source_info.get("Mounts", []):
                if m["Type"] == "volume":
                    binds.append(f"{m['Name']}:{m['Destination']}")
                elif m["Type"] == "bind":
                    binds.append(f"{m['Source']}:{m['Destination']}")

        host_config_kwargs = {
            "binds": binds,
            "cap_add": cap_add or [],
            "security_opt": ["label:disable"],
        }
        if ports:
            port_bindings = {}
            exposed_ports = {}
            for p in ports:
                if isinstance(p, str) and ":" in p:
                    host_port, container_port = p.split(":")
                    port_bindings[f"{container_port}/tcp"] = int(host_port)
                    exposed_ports[f"{container_port}/tcp"] = {}
                else:
                    port_str = str(p)
                    port_bindings[f"{port_str}/tcp"] = None
                    exposed_ports[f"{port_str}/tcp"] = {}
            host_config_kwargs["port_bindings"] = port_bindings
        else:
            exposed_ports = {}

        host_config = self._docker_client.api.create_host_config(**host_config_kwargs)

        container_config = {
            "image": image,
            "hostname": hostname,
            "environment": env or {},
            "host_config": host_config,
            "detach": True,
            "ports": exposed_ports if exposed_ports else None,
        }
        if entrypoint is not None:
            container_config["entrypoint"] = entrypoint
        if command is not None:
            container_config["command"] = command
        if healthcheck is not None:
            container_config["healthcheck"] = healthcheck

        container = self._docker_client.api.create_container(**container_config)
        container_id = container["Id"]

        # Connect to network before starting
        self._docker_client.api.connect_container_to_network(
            container_id, self._network_name,
            aliases=[hostname]
        )

        self._docker_client.api.start(container_id)

        self._containers[name] = container
        self._container_ids[name] = container_id
        return container_id

    def _stop_container(self, name):
        """Stop and remove a container."""
        container_id = self._container_ids.get(name)
        if container_id:
            try:
                self._docker_client.api.stop(container_id, timeout=5)
            except Exception:
                pass
            try:
                self._docker_client.api.remove_container(container_id, force=True, v=True)
            except Exception:
                pass
            self._container_ids.pop(name, None)
            self._containers.pop(name, None)

    def up(self, timeout=120):
        if self.local:
            with Given("I am running in local mode"):
                with Then("clean local clickhouse instances logs"):
                    for node_type in self.nodes:
                        if node_type == "clickhouse":
                            for node in self.nodes[node_type]:
                                logs_path = f"{self.configs_dir}/_instances/{node}/logs/*.log"
                                if glob.glob(logs_path):
                                    self.command(None, f"truncate -s 0 {logs_path}")

            with And("I set all the necessary environment variables"):
                self.environ["COMPOSE_HTTP_TIMEOUT"] = "300"
                self.environ["CLICKHOUSE_TESTS_DIR"] = self.configs_dir

            with And("I list environment variables to show their values"):
                self.command(None, "env | grep CLICKHOUSE")

        clickhouse_version = os.environ.get("CLICKHOUSE_VERSION", "23.3")
        clickhouse_image = os.environ.get("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server")
        zookeeper_version = os.environ.get("ZOOKEEPER_VERSION", "3.8.4")
        zookeeper_image = os.environ.get("ZOOKEEPER_IMAGE", "docker.io/zookeeper")
        mysql_version = os.environ.get("MYSQL_VERSION", "8.0")
        pgsql_version = os.environ.get("PGSQL_VERSION", "latest")
        minio_version = os.environ.get("MINIO_VERSION", "latest")
        tests_dir = os.environ.get("CLICKHOUSE_TESTS_DIR", self.configs_dir)
        log_level = os.environ.get("LOG_LEVEL", "info")
        gcs_cred_json = os.environ.get("QA_GCS_CRED_JSON", "")
        gcs_cred_json_encoded = os.environ.get("QA_GCS_CRED_JSON_ENCODED", "")

        with Given("testcontainers cluster"):
            max_attempts = 5
            for attempt in range(max_attempts):
                with When(f"attempt {attempt}/{max_attempts}"):
                    try:
                        self._do_up(
                            clickhouse_image=clickhouse_image,
                            clickhouse_version=clickhouse_version,
                            zookeeper_image=zookeeper_image,
                            zookeeper_version=zookeeper_version,
                            mysql_version=mysql_version,
                            pgsql_version=pgsql_version,
                            minio_version=minio_version,
                            tests_dir=tests_dir,
                            log_level=log_level,
                            gcs_cred_json=gcs_cred_json,
                            gcs_cred_json_encoded=gcs_cred_json_encoded,
                            timeout=timeout,
                        )
                        break
                    except Exception as e:
                        note(f"Attempt {attempt} failed: {e}")
                        self._do_down()
                        if attempt >= max_attempts - 1:
                            fail(f"could not bring up testcontainers cluster after {max_attempts} attempts")

        with Then("wait all nodes report healthy"):
            for node_name in self.nodes["clickhouse"]:
                self.node(node_name).wait_healthy()

    def _do_up(self, clickhouse_image, clickhouse_version, zookeeper_image, zookeeper_version,
               mysql_version, pgsql_version, minio_version, tests_dir, log_level,
               gcs_cred_json, gcs_cred_json_encoded, timeout):
        """Internal: create network and start all containers in dependency order."""
        # Clean up any leftover containers from previous runs
        self._do_down()
        self._create_network()

        docker_dir = self._docker_dir

        # Ensure log directories exist
        for node in self.nodes.get("clickhouse", ()):
            logs_dir = os.path.join(tests_dir, f"_instances/{node}/logs")
            os.makedirs(logs_dir, exist_ok=True)

        # 1. ZooKeeper
        with By("starting zookeeper"):
            self._start_container(
                name="zookeeper",
                image=f"{zookeeper_image}:{zookeeper_version}",
                hostname="zookeeper",
                env={
                    "ZOO_TICK_TIME": "500",
                    "ZOO_MY_ID": "1",
                    "ZOO_4LW_COMMANDS_WHITELIST": "*",
                },
                healthcheck={
                    "Test": ["CMD-SHELL", "echo ruok | nc 127.0.0.1 2181 | grep imok"],
                    "Interval": 3 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 5,
                    "StartPeriod": 2 * 1_000_000_000,
                },
            )
            _wait_for_container_healthy(self._docker_client, self._container_ids["zookeeper"], timeout=60)

        # 2. MySQL, PostgreSQL, RabbitMQ, FTP, SFTP, MinIO (parallel-ish, sequential for simplicity)
        with And("starting mysql"):
            self._start_container(
                name="mysql",
                image=f"mysql:{mysql_version}",
                hostname="mysql",
                env={
                    "MYSQL_ALLOW_EMPTY_PASSWORD": "True",
                    "MYSQL_ROOT_PASSWORD": "qwerty",
                },
                command=["--gtid_mode=on", "--enforce_gtid_consistency=ON"],
                healthcheck={
                    "Test": ["CMD-SHELL", "mysqladmin -p=qwerty ping -h localhost"],
                    "Timeout": 20 * 1_000_000_000,
                    "Retries": 10,
                },
            )

        with And("starting postgres"):
            self._start_container(
                name="postgres",
                image=f"postgres:{pgsql_version}",
                hostname="postgres",
                env={
                    "POSTGRES_PASSWORD": "qwerty",
                    "POSTGRES_USER": "test",
                },
                command=["postgres", "-c", "wal_level=logical"],
                healthcheck={
                    "Test": ["CMD-SHELL", "pg_isready -U test"],
                    "Timeout": 20 * 1_000_000_000,
                    "Retries": 10,
                },
            )

        with And("starting rabbitmq"):
            self._start_container(
                name="rabbitmq",
                image="docker.io/rabbitmq:alpine",
                hostname="rabbitmq",
                env={
                    "RABBITMQ_DEFAULT_USER": "test",
                    "RABBITMQ_DEFAULT_PASS": "qwerty",
                },
                healthcheck={
                    "Test": ["CMD-SHELL", "rabbitmq-diagnostics -q ping"],
                    "Interval": 10 * 1_000_000_000,
                    "Timeout": 15 * 1_000_000_000,
                    "Retries": 20,
                    "StartPeriod": 15 * 1_000_000_000,
                },
            )

        with And("starting ftp_server"):
            self._start_container(
                name="ftp_server",
                image="gists/pure-ftpd:latest",
                hostname="ftp_server",
                env={
                    "FTP_USER_NAME": "test",
                    "FTP_USER_PASS": "test",
                    "FTP_USER_HOME": "/home/ftpuser/test",
                    "PUBLICHOST": "ftp_server",
                    "MIN_PASV_PORT": "30000",
                    "MAX_PASV_PORT": "31000",
                },
                entrypoint=["/bin/sh", "-c"],
                command=[
                    'mkdir -p /etc/pureftpd && '
                    'mkdir -p "$FTP_USER_HOME" && '
                    'chown -R 1000:1000 /home/ftpuser && '
                    'touch /etc/pureftpd/pureftpd.passwd && '
                    'printf \'%s\\n%s\\n\' "$FTP_USER_PASS" "$FTP_USER_PASS" | '
                    'pure-pw useradd "$FTP_USER_NAME" -u 1000 -g 1000 -d "$FTP_USER_HOME" -f /etc/pureftpd/pureftpd.passwd && '
                    'pure-pw mkdb /etc/pureftpd/pureftpd.pdb -f /etc/pureftpd/pureftpd.passwd && '
                    'exec /usr/sbin/pure-ftpd '
                    '-l puredb:/etc/pureftpd/pureftpd.pdb '
                    '-E -j -R '
                    '-P "$PUBLICHOST" '
                    '-p "$MIN_PASV_PORT:$MAX_PASV_PORT"'
                ],
                healthcheck={
                    "Test": ["CMD-SHELL", "nc -z localhost 21"],
                    "Interval": 10 * 1_000_000_000,
                    "Timeout": 3 * 1_000_000_000,
                    "Retries": 10,
                    "StartPeriod": 5 * 1_000_000_000,
                },
            )

        with And("starting sftp_server"):
            self._start_container(
                name="sftp_server",
                image="panubo/sshd:latest",
                hostname="sftp_server",
                env={
                    "SSH_ENABLE_ROOT": "true",
                    "SSH_ENABLE_PASSWORD_AUTH": "true",
                },
                command=[
                    "sh", "-c",
                    'echo "PermitRootLogin yes" >> /etc/ssh/sshd_config && '
                    'echo "root:JFzMHfVpvTgEd74XXPq6wARA2Qg3AutJ" | chpasswd && '
                    '/usr/sbin/sshd -D -e -f /etc/ssh/sshd_config'
                ],
                healthcheck={
                    "Test": ["CMD-SHELL", "echo 1"],
                    "Interval": 3 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 20,
                    "StartPeriod": 10 * 1_000_000_000,
                },
            )

        with And("starting minio"):
            self._start_container(
                name="minio",
                image=f"minio/minio:{minio_version}",
                hostname="minio",
                env={
                    "MINIO_ACCESS_KEY": "access_key",
                    "MINIO_SECRET_KEY": "it_is_my_super_secret_key",
                },
                entrypoint=["sh"],
                command=["-c", "mkdir -p doc_gen_minio/export/clickhouse && minio server doc_gen_minio/export"],
                healthcheck={
                    "Test": ["CMD-SHELL", "echo 1"],
                    "Interval": 3 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 20,
                    "StartPeriod": 10 * 1_000_000_000,
                },
            )

        # Wait for tier-2 services
        for svc in ["mysql", "postgres", "rabbitmq", "ftp_server", "sftp_server", "minio"]:
            if svc in self._container_ids:
                _wait_for_container_healthy(self._docker_client, self._container_ids[svc], timeout=120)

        # 3. Kafka (depends on zookeeper)
        with And("starting kafka"):
            self._start_container(
                name="kafka",
                image="confluentinc/cp-kafka:7.7.7",
                hostname="kafka",
                env={
                    "KAFKA_LISTENERS": "PLAINTEXT://0.0.0.0:9092",
                    "KAFKA_ZOOKEEPER_CONNECT": "zookeeper:2181",
                    "ZOOKEEPER": "zookeeper:2181",
                    "KAFKA_ADVERTISED_LISTENERS": "PLAINTEXT://kafka:9092",
                    "KAFKA_BROKER_ID": "1",
                    "BOOTSTRAP_SERVERS": "kafka:9092",
                    "KAFKA_LOG4J_LOGGERS": "kafka.controller=INFO,kafka.producer.async.DefaultEventHandler=INFO,state.change.logger=INFO",
                },
                healthcheck={
                    "Test": ["CMD-SHELL", "echo dump | nc zookeeper 2181 | grep '/brokers/ids/1'"],
                    "Interval": 10 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 40,
                    "StartPeriod": 10 * 1_000_000_000,
                },
            )
            _wait_for_container_healthy(self._docker_client, self._container_ids["kafka"], timeout=120)

        # 4. ClickHouse nodes
        ch_base_volumes = [
            (os.path.join(docker_dir, "custom_entrypoint.sh"), "/custom_entrypoint.sh"),
            (os.path.join(docker_dir, "dynamic_settings.sh"), "/docker-entrypoint-initdb.d/dynamic_settings.sh"),
            (os.path.join(tests_dir, "configs/clickhouse/ssl"), "/etc/clickhouse-server/ssl"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/common.xml"), "/etc/clickhouse-server/config.d/common.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/graphite_rollup.xml"), "/etc/clickhouse-server/config.d/graphite_rollup.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/logs.xml"), "/etc/clickhouse-server/config.d/logs.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/postgres.xml"), "/etc/clickhouse-server/config.d/postgres.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/remote.xml"), "/etc/clickhouse-server/config.d/remote.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/replication.xml"), "/etc/clickhouse-server/config.d/replication.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/config.d/zookeeper.xml"), "/etc/clickhouse-server/config.d/zookeeper.xml"),
            (os.path.join(tests_dir, "configs/clickhouse/users.d/default.xml"), "/etc/clickhouse-server/users.d/default.xml"),
        ]

        ch_image = f"{clickhouse_image}:{clickhouse_version}"

        # Find the install_delve.sh path
        install_delve_path = os.path.normpath(os.path.join(tests_dir, "../../../test/integration/install_delve.sh"))

        with And("starting clickhouse2"):
            ch2_volumes = ch_base_volumes + [
                (os.path.join(tests_dir, "_instances/clickhouse2/logs"), "/var/log/clickhouse-server"),
                (os.path.join(tests_dir, "configs/clickhouse2/config.d/macros.xml"), "/etc/clickhouse-server/config.d/macros.xml"),
            ]
            self._start_container(
                name="clickhouse2",
                image=ch_image,
                hostname="clickhouse2",
                env={
                    "CLICKHOUSE_VERSION": clickhouse_version,
                    "CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS": "true",
                    "CLICKHOUSE_SKIP_USER_SETUP": "1",
                },
                volumes=ch2_volumes,
                entrypoint=["/custom_entrypoint.sh"],
                cap_add=["SYS_PTRACE", "SYS_NICE"],
                healthcheck={
                    "Test": ["CMD-SHELL", "wget http://localhost:8123/ping"],
                    "Interval": 3 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 30,
                    "StartPeriod": 5 * 1_000_000_000,
                },
            )

        with And("starting clickhouse1"):
            ch1_volumes = ch_base_volumes + [
                (os.path.join(tests_dir, "configs/clickhouse1/storage_configuration.sh"), "/docker-entrypoint-initdb.d/storage_configuration.sh"),
                (os.path.join(tests_dir, "files"), "/var/lib/clickhouse/user_files"),
                (os.path.join(tests_dir, "_instances/clickhouse1/logs"), "/var/log/clickhouse-server"),
                (os.path.join(tests_dir, "configs/clickhouse1/config.d/macros.xml"), "/etc/clickhouse-server/config.d/macros.xml"),
                (os.path.join(tests_dir, "configs/clickhouse1/config.d/rabbitmq.xml"), "/etc/clickhouse-server/config.d/rabbitmq.xml"),
            ]
            if os.path.exists(install_delve_path):
                ch1_volumes.append((install_delve_path, "/tmp/install_delve.sh"))

            self._start_container(
                name="clickhouse1",
                image=ch_image,
                hostname="clickhouse1",
                env={
                    "CLICKHOUSE_VERSION": clickhouse_version,
                    "CLICKHOUSE_ALWAYS_RUN_INITDB_SCRIPTS": "true",
                    "CLICKHOUSE_SKIP_USER_SETUP": "1",
                },
                volumes=ch1_volumes,
                entrypoint=["/custom_entrypoint.sh"],
                cap_add=["SYS_PTRACE", "SYS_NICE"],
                healthcheck={
                    "Test": ["CMD-SHELL", "wget http://localhost:8123/ping"],
                    "Interval": 3 * 1_000_000_000,
                    "Timeout": 2 * 1_000_000_000,
                    "Retries": 30,
                    "StartPeriod": 5 * 1_000_000_000,
                },
            )

        # Wait for both ClickHouse nodes
        _wait_for_container_healthy(self._docker_client, self._container_ids["clickhouse2"], timeout=120)
        _wait_for_container_healthy(self._docker_client, self._container_ids["clickhouse1"], timeout=120)

        # 5. clickhouse_backup (shares volumes from clickhouse1)
        with And("starting clickhouse_backup"):
            # Find the backup binary
            backup_binary = os.path.normpath(os.path.join(tests_dir, "../../../clickhouse-backup/clickhouse-backup-race"))
            coverage_dir = os.path.normpath(os.path.join(tests_dir, "../_coverage_"))
            os.makedirs(coverage_dir, exist_ok=True)

            backup_volumes = [
                (backup_binary, "/bin/clickhouse-backup"),
                (os.path.join(tests_dir, "configs/backup"), "/etc/clickhouse-backup"),
                (coverage_dir, "/tmp/_coverage_"),
            ]

            self._start_container(
                name="clickhouse_backup",
                image="ubuntu:latest",
                hostname="backup",
                env={
                    "DEBIAN_FRONTEND": "noninteractive",
                    "LOG_LEVEL": log_level,
                    "GCS_CREDENTIALS_JSON": gcs_cred_json,
                    "GCS_CREDENTIALS_JSON_ENCODED": gcs_cred_json_encoded,
                    "CLICKHOUSE_HOST": "clickhouse1",
                    "CLICKHOUSE_BACKUP_CONFIG": "/etc/clickhouse-backup/config.yml",
                    "TZ": "Europe/Moscow",
                    "GOCOVERDIR": "/tmp/_coverage_/",
                },
                volumes=backup_volumes,
                volumes_from_name="clickhouse1",
                ports=["7171", "40002"],
                entrypoint=["/bin/bash"],
                command=[
                    "-c",
                    "set -x && "
                    "apt-get update && "
                    "apt-get install -y ca-certificates tzdata bash curl && "
                    "update-ca-certificates && "
                    "clickhouse-backup server"
                ],
                cap_add=["SYS_NICE"],
                healthcheck={
                    "Test": ["CMD-SHELL", "curl http://backup:7171/backup/status"],
                    "Interval": 5 * 1_000_000_000,
                    "Timeout": 5 * 1_000_000_000,
                    "Retries": 40,
                    "StartPeriod": 10 * 1_000_000_000,
                },
            )
            _wait_for_container_healthy(self._docker_client, self._container_ids["clickhouse_backup"], timeout=300)

    def _do_down(self):
        """Internal: stop all containers and remove network."""
        for name in list(self._containers.keys()):
            self._stop_container(name)
        # Remove shared volumes
        if self._docker_client:
            for vol_name in self._shared_volumes:
                try:
                    vol = self._docker_client.volumes.get(vol_name)
                    vol.remove(force=True)
                except Exception:
                    pass
        self._shared_volumes = []
        self._remove_network()

    def down(self, timeout=300):
        """Bring cluster down."""

        # add message to each clickhouse-server.log
        if settings.debug:
            for node in self.nodes.get("clickhouse", ()):
                if node in self._container_ids:
                    self.command(node=node, command=f"echo -e \"\n-- sending stop to: {node} --\n\" >> /var/log/clickhouse-server/clickhouse-server.log")
        bash = self.bash(None)
        try:
            with self.lock:
                # remove and close all not None node terminals
                for shell_id in list(self.shells.keys()):
                    shell = self.shells.pop(shell_id)
                    if shell is not bash:
                        shell.__exit__(None, None, None)
                    else:
                        self.shells[shell_id] = shell
        finally:
            self._do_down()
            with self.lock:
                if self._control_shell:
                    self._control_shell.__exit__(None, None, None)
                    self._control_shell = None

    def temp_path(self):
        """Return temporary folder path.
        """
        p = f"{self.environ['CLICKHOUSE_TESTS_DIR']}/_temp"
        if not os.path.exists(p):
            os.mkdir(p)
        return p

    def temp_file(self, file_name):
        """Return absolute temporary file path.
        """
        return f"{os.path.join(self.temp_path(), file_name)}"

    def command(self, node, command, expect_message=None, exitcode=None, steps=True, bash=None, *nargs, **kwargs):
        """Execute and check command.
        :param bash:
        :param node: name of the service
        :param command: command
        :param expect_message: expected message that should be in the output, default: None
        :param exitcode: expected exitcode, default: None
        :param steps: don't break command into steps, default: True
        """
        with By("executing command", description=command, format_description=False) if steps else NullStep():
            if bash is None:
                bash = self.bash(node)
            try:
                r = bash(command, *nargs, **kwargs)
            except ExpectTimeoutError:
                self.close_bash(node)
                raise
        if exitcode is not None:
            with Then(f"exitcode should be {exitcode}", format_name=False) if steps else NullStep():
                assert r.exitcode == exitcode, error(r.output)
        if expect_message is not None:
            with Then(f"output should contain message", description=expect_message, format_description=False) if steps else NullStep():
                assert expect_message in r.output, error(r.output)
        return r
