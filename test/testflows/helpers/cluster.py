import glob
import inspect
import io
import os
import shlex
import shutil
import stat
import tarfile
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


def _materialize_binary_from_docker_image(docker_client, image_tag, path_in_image, host_path):
    """Materialize a single file from a local Docker image to ``host_path``.

    Used so that when an upstream Makefile target has already produced a binary
    *inside* a Docker image (e.g. ``make build-race-fips-docker`` -> image
    ``clickhouse-backup:build-race-fips`` with the FIPS race binary at
    ``/src/clickhouse-backup/clickhouse-backup-race-fips``), but the host-side
    copy has since been removed (``make clean``, a partial rebuild, etc.), the
    test cluster can recover the binary directly from the local Docker
    registry instead of forcing a fresh ``docker buildx build``. No
    ``subprocess``, no ``make`` - this only talks to the Docker daemon via the
    ``docker`` Python SDK that is already used everywhere else in this file.

    Returns True if ``host_path`` exists and is executable on return,
    False otherwise (e.g. the image isn't in the local registry, or the file
    is missing inside it). Failures are intentionally non-fatal: the caller
    is expected to fall back to its existing skip-FIPS-scenarios path.
    """
    if os.path.isfile(host_path):
        return True
    try:
        docker_client.images.get(image_tag)
    except docker.errors.ImageNotFound:
        return False
    except Exception:
        return False

    container = None
    try:
        container = docker_client.containers.create(image_tag)
        bits, _ = container.get_archive(path_in_image)
        buf = io.BytesIO(b"".join(bits))
        with tarfile.open(fileobj=buf, mode="r") as tar:
            member_name = os.path.basename(path_in_image)
            try:
                member = tar.getmember(member_name)
            except KeyError:
                return False
            src = tar.extractfile(member)
            if src is None:
                return False
            os.makedirs(os.path.dirname(host_path) or ".", exist_ok=True)
            tmp_path = host_path + ".tmp"
            with open(tmp_path, "wb") as dst:
                shutil.copyfileobj(src, dst)
            os.chmod(
                tmp_path,
                os.stat(tmp_path).st_mode
                | stat.S_IXUSR | stat.S_IXGRP | stat.S_IXOTH,
            )
            os.replace(tmp_path, host_path)
        return os.path.isfile(host_path)
    except Exception:
        return False
    finally:
        if container is not None:
            try:
                container.remove(force=True)
            except Exception:
                pass

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

    def write_file(self, path, contents, mode=None, steps=False):
        """Write text ``contents`` to ``path`` inside this node's container.

        Uses base64 encoding so the data round-trips through the docker-exec
        bash channel safely (no quote/newline escaping issues). Useful for
        dropping ad-hoc YAML/PEM/XML test fixtures into a container without
        committing them to ``configs/`` or generating them in test code.
        """
        import base64
        data = contents if isinstance(contents, (bytes, bytearray)) else contents.encode("utf-8")
        b64 = base64.b64encode(data).decode("ascii")
        path_q = shlex.quote(path)
        # Use here-string to avoid shell expansion of the encoded payload.
        self.cmd(
            f"bash -lc 'set -e; echo {b64} | base64 -d > {path_q}"
            + (f"; chmod {mode} {path_q}" if mode else "")
            + "'",
            exitcode=0,
            steps=steps,
        )


class BackupNode(Node):
    """Node helpers for clickhouse-backup process control inside the container.

    Used by the dedicated ``clickhouse_backup_fips`` container which is started
    in non-autostart mode (``sleep infinity``) so tests can start, stop or
    kill the ``clickhouse-backup-fips`` process explicitly. Also useful for the
    regular ``clickhouse_backup`` container when a test needs to restart the
    server with different environment variables (for example ``GODEBUG``).

    All process state is kept on disk inside the container (PID + log files)
    so callers can use it across separate ``cmd()`` invocations.
    """

    server_pid_file = "/tmp/clickhouse-backup-server.pid"
    server_log_file = "/tmp/clickhouse-backup-server.log"

    def _format_env(self, extra_env):
        if not extra_env:
            return ""
        return " ".join(f"{k}={shlex.quote(str(v))}" for k, v in extra_env.items()) + " "

    def start_server(self, binary="/bin/clickhouse-backup", config="/etc/clickhouse-backup/config.yml",
                     extra_env=None, listen_port=7171, wait_ready=True, timeout=60):
        """Start ``<binary> server`` in the background inside this container.

        :param binary: absolute path to the binary inside the container (e.g.
            ``/bin/clickhouse-backup`` or ``/bin/clickhouse-backup-fips``).
        :param config: absolute path to the YAML config inside the container.
        :param extra_env: optional dict of env vars (e.g. ``{"GODEBUG": "fips140=only"}``)
            that will be exported in front of the binary invocation.
        :param listen_port: REST API port to wait on (defaults to 7171).
        :param wait_ready: poll the listen port over TCP until it accepts a
            connection or ``timeout`` seconds elapse.
        :param timeout: how long to wait for readiness, in seconds.

        Process lifecycle is tracked exclusively through the PID file written
        below. ``pkill -f <pattern>`` is deliberately not used to clean up
        stragglers here: the wrapper ``bash -lc '…'`` that runs this command
        has the binary invocation and the kill pattern in its own argv, so
        ``pkill -f`` would match the wrapper itself, SIGTERM its own parent,
        and exit 143 ("Terminated") before ever launching the server. Cleanup
        of a prior server (if any) is handled by ``stop_server`` -> PID file.
        """
        env_prefix = self._format_env(extra_env)
        binary_q = shlex.quote(binary)
        config_q = shlex.quote(config)
        log_q = shlex.quote(self.server_log_file)
        pid_q = shlex.quote(self.server_pid_file)
        # ``VAR=value cmd`` is *shell* env-prefix syntax; ``nohup VAR=value cmd``
        # is NOT - nohup would try to ``execve`` "VAR=value" as a program and
        # exit with "No such file or directory". Route the env vars through
        # ``env`` so nohup's argv[1] is the literal ``env`` binary and the
        # KEY=VALUE pairs become arguments to it. ``env`` is omitted entirely
        # when ``extra_env`` is empty so we don't add a no-op fork.
        env_via_env = f"env {env_prefix}" if env_prefix else ""
        # Best-effort PID-file-based cleanup of a previous run, then launch.
        # Split into two `cmd()` calls so the launching bash's argv contains
        # *only* the launch command (no kill pattern that could re-trigger the
        # pkill self-match bug if anyone reintroduces pkill -f here later).
        self.cmd(
            f"bash -lc 'if [ -f {pid_q} ]; then "
            f"kill $(cat {pid_q}) >/dev/null 2>&1 || true; "
            f"rm -f {pid_q}; fi'",
            exitcode=0,
        )
        self.cmd(
            f"bash -lc 'nohup {env_via_env}{binary_q} -c {config_q} server >{log_q} 2>&1 & "
            f"echo $! > {pid_q}'",
            exitcode=0,
        )
        if wait_ready:
            # TCP-only connection check: works whether the listener is plain HTTP or
            # TLS (api.secure: true). Avoids tying the readiness check to an
            # HTTP method that may be wrapped in TLS.
            self.cmd(
                f"bash -lc 'for i in $(seq 1 {int(timeout)}); do "
                f"timeout 1 bash -c \"</dev/tcp/localhost/{int(listen_port)}\" >/dev/null 2>&1 && exit 0; "
                f"sleep 1; done; "
                f"echo \"server did not become ready on :{int(listen_port)} in {int(timeout)}s\"; "
                f"tail -n 50 {log_q} || true; exit 1'",
                exitcode=0,
                timeout=int(timeout) + 30,
            )

    def stop_server(self):
        """Stop the server gracefully via SIGTERM addressed to the PID file.

        ``pkill -f <pattern>`` is intentionally avoided here: the wrapping
        ``bash -lc '…'`` command's own argv contains the kill pattern as a
        literal substring, so ``pkill -f`` would also match (and SIGTERM)
        its own parent bash, surfacing as exit 143 / "Terminated" through
        ``Node.cmd``'s ``exitcode == 0`` assertion. The PID file written by
        ``start_server`` is unambiguous; if it's missing we simply have
        nothing to stop.
        """
        pid_q = shlex.quote(self.server_pid_file)
        self.cmd(
            f"bash -lc 'if [ -f {pid_q} ]; then "
            f"kill $(cat {pid_q}) >/dev/null 2>&1 || true; "
            f"rm -f {pid_q}; fi'",
            exitcode=0,
        )

    def kill_server(self):
        """Kill the server immediately (SIGKILL) via the PID file.

        Same rationale as ``stop_server``: no ``pkill -f`` to avoid the
        self-match SIGTERM that fires when the kill pattern is a substring
        of the wrapping shell's argv (see ``start_server`` docstring).
        """
        pid_q = shlex.quote(self.server_pid_file)
        self.cmd(
            f"bash -lc 'if [ -f {pid_q} ]; then "
            f"kill -9 $(cat {pid_q}) >/dev/null 2>&1 || true; "
            f"rm -f {pid_q}; fi'",
            exitcode=0,
        )

    def is_server_running(self):
        """Return ``True`` if the PID recorded by ``start_server`` is alive.

        Uses ``kill -0 $PID`` (no-op signal) rather than ``pgrep -f <pattern>``
        for the same reason ``stop_server`` skips ``pkill -f``: the wrapping
        bash's argv contains the pattern as a literal, so ``pgrep -f`` would
        self-match its own parent and yield a misleading "running" answer.
        """
        pid_q = shlex.quote(self.server_pid_file)
        r = self.cmd(
            f"bash -lc 'if [ -f {pid_q} ] && kill -0 $(cat {pid_q}) >/dev/null 2>&1; "
            f"then echo running; else echo stopped; fi'",
            no_checks=True,
        )
        return "running" in (r.output or "")


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


def _get_container_diagnostic(docker_client, container_id):
    """Collect diagnostic info for an unhealthy container: health log, state, and tail of container logs."""
    diag_lines = []
    try:
        info = docker_client.api.inspect_container(container_id)
        name = info.get("Name", container_id)
        state = info.get("State", {})
        health = state.get("Health", {})
        status = health.get("Status", "unknown")
        diag_lines.append(f"--- Diagnostic for container {name} ({container_id[:12]}) ---")
        diag_lines.append(f"State: Status={state.get('Status')}, Running={state.get('Running')}, "
                          f"ExitCode={state.get('ExitCode')}, OOMKilled={state.get('OOMKilled')}")
        diag_lines.append(f"Health: Status={status}")
        for entry in health.get("Log", [])[-5:]:
            diag_lines.append(f"  HealthCheck: ExitCode={entry.get('ExitCode')} Output={entry.get('Output', '').strip()}")
    except Exception as e:
        diag_lines.append(f"Failed to inspect container {container_id}: {e}")
    try:
        logs = docker_client.api.logs(container_id, tail=30, timestamps=True).decode("utf-8", errors="replace")
        diag_lines.append(f"Container logs (last 30 lines):\n{logs}")
    except Exception as e:
        diag_lines.append(f"Failed to get container logs: {e}")
    return "\n".join(diag_lines)


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
    diag = _get_container_diagnostic(docker_client, container_id)
    raise RuntimeError(f"Container {container_id} did not become healthy within {timeout}s\n{diag}")


class Cluster(object):
    """Cluster managed by testcontainers-python."""

    def __init__(self, local=False,
                 configs_dir=None,
                 nodes=None,
                 docker_dir=None,
                 backup_config_dir=None,
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
        self._backup_config_dir = backup_config_dir

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
                    self._dump_container_logs_shell(node)
                    raise RuntimeError(f"failed to get docker container healthy status for the {node} service")

    def _dump_container_logs_shell(self, node):
        """Dump container logs and health status via shell when docker client is not available."""
        try:
            cid = self.get_container_id(node)
            note(f"--- Diagnostic for {node} (shell fallback) ---")
            c = self.control_shell(f"docker inspect --format '{{{{.State.Status}}}} Health={{{{.State.Health.Status}}}}' {cid}", timeout=30)
            if c.exitcode == 0:
                note(f"Container state: {c.output.strip()}")
            c = self.control_shell(f"docker logs --tail 30 --timestamps {cid}", timeout=30)
            if c.exitcode == 0:
                note(f"Container logs (last 30 lines):\n{c.output}")
        except Exception as e:
            note(f"Failed to collect diagnostic for {node}: {e}")

    def _dump_all_container_diagnostics(self):
        """Dump diagnostic info for all tracked containers on cluster startup failure."""
        if not self._docker_client:
            note("Docker client not available, skipping container diagnostics")
            return
        note("=== Container diagnostics for all tracked containers ===")
        for name, cid in self._container_ids.items():
            try:
                diag = _get_container_diagnostic(self._docker_client, cid)
                note(diag)
            except Exception as e:
                note(f"Failed to get diagnostic for {name}: {e}")
        note("=== End container diagnostics ===")

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
        if node_name in ("clickhouse_backup", "clickhouse_backup_fips"):
            return BackupNode(self, node_name)
        if node_name.startswith("clickhouse"):
            return ClickHouseNode(self, node_name)
        return Node(self, node_name)

    def has_node(self, node_name):
        """Return True if a container with ``node_name`` is currently tracked."""
        return node_name in self._container_ids

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

        # Pull image if not present locally
        try:
            self._docker_client.api.inspect_image(image)
        except Exception:
            note(f"pulling image {image}")
            self._docker_client.api.pull(image)

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

    def start_auxiliary_container(self, name, image, hostname=None, env=None, volumes=None,
                                  ports=None, entrypoint=None, command=None, cap_add=None,
                                  healthcheck=None, volumes_from_name=None, wait_healthy=False,
                                  timeout=60):
        """Start a sidecar container on the cluster's network on demand.

        Used by tests that need ad-hoc helper containers (for example an
        ``openssl s_server`` cipher-policy probe at ``openssl_server:9443``).
        The container is added to the cluster's bookkeeping so :py:meth:`down`
        also cleans it up if the test forgets to.

        :param name: unique container/service name on this cluster.
        :param wait_healthy: wait for the docker HEALTHCHECK to report healthy
            (only useful when ``healthcheck`` is provided).
        """
        if name in self._container_ids:
            raise RuntimeError(f"auxiliary container '{name}' already exists")
        cid = self._start_container(
            name=name, image=image, hostname=hostname, env=env, volumes=volumes,
            ports=ports, entrypoint=entrypoint, command=command, cap_add=cap_add,
            healthcheck=healthcheck, volumes_from_name=volumes_from_name,
        )
        if wait_healthy and healthcheck is not None:
            _wait_for_container_healthy(self._docker_client, cid, timeout=timeout)
        return cid

    def stop_auxiliary_container(self, name):
        """Stop and remove a previously started auxiliary container."""
        self._stop_container(name)

    def container_logs(self, name, *, tail=None):
        """Return stdout+stderr of the named container as a single string.

        Useful for tests that need to corroborate an in-container event by
        inspecting what the container itself printed - for example, the
        FIPS outbound-TLS scenarios cross-check the client-side handshake
        outcome against the ``openssl s_server`` aux-container's log
        (``Cipher    : <name>`` line, ``no shared cipher`` error, etc.).

        :param name: cluster service name passed to
            :py:meth:`start_auxiliary_container` /
            :py:meth:`start_clickhouse_server_container` /
            :py:meth:`start_openssl_container`.
        :param tail: optional integer; when set, return only the last
            ``tail`` log lines (forwarded to the Docker API). ``None``
            (default) returns the full log.
        """
        cid = self._container_ids.get(name)
        if cid is None:
            raise RuntimeError(
                f"container '{name}' is not registered with this cluster; "
                f"call start_auxiliary_container / start_openssl_container first."
            )
        kwargs = {}
        if tail is not None:
            kwargs["tail"] = int(tail)
        raw = self._docker_client.api.logs(cid, **kwargs)
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return raw

    @property
    def tests_dir(self):
        """Host-side root of the testflows tests for this cluster.

        Resolves the same way as ``_do_up`` does internally: the
        ``CLICKHOUSE_TESTS_DIR`` env var when set, otherwise the directory
        that constructed this :class:`Cluster` (``clickhouse_backup/`` when
        the cluster is brought up by ``regression.py``). Tests use this to
        compute host paths of fixture files (config.d overrides, scripts,
        certificates, ...) without duplicating the env-var fallback.
        """
        return os.environ.get("CLICKHOUSE_TESTS_DIR", self.configs_dir)

    @property
    def ssl_certs_dir(self):
        """Host-side directory containing the cluster's static SSL fixtures.

        Always points at ``configs/clickhouse/ssl/`` of the testflows project,
        which holds ``server.crt``, ``server.key`` and ``dhparam.pem`` and is
        the same source the cluster bind-mounts on every ClickHouse node.
        Tests reuse these files so they never have to generate certificates.
        """
        return os.path.join(self.tests_dir, "configs/clickhouse/ssl")

    def start_clickhouse_server_container(self, name, image_tag, *,
                                          extra_volumes=None, env=None,
                                          ports=None, hostname=None,
                                          timeout=180):
        """Start a dedicated ClickHouse server on the cluster network.

        Used by FIPS connectivity scenarios to bring up a fixed-version
        Altinity ClickHouse image next to the regression's default
        ``clickhouse1`` / ``clickhouse2`` nodes - without altering the
        cluster-wide image / version chosen via ``CLICKHOUSE_VERSION``.

        :param name: cluster-unique container name (also used as docker
            hostname / network alias by default; other cluster nodes can
            reach the server at ``<name>:<port>``).
        :param image_tag: full ``image:tag`` reference, e.g.
            ``altinity/clickhouse-server:25.3.8.30001.altinityfips`` or
            ``altinity/clickhouse-server:25.8.16.10002.altinitystable``.
        :param extra_volumes: optional list of ``(host_path, container_path)``
            bind-mount tuples (FIPS scenario uses this to drop a TLS
            ``config.d`` override and the cluster's static SSL certs in).
        :param env: optional dict of additional env vars; defaults already
            include ``CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT=1`` so the image's
            ``default`` user keeps no-password access from across the docker
            network (matches the rest of the regression).
        :param ports: optional list of container ports to publish on the
            host. Inter-container communication uses the docker network so
            this is normally not needed.
        :param hostname: docker hostname / network alias; defaults to ``name``.
        :param timeout: seconds to wait for the image's HEALTHCHECK to flip
            to ``healthy`` (default 180s, generous for first-time pulls).
        """
        merged_env = {"CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT": "1"}
        if env:
            merged_env.update(env)
        return self.start_auxiliary_container(
            name=name,
            image=image_tag,
            hostname=hostname or name,
            env=merged_env,
            volumes=list(extra_volumes) if extra_volumes else [],
            ports=ports,
            cap_add=["SYS_PTRACE", "SYS_NICE"],
            healthcheck={
                "Test": ["CMD-SHELL", "wget -q -T 2 -O- http://localhost:8123/ping || exit 1"],
                "Interval": 3 * 1_000_000_000,
                "Timeout": 3 * 1_000_000_000,
                "Retries": 30,
                "StartPeriod": 5 * 1_000_000_000,
            },
            wait_healthy=True,
            timeout=timeout,
        )

    def start_openssl_container(self, name, role, listen=9443, cert_path=None, key_path=None,
                                dhparam_path=None,
                                cipher=None, ciphersuites=None, tls_version="-tls1_2",
                                extra_args=""):
        """Convenience helper to start an ``openssl s_server`` container.

        :param name: cluster-unique service name (also used as hostname / network alias).
        :param role: ``"s_server"`` (only role implemented for now). ``"s_client"`` probes
            should be issued via ``backup_fips.cmd("openssl s_client ...")`` since
            the FIPS backup container already has ``openssl`` installed.
        :param listen: port to listen on inside the container.
        :param cert_path: absolute host path of the server certificate to bind-mount at
            ``/certs/server.crt``. Reuse the static cluster certs in
            ``configs/clickhouse/ssl/server.crt`` to avoid generating new ones.
        :param key_path: absolute host path of the server key.
        :param dhparam_path: optional host path of a DH-parameters file to bind-mount
            at ``/certs/dhparam.pem``. Required for DHE-* ciphers on some OpenSSL
            builds; reuse ``configs/clickhouse/ssl/dhparam.pem`` if present.
        :param cipher: TLSv1.2 ``-cipher`` argument (OpenSSL name format).
        :param ciphersuites: TLSv1.3 ``-ciphersuites`` argument.
        :param tls_version: ``-tls1_2`` or ``-tls1_3``.
        :param extra_args: any additional ``openssl s_server`` flags.
        """
        if role != "s_server":
            raise ValueError("only role='s_server' is supported here; use backup_fips.cmd for s_client probes")
        if not cert_path or not key_path:
            raise ValueError("cert_path and key_path are required (reuse configs/clickhouse/ssl/*)")

        volumes = [
            (cert_path, "/certs/server.crt"),
            (key_path, "/certs/server.key"),
        ]
        if dhparam_path:
            volumes.append((dhparam_path, "/certs/dhparam.pem"))
        flags = [
            f"-accept {int(listen)}",
            "-cert /certs/server.crt",
            "-key /certs/server.key",
            tls_version,
            "-www",
        ]
        if dhparam_path:
            flags.append("-dhparam /certs/dhparam.pem")
        if cipher:
            flags.append(f"-cipher '{cipher}'")
        if ciphersuites:
            flags.append(f"-ciphersuites {ciphersuites}")
        if extra_args:
            flags.append(extra_args)

        return self.start_auxiliary_container(
            name=name,
            image="alpine/openssl:latest",
            hostname=name,
            volumes=volumes,
            ports=[str(listen)],
            entrypoint=["sh", "-c"],
            command=["openssl s_server " + " ".join(flags)],
            healthcheck={
                "Test": ["CMD-SHELL", f"nc -z localhost {int(listen)} || exit 1"],
                "Interval": 2 * 1_000_000_000,
                "Timeout": 2 * 1_000_000_000,
                "Retries": 15,
                "StartPeriod": 3 * 1_000_000_000,
            },
            wait_healthy=True,
            timeout=45,
        )

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

        clickhouse_version = os.environ.get("CLICKHOUSE_VERSION", "26.3")
        clickhouse_image = os.environ.get("CLICKHOUSE_IMAGE", "clickhouse/clickhouse-server")
        zookeeper_version = os.environ.get("ZOOKEEPER_VERSION", "3.9.5")
        zookeeper_image = os.environ.get("ZOOKEEPER_IMAGE", "docker.io/zookeeper")
        mysql_version = os.environ.get("MYSQL_VERSION", "latest")
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
                        self._dump_all_container_diagnostics()
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
        install_delve_path = os.path.normpath(os.path.join(tests_dir, "../../../test/integration/configs/install_delve.sh"))

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

            backup_config_mount = self._backup_config_dir or os.path.join(tests_dir, "configs/backup")
            backup_volumes = [
                (backup_binary, "/bin/clickhouse-backup"),
                (backup_config_mount, "/etc/clickhouse-backup"),
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

        # 6. clickhouse_backup_fips (optional; same volumes view as clickhouse1)
        # This container hosts the FIPS-compatible binary built by ``make build-race-fips``.
        # It does not auto-start clickhouse-backup so tests can start/stop/kill it
        # explicitly via BackupNode.start_server/stop_server/kill_server.
        # If the FIPS binary is missing on the host we silently skip the container. FIPS scenarios then ``skip()``.
        backup_fips_binary = os.path.normpath(
            os.path.join(tests_dir, "../../../clickhouse-backup/clickhouse-backup-race-fips")
        )
        # If the host-side binary is missing but the upstream Makefile target
        # `build-race-fips-docker` has already left its image in the local
        # Docker registry, recover the binary out of that image rather than
        # silently skipping the whole FIPS suite. Pure Docker SDK - no
        # subprocess, no make - so it works the same in dev shells and in CI.
        if not os.path.isfile(backup_fips_binary):
            _materialize_binary_from_docker_image(
                self._docker_client,
                image_tag="clickhouse-backup:build-race-fips",
                path_in_image="/src/clickhouse-backup/clickhouse-backup-race-fips",
                host_path=backup_fips_binary,
            )
        if os.path.isfile(backup_fips_binary):
            with And("starting clickhouse_backup_fips"):
                # The container picks up ``/etc/clickhouse-server/ssl/``
                # via ``volumes_from_name="clickhouse1"`` below (the same static
                # certs the cluster bind-mounts on every CH node), so FIPS scenarios
                # never need to generate or relocate certificates.
                # Helper scripts (e.g. ``tamper_go_fips_checksum.sh``) are
                # bind-mounted read-only so FIPS scenarios can invoke the
                # exact same script paths the test plan documents.
                backup_fips_scripts_dir = os.path.join(tests_dir, "scripts")
                backup_fips_volumes = [
                    (backup_fips_binary, "/bin/clickhouse-backup-fips"),
                    (backup_config_mount, "/etc/clickhouse-backup"),
                    (backup_fips_scripts_dir, "/scripts"),
                    (coverage_dir, "/tmp/_coverage_"),
                ]
                self._start_container(
                    name="clickhouse_backup_fips",
                    image="ubuntu:latest",
                    hostname="backup_fips",
                    env={
                        "DEBIAN_FRONTEND": "noninteractive",
                        "LOG_LEVEL": log_level,
                        "GCS_CREDENTIALS_JSON": gcs_cred_json,
                        "GCS_CREDENTIALS_JSON_ENCODED": gcs_cred_json_encoded,
                        "TZ": "Europe/Moscow",
                        "GOCOVERDIR": "/tmp/_coverage_/",
                    },
                    volumes=backup_fips_volumes,
                    volumes_from_name="clickhouse1",
                    ports=["7172"],
                    entrypoint=["/bin/bash"],
                    command=[
                        "-c",
                        "set -x && "
                        "apt-get update && "
                        "apt-get install -y --no-install-recommends "
                        # binutils -> readelf (.go.fipsinfo offset lookup),
                        # python3  -> tamper_go_fips_checksum.sh byte editor,
                        # golang-go -> `go version -m` for build-flag scenario.
                        "ca-certificates tzdata bash curl openssl procps binutils python3 golang-go && "
                        "update-ca-certificates && "
                        # Do not start the binary; tests start/stop it explicitly.
                        "exec sleep infinity"
                    ],
                    cap_add=["SYS_NICE"],
                    healthcheck={
                        # The container is "ready" as soon as the openssl install is done.
                        "Test": ["CMD-SHELL", "command -v openssl >/dev/null && test -x /bin/clickhouse-backup-fips"],
                        "Interval": 5 * 1_000_000_000,
                        "Timeout": 5 * 1_000_000_000,
                        "Retries": 60,
                        "StartPeriod": 10 * 1_000_000_000,
                    },
                )
                _wait_for_container_healthy(
                    self._docker_client, self._container_ids["clickhouse_backup_fips"], timeout=300
                )
        else:
            note(
                f"clickhouse-backup-race-fips not found at {backup_fips_binary}; "
                "skipping clickhouse_backup_fips container. Build with `make build-race-fips-docker` "
                "to enable FIPS scenarios."
            )

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
