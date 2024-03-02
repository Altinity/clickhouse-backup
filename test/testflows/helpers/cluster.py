import glob
import inspect
import os
import tempfile
import threading
import time

import testflows.settings as settings
from testflows.asserts import error
from testflows.connect import Shell as ShellBase
from testflows.core import *
from testflows.uexpect import ExpectTimeoutError

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
        # 'docker-compose exec {name} bash --noediting'
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
            r = self.cluster.command(None, f'{self.cluster.docker_compose} restart {self.name}', timeout=timeout)
            if r.exitcode == 0:
                break

    def start(self, timeout=300, num_retries=5):
        """Start node.
        """
        for _ in range(num_retries):
            r = self.cluster.command(None, f'{self.cluster.docker_compose} start {self.name}', timeout=timeout)
            if r.exitcode == 0:
                break

    def stop(self, timeout=300, num_retries=5, safe=True):
        """Stop node.
        """
        self.close_bashes()

        for _ in range(num_retries):
            r = self.cluster.command(None, f'{self.cluster.docker_compose} stop {self.name}', timeout=timeout)
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
            r = self.cluster.command(None, f'{self.cluster.docker_compose} stop {self.name}', timeout=timeout)
            if r.exitcode == 0:
                break

    def start(self, timeout=300, wait_healthy=True, num_retries=5):
        """Start node.
        """
        for _ in range(num_retries):
            r = self.cluster.command(None, f'{self.cluster.docker_compose} start {self.name}', timeout=timeout)
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
            r = self.cluster.command(None, f'{self.cluster.docker_compose} restart {self.name}', timeout=timeout)
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
                command = f"set -o pipefail && cat \"{query.name}\" | {self.cluster.docker_compose} exec -T {self.name} {client} | {hash_utility}"
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
                command = f"diff <(cat \"{query.name}\" | {self.cluster.docker_compose} exec -T {self.name} {client}) {expected_output}"
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
            command = f"diff <(echo -e \"{sql}\" | {self.cluster.docker_compose} exec -T {self.name} {client}) {expected_output}"
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
                command = f"cat \"{query.name}\" | {self.cluster.docker_compose} exec -T {self.name} {client}"
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


class Cluster(object):
    """Simple object around docker-compose cluster.
    """

    def __init__(self, local=False,
                 configs_dir=None,
                 nodes=None,
                 docker_compose="docker-compose", docker_compose_project_dir=None,
                 docker_compose_file="docker-compose.yml",
                 environ=None):

        self.shells = {}
        self._control_shell = None
        self.environ = {} if (environ is None) else environ
        self.configs_dir = configs_dir
        self.local = local
        self.nodes = nodes or {}
        self.docker_compose = docker_compose

        frame = inspect.currentframe().f_back
        caller_dir = os.path.dirname(os.path.abspath(frame.f_globals["__file__"]))

        # auto set configs directory
        if self.configs_dir is None:
            caller_configs_dir = caller_dir
            if os.path.exists(caller_configs_dir):
                self.configs_dir = caller_configs_dir

        if not os.path.exists(self.configs_dir):
            raise TypeError(f"configs directory '{self.configs_dir}' does not exist")

        if docker_compose_project_dir is None:
            caller_project_dir = os.path.join(caller_dir, "docker-compose")
            if os.path.exists(caller_project_dir):
                docker_compose_project_dir = caller_project_dir

        docker_compose_file_path = os.path.join(docker_compose_project_dir or "", docker_compose_file)

        if not os.path.exists(docker_compose_file_path):
            raise TypeError(f"docker compose file '{docker_compose_file_path}' does not exist")

        self.docker_compose += f" --ansi never --project-directory \"{docker_compose_project_dir}\" --file \"{docker_compose_file_path}\""
        self.lock = threading.Lock()

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
        """Must be called with `self.lock` acquired.
        """
        time_start = time.time()
        while True:
            try:
                c = self.control_shell(f"{self.docker_compose} ps -q {node}", timeout=timeout)
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
        """Must be called with `self.lock` acquired.
        """
        time_start = time.time()
        while True:
            try:
                c = self.control_shell(f"{self.docker_compose} ps {node} | grep {node}", timeout=timeout)
                if c.exitcode == 0 and '(healthy)' in c.output:
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
        with Given("docker-compose cluster"):
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

    def down(self, timeout=300):
        """Bring cluster down by executing docker-compose down."""

        # add message to each clickhouse-server.log
        if settings.debug:
            for node in self.nodes["clickhouse"]:
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
            if not self.local:
                self.command(
                    None, f"{self.docker_compose} down -v --remove-orphans --timeout 60", bash=bash, timeout=timeout
                )
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

        with Given("docker-compose"):
            max_attempts = 5
            max_up_attempts = 1

            for attempt in range(max_attempts):
                with When(f"attempt {attempt}/{max_attempts}"):
                    with By("checking if any containers are already running"):
                        self.command(None, f"{self.docker_compose} ps | tee")

                    with And("executing docker-compose down just in case it is up"):
                        cmd = self.command(
                            None, f"{self.docker_compose} down --timeout=10 2>&1 | tee", exitcode=None, timeout=timeout
                        )
                        if cmd.exitcode != 0:
                            continue

                    with And("checking if any containers are still left running"):
                        self.command(None, f"{self.docker_compose} ps | tee")

                    with And("executing docker-compose up"):
                        for up_attempt in range(max_up_attempts):
                            with By(f"attempt {up_attempt}/{max_up_attempts}"):
                                cmd = self.command(
                                    None, f"{self.docker_compose} up --timeout 120 -d 2>&1 | tee", timeout=timeout
                                )
                                if "is unhealthy" not in cmd.output:
                                    break

                    with Then("check there are no unhealthy containers"):
                        ps_cmd = self.command(None, f"{self.docker_compose} ps | tee | grep -v \"Exit 0\"")
                        if "is unhealthy" in cmd.output or "Exit" in ps_cmd.output:
                            self.command(None, f"{self.docker_compose} logs | tee")
                            continue

                    if cmd.exitcode == 0 and "is unhealthy" not in cmd.output and "Exit" not in ps_cmd.output:
                        break

            if cmd.exitcode != 0 or "is unhealthy" in cmd.output or "Exit" in ps_cmd.output:
                fail("could not bring up docker-compose cluster")

        with Then("wait all nodes report healthy"):
            for node_name in self.nodes["clickhouse"]:
                self.node(node_name).wait_healthy()

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
