from pathlib import Path


backend = Path("providers/common/ai/src/airflow/providers/common/ai/sandbox/ascii_box.py")
text = backend.read_text()

# Support SDK response variants that either return a Box directly or wrap it
# under a `.box` attribute.
if "box_id = created.box.id" in text:
    text = text.replace(
        "            box_id = created.box.id\n",
        "            created_box = getattr(created, \"box\", created)\n"
        "            box_id = created_box.id\n",
        1,
    )
assert 'created_box = getattr(created, "box", created)' in text

old = "            box = self._get_api().get(sandbox, _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT)).box\n"
new = (
    "            response = self._get_api().get(\n"
    "                sandbox, _request_timeout=self._http_timeout(_FILE_OP_TIMEOUT)\n"
    "            )\n"
    "            box = getattr(response, \"box\", response)\n"
)
if old in text:
    text = text.replace(old, new, 1)
assert 'box = getattr(response, "box", response)' in text

# Do not claim a path restriction the backend does not implement.
text = text.replace(
    "    implementation, because the native read API takes no size parameter and\n"
    "    would land a whole file in worker memory before ``max_bytes`` could reject\n"
    "    it. Written paths must resolve under ``/home/user`` or ``/tmp``.\n",
    "    implementation, because the native read API takes no size parameter and\n"
    "    would land a whole file in worker memory before ``max_bytes`` could reject\n"
    "    it.\n",
    1,
)
backend.write_text(text)

provider = Path("providers/common/ai/provider.yaml")
text = provider.read_text()
docker = (
    "  - integration-name: Docker Sandboxes\n"
    "    external-doc-url: https://docs.docker.com/ai/sandboxes/\n"
    "    tags: [software]\n"
)
ascii_box = (
    "  - integration-name: Ascii Box\n"
    "    external-doc-url: https://docs.ascii.dev/box/quickstart\n"
    "    tags: [service]\n"
)
if "integration-name: Ascii Box" not in text:
    assert docker in text
    text = text.replace(docker, docker + ascii_box, 1)
provider.write_text(text)

docs = Path("providers/common/ai/docs/toolsets.rst")
text = docs.read_text()
old_warning = """   Treat it as the backend you develop and test a sandboxed agent against, then
   run something else in production. A hosted backend plugs in through
   :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`, but none ships
   with the provider yet.
"""
new_warning = """   Treat it as the backend you develop and test a sandboxed agent against, then
   run something else in production -- either
   :class:`~airflow.providers.common.ai.sandbox.AsciiBoxSandboxBackend` below,
   or your own hosted backend behind
   :class:`~airflow.providers.common.ai.sandbox.SandboxBackend`.
"""
if old_warning in text:
    text = text.replace(old_warning, new_warning, 1)

section = """Ascii Box backend
^^^^^^^^^^^^^^^^^

:class:`~airflow.providers.common.ai.sandbox.AsciiBoxSandboxBackend` runs each
sandbox in an `Ascii Box <https://docs.ascii.dev/box/quickstart>`__, a hosted
cloud-computer API. The Airflow worker needs only network access and an API key;
it needs no local daemon, Docker socket, KVM, or nested virtualization.

Install the optional SDK with the ``sandbox-ascii-box`` extra::

    pip install "apache-airflow-providers-common-ai[sandbox-ascii-box]"

.. code-block:: python

    from airflow.providers.common.ai.sandbox import AsciiBoxSandboxBackend, SandboxSpec
    from airflow.providers.common.ai.toolsets import SandboxToolset

    SandboxToolset(
        AsciiBoxSandboxBackend(box_conn_id="ascii_box_default"),
        spec=SandboxSpec(block_network=False),
    )

By default, credentials resolve lazily from a generic Airflow connection on
first use, so the API key can remain in the configured secrets backend:

- ``password``: Box API key. Required.
- ``host``: API base URL. Optional; defaults to ``https://ascii.dev/api/box/v1``.
- Extra ``timeout``: HTTP request timeout in seconds.
- Extra ``no_env``: whether Box should withhold account-stored secrets. Defaults
  to ``true``.

Constructor parameters:

- ``box_conn_id``: Connection ID. Default ``"ascii_box_default"``. Passing
  ``None`` reads ``BOX_API_KEY`` and optional ``BOX_BASE_URL`` from the worker
  environment instead.
- ``machine_type``: ``"small"``, ``"default"``, or ``"large"``.
- ``ttl_seconds``: server-side auto-stop TTL. Default ``3600`` seconds.
- ``ready_timeout``: provisioning deadline. Default ``300`` seconds.
- ``no_env``: explicit override for the connection's ``no_env`` setting.

``SandboxSpec.env`` is passed when the Box is created. Box cannot enforce a
deny-all network policy or a per-domain egress allowlist, so the backend refuses
``block_network=True`` and ``allow_egress_to`` rather than silently weakening
the requested isolation. Use ``SandboxSpec(block_network=False)`` only when open
egress is acceptable.

Writes use Box's native file API. Reads deliberately use the inherited bounded
shell implementation so ``max_bytes`` is enforced inside the guest before file
contents reach worker memory. Command timeouts are capped at 600 seconds; a Box
whose command times out, or that never becomes ready, is torn down immediately,
with the server-side TTL as the orphan-cleanup backstop.

"""
anchor = "Bringing your own backend\n^^^^^^^^^^^^^^^^^^^^^^^^^\n"
if "Ascii Box backend\n^^^^^^^^^^^^^^^^^" not in text:
    assert anchor in text
    text = text.replace(anchor, section + anchor, 1)
docs.write_text(text)
