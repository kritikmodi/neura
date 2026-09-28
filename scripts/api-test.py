#!/usr/bin/env python3
import json, os, urllib.request, urllib.error, uuid

base = os.environ.get("NEURA_TEST_API_URL", "http://127.0.0.1:58080")


def req(path, body=None, auth=True):
    h = {"Content-Type": "application/json"}
    if auth:
        h["Authorization"] = "Bearer " + os.environ["NEURA_API_TOKEN"]
    return urllib.request.urlopen(
        urllib.request.Request(
            base + path,
            data=None if body is None else json.dumps(body).encode(),
            headers=h,
        )
    )


try:
    req("/v1/sources", auth=False)
    raise AssertionError("unauthenticated request accepted")
except urllib.error.HTTPError as e:
    assert e.code == 401
id = str(uuid.uuid4())
with req("/v1/query", {"id": id, "sql": "SELECT 1 AS n"}) as r:
    events = [json.loads(l) for l in r]
assert [e["type"] for e in events] == ["schema", "row", "complete"]
assert events[1]["values"] == ["1"]
assert json.load(req("/v1/queries/" + id))["status"] == "succeeded"
for sql in [
    "COPY (SELECT 1) TO '/tmp/neura-must-not-exist'",
    "DROP TABLE pg.customers",
    "SELECT 1; SELECT 2",
]:
    try:
        req("/v1/query", {"sql": sql})
        raise AssertionError("mutation/multiple statements accepted")
    except urllib.error.HTTPError as e:
        assert e.code == 400
print(
    "PASS: REST auth, streaming result completion, diagnostics and read-only validation"
)
